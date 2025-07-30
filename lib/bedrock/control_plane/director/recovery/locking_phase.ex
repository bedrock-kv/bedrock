defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhase do
  @moduledoc """
  Establishes exclusive director control by selectively locking services from the old system layout.

  Services are locked to prevent split-brain scenarios where multiple directors attempt
  concurrent control. Each service accepts locks from only one director at a time, with
  newer epochs causing older ones to terminate.

  Only services referenced in the old transaction system layout are locked - these contain
  data that must be copied during recovery. If the old layout is empty (first-time
  initialization), no services are locked. Other available services remain unlocked until
  recruitment phases assign them to specific roles.

  The recovery path is determined by what gets locked: nothing locked means first-time
  initialization, services locked means recovery from existing data.
  """

  require Logger

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  @impl true
  def execute(recovery_attempt, context) do
    old_system_services =
      extract_old_system_services(
        context.old_transaction_system_layout,
        context.available_services
      )

    lock_old_system_services(old_system_services, recovery_attempt.epoch, context)
    |> case do
      {:error, :newer_epoch_exists = reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, locked_service_ids, log_recovery_info_by_id, storage_recovery_info_by_id,
       transaction_services, service_pids} ->
        next_phase_module =
          if MapSet.size(locked_service_ids) == 0 do
            Bedrock.ControlPlane.Director.Recovery.InitializationPhase
          else
            Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhase
          end

        updated_recovery_attempt =
          recovery_attempt
          |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
          |> Map.update!(
            :storage_recovery_info_by_id,
            &Map.merge(storage_recovery_info_by_id, &1)
          )
          |> Map.put(:locked_service_ids, locked_service_ids)
          |> Map.update!(:transaction_services, &Map.merge(transaction_services, &1))
          |> Map.update!(:service_pids, &Map.merge(service_pids, &1))

        {updated_recovery_attempt, next_phase_module}
    end
  end

  @spec lock_old_system_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_old_system_services_timeout, do: 200

  @spec lock_old_system_services(
          %{Worker.id() => %{kind: atom(), last_seen: {atom(), node()}}},
          Bedrock.epoch(),
          map()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()),
           new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()},
           transaction_services :: %{
             Worker.id() => %{
               status: {:up, pid()},
               kind: :log | :storage,
               last_seen: {atom(), node()}
             }
           }, service_pids :: %{Worker.id() => pid()}}
          | {:error, :newer_epoch_exists}
  def lock_old_system_services(old_system_services, epoch, context \\ %{}) do
    timeout_in_ms = lock_old_system_services_timeout()

    old_system_services
    |> Task.async_stream(
      fn {id, service} ->
        {id, service, lock_service_for_recovery(service, epoch, context)}
      end,
      timeout: timeout_in_ms,
      on_timeout: :kill_task,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({MapSet.new(), %{}, %{}, %{}}, fn
      {:ok, {_, _, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {id, service, {:ok, pid, info}}},
      {locked_ids, info_by_id, transaction_services, service_pids} ->
        {:cont,
         {MapSet.put(locked_ids, id), Map.put(info_by_id, id, info),
          Map.put(transaction_services, id, %{
            status: {:up, pid},
            kind: info.kind,
            last_seen:
              case service do
                {_kind, location} -> location
                %{last_seen: location} -> location
              end
          }), Map.put(service_pids, id, pid)}}

      {:ok, {_id, _, {:error, _}}}, acc ->
        {:cont, acc}
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, info_by_id, transaction_services, service_pids} ->
        grouped_recovery_info = info_by_id |> Enum.group_by(&Map.get(elem(&1, 1), :kind))
        new_log_recovery_info_by_id = grouped_recovery_info |> Map.get(:log, []) |> Map.new()

        new_storage_recovery_info_by_id =
          grouped_recovery_info |> Map.get(:storage, []) |> Map.new()

        {:ok, locked_ids, new_log_recovery_info_by_id, new_storage_recovery_info_by_id,
         transaction_services, service_pids}
    end
  end

  @spec lock_service_for_recovery(
          {atom(), {atom(), node()}},
          Bedrock.epoch(),
          map()
        ) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(service, epoch, context \\ %{}) do
    lock_fn = Map.get(context, :lock_service_fn, &lock_service_impl/2)
    lock_fn.(service, epoch)
  end

  @spec lock_service_impl({atom(), {atom(), node()}}, Bedrock.epoch()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_service_impl({:log, name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  defp lock_service_impl({:storage, name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  defp lock_service_impl(_, _), do: {:error, :unavailable}

  @spec extract_old_system_services(map(), %{
          Worker.id() => {atom(), {atom(), node()}}
        }) ::
          %{Worker.id() => {atom(), {atom(), node()}}}
  defp extract_old_system_services(old_layout, available_services) do
    old_service_ids =
      (Map.keys(Map.get(old_layout, :logs, %{})) ++
         Enum.flat_map(Map.get(old_layout, :storage_teams, []), & &1.storage_ids))
      |> MapSet.new()

    available_services
    |> Enum.filter(fn {service_id, _} ->
      MapSet.member?(old_service_ids, service_id)
    end)
    |> Map.new()
  end
end
