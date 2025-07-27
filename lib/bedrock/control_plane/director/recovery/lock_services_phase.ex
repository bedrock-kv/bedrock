defmodule Bedrock.ControlPlane.Director.Recovery.LockServicesPhase do
  @moduledoc """
  Locks all available services with the current epoch and determines the recovery path.

  This phase prevents split-brain by ensuring only one director can control services.
  Each service accepts locks from only one director at a time. Services with older
  epochs terminate when they detect newer epochs.

  After locking services, the phase checks for existing cluster state to decide
  between first-time initialization or recovery from existing data.

  Service locking happens first because it establishes exclusive control before
  any other recovery work begins. If locking fails, recovery stalls early rather
  than proceeding with incomplete service availability.

  Transitions to :first_time_initialization if no existing layout is found,
  or :determine_old_logs_to_copy if existing layout exists.
  """

  require Logger

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @impl true
  def execute(%{state: :lock_available_services} = recovery_attempt, context) do
    lock_available_services(context.available_services, recovery_attempt.epoch, 200)
    |> case do
      {:error, :newer_epoch_exists = reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, locked_service_ids, log_recovery_info_by_id, storage_recovery_info_by_id} ->
        recovery_attempt
        |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
        |> Map.update!(:storage_recovery_info_by_id, &Map.merge(storage_recovery_info_by_id, &1))
        |> Map.put(:locked_service_ids, locked_service_ids)
        |> determine_next_phase(context.cluster_config.transaction_system_layout)
    end
  end

  @spec determine_next_phase(map(), TransactionSystemLayout.t()) :: map()
  defp determine_next_phase(recovery_attempt, layout) do
    logs = Map.get(layout, :logs, %{})
    storage_teams = Map.get(layout, :storage_teams, [])

    case {map_size(logs), Enum.empty?(storage_teams)} do
      {0, true} ->
        recovery_attempt |> Map.put(:state, :first_time_initialization)

      _ ->
        recovery_attempt |> Map.put(:state, :determine_old_logs_to_copy)
    end
  end

  @spec lock_available_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_available_services_timeout, do: 200

  @spec lock_available_services(
          %{Worker.id() => ServiceDescriptor.t()},
          Bedrock.quorum(),
          Bedrock.timeout_in_ms()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()),
           new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}}
          | {:error, :newer_epoch_exists}
  def lock_available_services(available_services, epoch, timeout_in_ms) do
    available_services
    |> Task.async_stream(
      fn {id, service} ->
        {id, lock_service_for_recovery(service, epoch)}
      end,
      timeout: timeout_in_ms,
      on_timeout: :kill_task,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({MapSet.new(), %{}}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {id, {:ok, _pid, info}}}, {locked_ids, info_by_id} ->
        {:cont, {MapSet.put(locked_ids, id), Map.put(info_by_id, id, info)}}

      {:ok, {_id, {:error, _}}}, {locked_ids, info_by_id} ->
        {:cont, {locked_ids, info_by_id}}
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, info_by_id} ->
        grouped_recovery_info = info_by_id |> Enum.group_by(&Map.get(elem(&1, 1), :kind))
        new_log_recovery_info_by_id = grouped_recovery_info |> Map.get(:log, []) |> Map.new()

        new_storage_recovery_info_by_id =
          grouped_recovery_info |> Map.get(:storage, []) |> Map.new()

        {:ok, locked_ids, new_log_recovery_info_by_id, new_storage_recovery_info_by_id}
    end
  end

  @spec lock_service_for_recovery(ServiceDescriptor.t(), Bedrock.epoch()) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(_, _), do: {:error, :unavailable}
end
