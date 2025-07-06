defmodule Bedrock.ControlPlane.Director.Recovery.LockServicesPhase do
  @moduledoc """
  Handles the :lock_available_services phase of recovery.

  This phase is responsible for locking all available services for the current epoch
  and determining whether this is a first-time initialization or a recovery from
  an existing cluster state.
  """

  require Logger

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  @doc """
  Execute the service locking phase of recovery.

  Attempts to lock all available services for the current epoch. If successful,
  determines the next phase based on whether this is a first-time initialization
  or recovery from existing state.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :lock_available_services} = recovery_attempt) do
    lock_available_services(recovery_attempt.available_services, recovery_attempt.epoch, 200)
    |> case do
      {:error, :newer_epoch_exists = reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, locked_service_ids, updated_services, log_recovery_info_by_id,
       storage_recovery_info_by_id} ->
        recovery_attempt
        |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
        |> Map.update!(:storage_recovery_info_by_id, &Map.merge(storage_recovery_info_by_id, &1))
        |> Map.update!(:available_services, &Map.merge(&1, updated_services))
        |> Map.put(:locked_service_ids, locked_service_ids)
        |> determine_next_phase()
    end
  end

  defp determine_next_phase(
         %{last_transaction_system_layout: %{logs: %{}, storage_teams: []}} = recovery_attempt
       ) do
    recovery_attempt |> Map.put(:state, :first_time_initialization)
  end

  defp determine_next_phase(recovery_attempt) do
    recovery_attempt |> Map.put(:state, :determine_old_logs_to_copy)
  end

  @spec lock_available_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_available_services_timeout, do: 200

  @doc """
  Attempts to lock services for recovery. It then sends an invitation to each
  eligible service in parallel, requesting that they lock for recovery in this
  new epoch.

  The operation runs within the bounded-time  specified by `timeout_in_ms` and
  all of the services are contacted asynchronously. If the function encounters a
  service indicating that a newer epoch exists, it will halt further processing
  and return that as an error. Otherwise, it will collect the process IDs and
  lock/status information and return that in a success tuple.
  """
  @spec lock_available_services(
          %{Worker.id() => ServiceDescriptor.t()},
          Bedrock.quorum(),
          Bedrock.timeout_in_ms()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()),
           updated_services :: %{Worker.id() => ServiceDescriptor.t()},
           new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_storage_recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}}
          | {:error, :newer_epoch_exists}
  def lock_available_services(available_services, epoch, timeout_in_ms) do
    available_services
    |> Task.async_stream(
      fn {id, service} ->
        {id, service, lock_service_for_recovery(service, epoch)}
      end,
      timeout: timeout_in_ms,
      on_timeout: :kill_task,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({MapSet.new(), %{}, %{}}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {id, service, {:ok, pid, info}}}, {locked_ids, services, info_by_id} ->
        {:cont,
         {MapSet.put(locked_ids, id), Map.put(services, id, up(service, pid)),
          Map.put(info_by_id, id, info)}}

      {:ok, {id, service, {:error, _}}}, {locked_ids, services, info_by_id} ->
        {:cont, {locked_ids, Map.put(services, id, down(service)), info_by_id}}
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, updated_services, info_by_id} ->
        grouped_recovery_info =
          info_by_id
          |> Enum.group_by(&Map.get(elem(&1, 1), :kind))

        new_log_recovery_info_by_id =
          grouped_recovery_info |> Map.get(:log, []) |> Map.new()

        new_storage_recovery_info_by_id =
          grouped_recovery_info |> Map.get(:storage, []) |> Map.new()

        {:ok, locked_ids, updated_services, new_log_recovery_info_by_id,
         new_storage_recovery_info_by_id}
    end
  end

  def lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(_, _), do: {:error, :unavailable}

  defp up(service, pid), do: %{service | status: {:up, pid}}

  def down(service), do: %{service | status: :down}
end
