defmodule Bedrock.ControlPlane.Director.Recovery.LockServicesPhase do
  @moduledoc """
  Handles the :lock_available_services phase of recovery.

  This phase is responsible for locking all available services for the current epoch
  and determining whether this is a first-time initialization or a recovery from
  an existing cluster state.

  See: [Recovery Guide - Service Discovery and Locking](docs/knowledge_base/01-guides/recovery-guide.md#phase-3-service-discovery-and-locking)
  """

  require Logger

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @doc """
  Execute the service locking phase of recovery.

  Attempts to lock all available services for the current epoch. If successful,
  determines the next phase based on whether this is a first-time initialization
  or recovery from existing state.
  """
  @impl true
  def execute(%{state: :lock_available_services} = recovery_attempt, _context) do
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

  @spec determine_next_phase(map()) :: map()
  defp determine_next_phase(
         %{last_transaction_system_layout: %{logs: %{}, storage_teams: []}} = recovery_attempt
       ) do
    recovery_attempt |> Map.put(:state, :first_time_initialization)
  end

  defp determine_next_phase(recovery_attempt) do
    if has_stale_components?(recovery_attempt.last_transaction_system_layout) do
      Logger.warning(
        "Detected stale components in transaction system layout, forcing fresh initialization"
      )

      recovery_attempt |> Map.put(:state, :first_time_initialization)
    else
      recovery_attempt |> Map.put(:state, :determine_old_logs_to_copy)
    end
  end

  @spec has_stale_components?(map()) :: boolean()
  defp has_stale_components?(%{resolvers: resolvers, proxies: proxies, sequencer: sequencer}) do
    stale_resolvers = resolvers |> Enum.any?(&resolver_is_stale?/1)
    stale_proxies = proxies |> Enum.any?(&process_is_stale?/1)
    stale_sequencer = process_is_stale?(sequencer)

    stale_resolvers or stale_proxies or stale_sequencer
  end

  defp has_stale_components?(_), do: false

  @spec resolver_is_stale?(pid() | {Bedrock.key(), pid()}) :: boolean()
  defp resolver_is_stale?({_start_key, pid}) when is_pid(pid), do: pid_is_stale?(pid)
  defp resolver_is_stale?(pid) when is_pid(pid), do: pid_is_stale?(pid)
  defp resolver_is_stale?(_), do: false

  @spec process_is_stale?(pid()) :: boolean()
  defp process_is_stale?(pid) when is_pid(pid), do: pid_is_stale?(pid)
  defp process_is_stale?(_), do: false

  @spec pid_is_stale?(pid()) :: boolean()
  defp pid_is_stale?(pid) do
    not Process.alive?(pid)
  rescue
    ArgumentError ->
      # Remote PID that we can't check directly - assume it's stale
      true
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

  @spec lock_service_for_recovery(ServiceDescriptor.t(), Bedrock.epoch()) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(%{kind: :log, last_seen: name}, epoch),
    do: Log.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(%{kind: :storage, last_seen: name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)

  def lock_service_for_recovery(_, _), do: {:error, :unavailable}

  @spec up(ServiceDescriptor.t(), pid()) :: ServiceDescriptor.t()
  defp up(service, pid), do: %{service | status: {:up, pid}}

  @spec down(ServiceDescriptor.t()) :: ServiceDescriptor.t()
  def down(service), do: %{service | status: :down}
end
