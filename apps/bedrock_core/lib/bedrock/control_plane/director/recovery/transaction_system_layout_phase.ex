defmodule Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase do
  @moduledoc """
  Constructs the Transaction System Layout (TSL), the blueprint defining how all
  components in the recovered system connect and communicate.

  The TSL solves the coordination problem in distributed systems: without it,
  individual components exist in isolation unable to find each other. This phase
  validates component availability, builds the complete topology, and prepares
  services for the system transaction.

  ## Validation Process

  Validates four core transaction components (storage teams are not validated
  as their status was confirmed during storage recruitment):

  - **Sequencer**: Must have valid process ID (exactly one runs cluster-wide)
  - **Commit Proxies**: Must have valid process IDs for all proxies (list cannot be empty)  
  - **Resolvers**: Must have valid `{start_key, process_id}` pairs defining key range responsibilities
  - **Logs**: Must have corresponding service entries with `{:up, process_id}` status

  ## TSL Construction

  Builds the complete TSL data structure containing component process IDs,
  service mappings, and operational status.

  ## Service Mapping Algorithm

  The services field maps service IDs to descriptors through:

  1. Extract all service IDs from logs and storage teams
  2. For each service ID, check existence in context.available_services
  3. Build service descriptors containing:
     - **kind**: Service type (log, storage, etc.)
     - **last_seen**: Timestamp from service discovery
     - **status**: Either `{:up, process_id}` or `:down`

  ## Selective Service Unlocking

  Only specific components require unlocking before system transaction:

  - **Commit Proxies**: Unlocked via `CommitProxy.recover_from/3` with lock token and TSL
  - **Storage Servers**: Unlocked via `Storage.unlock_after_recovery/3` with durable version and TSL (5000ms timeout)

  Other components (sequencer, resolvers, logs) transition automatically when
  system transaction completes.

  ## Error Handling

  - Validation failures: `{:stalled, {:recovery_system_failed, {:invalid_recovery_state, reason}}}`
  - Construction failures: `{:stalled, {:recovery_system_failed, reason}}`
  - Unlock failures: `{:stalled, {:recovery_system_failed, {:unlock_failed, reason}}}`

  Transitions to `PersistencePhase` to commit layout through system transaction.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.TSLTypeValidator
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  @doc """
  Executes TSL construction with validation, building, and unlocking phases.

  This function implements the three-step TSL construction process:

  1. **Validation**: Confirms all transaction components are operational via `validate_recovery_state/1`
  2. **Construction**: Builds the complete TSL data structure via `build_transaction_system_layout/2`  
  3. **Unlocking**: Selectively unlocks commit proxies and storage servers via `unlock_services/4`

  ## Parameters

  - `recovery_attempt` - Recovery state containing component process IDs and configuration
  - `context` - Recovery context with available services, lock token, and capabilities

  ## Returns

  - `{updated_recovery_attempt, PersistencePhase}` - Success with TSL added to recovery attempt
  - `{recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}` - Failure with specific error

  ## Process Flow

  Uses `with` to ensure all steps succeed before transitioning. Any failure at validation,
  construction, or unlocking causes recovery to stall with detailed error information.

  The updated recovery attempt includes the constructed TSL, enabling the persistence
  phase to commit the layout through a system transaction.
  """
  @impl true
  def execute(recovery_attempt, context) do
    with :ok <- validate_recovery_state(recovery_attempt),
         {:ok, transaction_system_layout} <-
           build_transaction_system_layout(recovery_attempt, context),
         :ok <-
           unlock_services(
             recovery_attempt,
             transaction_system_layout,
             context.lock_token,
             context
           ) do
      # Validate the constructed TSL for type safety
      TSLTypeValidator.assert_type_safety!(transaction_system_layout)

      updated_recovery_attempt =
        %{recovery_attempt | transaction_system_layout: transaction_system_layout}

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MonitoringPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  defp build_transaction_system_layout(recovery_attempt, context) do
    {:ok,
     %{
       id: TransactionSystemLayout.random_id(),
       epoch: recovery_attempt.epoch,
       director: self(),
       sequencer: recovery_attempt.sequencer,
       rate_keeper: nil,
       proxies: recovery_attempt.proxies,
       resolvers: recovery_attempt.resolvers,
       logs: recovery_attempt.logs,
       storage_teams: recovery_attempt.storage_teams,
       services:
         build_services_for_layout(
           recovery_attempt,
           context
         )
     }}
  end

  @spec build_services_for_layout(RecoveryAttempt.t(), RecoveryPhase.context()) ::
          %{String.t() => ServiceDescriptor.t()}
  defp build_services_for_layout(recovery_attempt, context) do
    recovery_attempt
    |> extract_service_ids()
    |> Enum.map(fn service_id ->
      {service_id, build_service_descriptor(service_id, recovery_attempt, context)}
    end)
    |> Enum.reject(fn {_id, descriptor} -> is_nil(descriptor) end)
    |> Map.new()
  end

  defp extract_service_ids(recovery_attempt) do
    Enum.reduce(
      [extract_log_service_ids(recovery_attempt), extract_storage_service_ids(recovery_attempt)],
      MapSet.new(),
      &MapSet.union/2
    )
  end

  defp extract_log_service_ids(recovery_attempt) do
    recovery_attempt.logs
    |> Map.keys()
    |> MapSet.new()
  end

  defp extract_storage_service_ids(recovery_attempt) do
    recovery_attempt.storage_teams
    |> Enum.flat_map(& &1.storage_ids)
    |> MapSet.new()
  end

  @spec build_service_descriptor(
          String.t(),
          RecoveryAttempt.t(),
          RecoveryPhase.context()
        ) :: ServiceDescriptor.t() | nil
  defp build_service_descriptor(service_id, recovery_attempt, context) do
    case Map.get(context.available_services, service_id) do
      {kind, last_seen} = _service ->
        status = determine_service_status(service_id, recovery_attempt.service_pids)

        %{
          kind: kind,
          last_seen: last_seen,
          status: status
        }

      _ ->
        # Service not found in available services
        nil
    end
  end

  @spec determine_service_status(String.t(), %{String.t() => pid()}) :: ServiceDescriptor.status()
  defp determine_service_status(service_id, service_pids) do
    case Map.get(service_pids, service_id) do
      pid when is_pid(pid) -> {:up, pid}
      _ -> :down
    end
  end

  # Unlock commit proxies and storage servers before exercising the transaction system
  @spec unlock_services(
          RecoveryAttempt.t(),
          TransactionSystemLayout.t(),
          Bedrock.lock_token(),
          map()
        ) ::
          :ok | {:error, {:unlock_failed, :timeout | :unavailable}}
  defp unlock_services(recovery_attempt, transaction_system_layout, lock_token, context) when is_binary(lock_token) do
    with :ok <-
           unlock_commit_proxies(
             recovery_attempt.proxies,
             transaction_system_layout,
             lock_token,
             context
           ),
         :ok <-
           unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
      :ok
    else
      {:error, reason} -> {:error, {:unlock_failed, reason}}
    end
  end

  @spec unlock_commit_proxies([pid()], TransactionSystemLayout.t(), Bedrock.lock_token(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_commit_proxies(proxies, transaction_system_layout, lock_token, context) when is_list(proxies) do
    unlock_fn = Map.get(context, :unlock_commit_proxy_fn, &CommitProxy.recover_from/3)

    proxies
    |> Task.async_stream(
      &unlock_fn.(&1, lock_token, transaction_system_layout),
      ordered: false
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, {:commit_proxy_unlock_failed, reason}}}
      {:exit, reason}, _ -> {:halt, {:error, {:commit_proxy_unlock_crashed, reason}}}
    end)
  end

  @spec unlock_storage_servers(RecoveryAttempt.t(), TransactionSystemLayout.t(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
    # Use the latest version from logs (high end of version vector) instead of conservative durable_version
    # This prevents storage from purging committed data it already has
    {_first, latest_log_version} = recovery_attempt.version_vector
    unlock_fn = Map.get(context, :unlock_storage_fn, &Storage.unlock_after_recovery/3)

    transaction_system_layout.storage_teams
    |> Enum.flat_map(fn %{storage_ids: storage_ids} -> storage_ids end)
    |> Enum.uniq()
    |> Enum.map(fn storage_id ->
      recovery_attempt.transaction_services
      |> Map.fetch!(storage_id)
      |> then(fn %{status: {:up, pid}} -> {storage_id, pid} end)
    end)
    |> Task.async_stream(
      fn {storage_id, storage_pid} ->
        {storage_id, unlock_fn.(storage_pid, latest_log_version, transaction_system_layout)}
      end,
      ordered: false,
      timeout: 5000
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, {storage_id, :ok}}, :ok ->
        trace_recovery_storage_unlocking(storage_id)
        {:cont, :ok}

      {:ok, {_storage_id, {:error, reason}}}, _ ->
        {:halt, {:error, {:storage_unlock_failed, reason}}}

      {:exit, reason}, _ ->
        {:halt, {:error, {:storage_unlock_crashed, reason}}}
    end)
  end

  # Validate that recovery state is ready for system transaction
  @spec validate_recovery_state(RecoveryAttempt.t()) ::
          :ok | {:error, {:invalid_recovery_state, atom() | {atom(), [binary()]}}}
  defp validate_recovery_state(recovery_attempt) do
    with :ok <- validate_sequencer(recovery_attempt.sequencer),
         :ok <- validate_commit_proxies(recovery_attempt.proxies),
         :ok <- validate_resolvers(recovery_attempt.resolvers),
         :ok <- validate_logs(recovery_attempt.logs, recovery_attempt.transaction_services) do
      :ok
    else
      {:error, reason} -> {:error, {:invalid_recovery_state, reason}}
    end
  end

  @spec validate_sequencer(nil | pid()) :: :ok | {:error, atom()}
  defp validate_sequencer(nil), do: {:error, :no_sequencer}
  defp validate_sequencer(sequencer) when is_pid(sequencer), do: :ok
  defp validate_sequencer(_), do: {:error, :invalid_sequencer}

  @spec validate_commit_proxies([pid()]) :: :ok | {:error, atom()}
  defp validate_commit_proxies([]), do: {:error, :no_commit_proxies}

  defp validate_commit_proxies(proxies) when is_list(proxies) do
    if Enum.all?(proxies, &is_pid/1) do
      :ok
    else
      {:error, :invalid_commit_proxies}
    end
  end

  defp validate_commit_proxies(_), do: {:error, :invalid_commit_proxies}

  @spec validate_resolvers([{Bedrock.key(), pid()}]) :: :ok | {:error, atom()}
  defp validate_resolvers([]), do: {:error, :no_resolvers}

  defp validate_resolvers(resolvers) when is_list(resolvers) do
    valid_resolvers =
      Enum.all?(resolvers, fn
        {_start_key, pid} when is_pid(pid) -> true
        _ -> false
      end)

    if valid_resolvers do
      :ok
    else
      {:error, :invalid_resolvers}
    end
  end

  @spec validate_logs(%{Log.id() => LogDescriptor.t()}, %{Worker.id() => ServiceDescriptor.t()}) ::
          :ok | {:error, {:missing_log_services, [binary()]}}
  defp validate_logs(logs, transaction_services) when is_map(logs) do
    log_ids = Map.keys(logs)

    trace_recovery_log_validation_started(log_ids, transaction_services)

    # Check that all log IDs have corresponding services in transaction_services
    missing_services =
      Enum.reject(log_ids, fn log_id ->
        case Map.get(transaction_services, log_id) do
          %{status: {:up, pid}} when is_pid(pid) ->
            trace_recovery_log_service_status(log_id, :found, %{status: {:up, pid}})
            true

          _ ->
            trace_recovery_log_service_status(log_id, :missing, nil)
            false
        end
      end)

    case missing_services do
      [] ->
        :ok

      missing ->
        {:error, {:missing_log_services, missing}}
    end
  end
end
