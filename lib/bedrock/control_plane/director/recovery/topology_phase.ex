defmodule Bedrock.ControlPlane.Director.Recovery.TopologyPhase do
  @moduledoc """
  Constructs the Transaction System Layout (TSL), the blueprint defining how all
  components in the recovered system connect and communicate.

  The TSL solves the coordination problem in distributed systems: without it,
  individual components exist in isolation unable to find each other. This phase
  validates component availability, builds the complete topology, and prepares
  services for the system transaction.

  ## Validation Process

  Validates core transaction components:

  - **Sequencer**: Must have valid process ID (exactly one runs cluster-wide)
  - **Commit Proxies**: Must have valid process IDs for all proxies (list cannot be empty)
  - **Resolvers**: Must have valid `{start_key, process_id}` pairs defining key range responsibilities
  - **Logs**: Must have corresponding service entries with `{:up, process_id}` status

  ## TSL Construction

  Builds the complete TSL data structure containing component process IDs,
  service mappings, metadata materializer reference, and shard layout.

  ## Service Mapping Algorithm

  The services field maps service IDs to descriptors through:

  1. Extract all service IDs from logs
  2. For each service ID, check existence in context.available_services
  3. Build service descriptors containing:
     - **kind**: Service type (log, etc.)
     - **last_seen**: Timestamp from service discovery
     - **status**: Either `{:up, process_id}` or `:down`

  ## Selective Service Unlocking

  Commit proxies are unlocked via `CommitProxy.recover_from/5` with lock token and TSL.
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
       # Metadata materializer and shard layout from MaterializerBootstrapPhase
       metadata_materializer: recovery_attempt.metadata_materializer,
       shard_layout: recovery_attempt.shard_layout,
       # Shard materializers map: shard_tag -> pid
       shard_materializers: Map.get(recovery_attempt, :shard_materializers, %{}),
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
    # Only log services - storage teams are retired
    recovery_attempt.logs
    |> Map.keys()
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

  # Unlock commit proxies before exercising the transaction system
  # Storage servers are no longer unlocked here - materializers self-organize from logs
  @spec unlock_services(
          RecoveryAttempt.t(),
          TransactionSystemLayout.t(),
          Bedrock.lock_token(),
          map()
        ) ::
          :ok | {:error, {:unlock_failed, :timeout | :unavailable}}
  defp unlock_services(recovery_attempt, transaction_system_layout, lock_token, context) when is_binary(lock_token) do
    case unlock_commit_proxies(
           recovery_attempt.proxies,
           transaction_system_layout,
           lock_token,
           context
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
    end
  end

  @spec unlock_commit_proxies([pid()], TransactionSystemLayout.t(), Bedrock.lock_token(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_commit_proxies(proxies, transaction_system_layout, lock_token, context) when is_list(proxies) do
    unlock_fn = Map.get(context, :unlock_commit_proxy_fn, &CommitProxy.recover_from/5)

    # Extract what proxies need from TSL
    sequencer = transaction_system_layout.sequencer
    resolver_layout = CommitProxy.ResolverLayout.from_layout(transaction_system_layout)
    routing_data = build_routing_data(transaction_system_layout)

    proxies
    |> Task.async_stream(
      &unlock_fn.(&1, lock_token, sequencer, resolver_layout, routing_data),
      ordered: false
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, {:commit_proxy_unlock_failed, reason}}}
      {:exit, reason}, _ -> {:halt, {:error, {:commit_proxy_unlock_crashed, reason}}}
    end)
  end

  # Build full routing data from TSL for commit proxy unlock
  @spec build_routing_data(TransactionSystemLayout.t()) :: CommitProxy.RoutingData.t()
  defp build_routing_data(%{logs: logs, services: services, shard_layout: shard_layout}) do
    # Build shard_table ETS from shard_layout
    shard_table = :ets.new(:commit_proxy_shards, [:ordered_set, :public])

    Enum.each(shard_layout || %{}, fn {end_key, {tag, _start_key}} ->
      :ets.insert(shard_table, {end_key, tag})
    end)

    # Build log_map: index -> log_id
    log_map =
      logs
      |> Map.keys()
      |> Enum.sort()
      |> Enum.with_index()
      |> Map.new(fn {log_id, index} -> {index, log_id} end)

    # Build log_services: log_id -> pid or {name, node}
    log_services =
      logs
      |> Map.keys()
      |> Enum.reduce(%{}, fn log_id, acc ->
        case Map.get(services, log_id) do
          %{kind: :log, status: {:up, pid}} when is_pid(pid) ->
            Map.put(acc, log_id, pid)

          %{kind: :log, status: {:up, {name, node}}} when is_atom(name) and is_atom(node) ->
            Map.put(acc, log_id, {name, node})

          _ ->
            acc
        end
      end)

    replication_factor = max(1, map_size(logs))

    %CommitProxy.RoutingData{
      shard_table: shard_table,
      log_map: log_map,
      log_services: log_services,
      replication_factor: replication_factor
    }
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
