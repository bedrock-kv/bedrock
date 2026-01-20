defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase do
  @moduledoc """
  Bootstraps the metadata shard materializer for recovery.

  The metadata materializer holds the authoritative shard layout - the mapping from
  key ranges to shard tags. This phase ensures the materializer is available and
  queries it for the current shard layout.

  ## Fresh Cluster

  For a fresh cluster (no old logs), creates a default shard layout with two shards:
  - System shard (tag 0): Keys from 0xFF to end-of-keyspace (system metadata)
  - User shard (tag 1): Keys from empty string to 0xFF (user data)

  ## Existing Cluster

  For an existing cluster:
  1. Find materializer with shard_id = 0 (system shard) in available_services
  2. If not found, create a new materializer on a capable node
  3. Lock materializer for recovery
  4. Unlock it with system shard logs to start pulling
  5. Wait for materializer to catch up (60s timeout)
  6. Query shard layout from `\\xff/system/shard_keys/*`

  Stalls if the materializer is unavailable and cannot be created, or if catchup
  times out. Transitions to CommitProxyStartupPhase with the materializer pid and
  shard layout.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  require Logger

  # Catchup timeout: 60 seconds before stalling and retrying
  @catchup_timeout_ms 60_000
  @catchup_poll_interval_ms 500

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    if fresh_cluster?(context) do
      handle_fresh_cluster(recovery_attempt)
    else
      handle_existing_cluster(recovery_attempt, context)
    end
  end

  @doc """
  Returns the default shard layout for a fresh cluster.

  Layout has two shards:
  - Tag 0: System keys (0xFF to end_of_keyspace)
  - Tag 1: User keys (empty string to 0xFF)

  The map is keyed by end_key, with values of {tag, start_key}.
  """
  @spec default_shard_layout() :: RecoveryAttempt.shard_layout()
  def default_shard_layout do
    %{
      # User shard: "" to 0xFF
      <<0xFF>> => {1, <<>>},
      # System shard: 0xFF to end
      end_of_keyspace() => {0, <<0xFF>>}
    }
  end

  # Private implementation

  defp fresh_cluster?(%{old_transaction_system_layout: nil}), do: true
  defp fresh_cluster?(%{old_transaction_system_layout: %{logs: logs}}) when map_size(logs) == 0, do: true

  defp fresh_cluster?(_context), do: false

  defp handle_fresh_cluster(recovery_attempt) do
    Logger.debug("Fresh cluster detected, using default shard layout")

    updated_attempt =
      recovery_attempt
      |> Map.put(:metadata_materializer, nil)
      |> Map.put(:shard_layout, default_shard_layout())

    {updated_attempt, CommitProxyStartupPhase}
  end

  defp handle_existing_cluster(recovery_attempt, context) do
    # Read at the newest version determined during log recovery planning
    {_oldest, read_version} = recovery_attempt.version_vector

    # Step 1-2: Find or create materializer for system shard
    with {:ok, materializer_service} <- find_or_create_materializer(recovery_attempt, context),
         # Step 3: Lock for recovery
         {:ok, materializer_pid} <- lock_materializer(materializer_service, recovery_attempt.epoch, context),
         # Step 4: Unlock with logs to start pulling
         :ok <- unlock_and_start_pulling(materializer_pid, recovery_attempt, context),
         # Step 5: Wait for catchup
         :ok <- wait_for_materializer_catchup(materializer_pid, recovery_attempt.durable_version, context),
         # Step 6: Query shard layout
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, read_version, context) do
      # Step 7: Continue
      updated_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, materializer_pid)
        |> Map.put(:shard_layout, shard_layout)

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  # Find existing materializer or create a new one for the system shard
  defp find_or_create_materializer(recovery_attempt, context) do
    case find_materializer_service(context) do
      {:ok, service} ->
        {:ok, service}

      {:error, {:materializer_unavailable, :not_in_available_services}} ->
        Logger.info("System shard materializer not found, creating new one")
        create_materializer(recovery_attempt, context)
    end
  end

  # Find materializer assigned to system shard (tag 0)
  # Supports both shard-based lookup (new format) and legacy string-key lookup
  defp find_materializer_service(%{available_services: services}) do
    system_shard = RecoveryAttempt.system_shard_id()

    # First try shard-based lookup (new format: {kind, ref, shard_id})
    shard_based_result =
      Enum.find(services, fn
        {_id, {kind, _ref, shard_id}} when is_integer(shard_id) ->
          kind == :materializer and shard_id == system_shard

        _ ->
          false
      end)

    case shard_based_result do
      {_id, service} ->
        {:ok, service}

      nil ->
        # Fall back to legacy string-key lookup for backward compatibility
        case Map.get(services, "metadata_materializer") do
          nil -> {:error, {:materializer_unavailable, :not_in_available_services}}
          service -> {:ok, service}
        end
    end
  end

  # Create a new materializer on a capable node
  defp create_materializer(recovery_attempt, context) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {worker_ref, node}} <- create_materializer_on_node(node, recovery_attempt, context) do
      {:ok, {:materializer, {worker_ref, node}, RecoveryAttempt.system_shard_id()}}
    end
  end

  # Find a node that can host materializers
  defp find_materializer_capable_node(%{node_capabilities: caps}) do
    case Map.get(caps, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  # Create the worker via Foreman with shard_id param
  defp create_materializer_on_node(node, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    system_shard = RecoveryAttempt.system_shard_id()

    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    # Pass shard_id in params so materializer knows its assignment
    case create_worker_fn.(foreman_ref, worker_id, :materializer, timeout: 30_000) do
      {:ok, worker_ref} -> {:ok, {worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, system_shard}}
    end
  end

  # Filter logs to only those relevant for the given shard (by tag)
  defp filter_logs_for_shard(logs, shard_id) do
    logs
    |> Enum.filter(fn {_log_id, tags} ->
      shard_id in tags
    end)
    |> Map.new()
  end

  # Unlock materializer with only the logs it needs to start pulling
  defp unlock_and_start_pulling(materializer_pid, recovery_attempt, context) do
    # Build TSL with only system shard logs
    system_shard = RecoveryAttempt.system_shard_id()
    system_logs = filter_logs_for_shard(recovery_attempt.logs, system_shard)

    # TransactionSystemLayout is a type, not a struct, so we build a map
    tsl = %{
      id: TransactionSystemLayout.random_id(),
      epoch: recovery_attempt.epoch,
      director: :unavailable,
      sequencer: recovery_attempt.sequencer,
      rate_keeper: nil,
      proxies: recovery_attempt.proxies,
      resolvers: recovery_attempt.resolvers,
      logs: system_logs,
      services: recovery_attempt.transaction_services
    }

    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(materializer_pid, recovery_attempt.durable_version, tsl) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason}}
    end
  end

  defp default_unlock_materializer(pid, durable_version, tsl) do
    Materializer.unlock_after_recovery(pid, durable_version, tsl, timeout_in_ms: 30_000)
  end

  # Poll until materializer reaches target version
  defp wait_for_materializer_catchup(pid, target_version, context) do
    timeout_ms = Map.get(context, :catchup_timeout_ms, @catchup_timeout_ms)
    poll_interval_ms = Map.get(context, :catchup_poll_interval_ms, @catchup_poll_interval_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context)
  end

  defp do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :catchup_timeout}
    else
      info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

      case info_fn.(pid, [:durable_version]) do
        {:ok, %{durable_version: v}} when v >= target_version ->
          Logger.debug("Materializer caught up to version #{inspect(v)}")
          :ok

        {:ok, %{durable_version: v}} ->
          Logger.debug("Materializer at version #{inspect(v)}, waiting for #{inspect(target_version)}")

          Process.sleep(poll_interval_ms)
          do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context)

        {:error, reason} ->
          {:error, {:catchup_info_failed, reason}}
      end
    end
  end

  defp default_materializer_info(pid, fact_names) do
    Materializer.info(pid, fact_names, timeout_in_ms: 5_000)
  end

  defp lock_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  defp default_lock_materializer({:materializer, name}, epoch) do
    name
    |> Materializer.lock_for_recovery(epoch)
    |> case do
      {:ok, pid, _info} -> {:ok, pid}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason}}
    end
  end

  # Handle new format with shard_id
  defp default_lock_materializer({:materializer, name, _shard_id}, epoch) do
    name
    |> Materializer.lock_for_recovery(epoch)
    |> case do
      {:ok, pid, _info} -> {:ok, pid}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason}}
    end
  end

  defp get_shard_layout(materializer_pid, read_version, context) do
    get_layout_fn = Map.get(context, :get_shard_layout_fn, &default_get_shard_layout/2)
    get_layout_fn.(materializer_pid, read_version)
  end

  defp default_get_shard_layout(materializer_pid, read_version) do
    # Query the materializer for shard layout via get_range on shard_keys prefix
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    end_key = prefix <> <<0xFF, 0xFF, 0xFF, 0xFF>>

    case Materializer.get_range(materializer_pid, prefix, end_key, read_version, limit: 1000) do
      {:ok, {entries, _more}} ->
        shard_layout =
          Map.new(entries, fn {key, value} ->
            # Key format: \xff/system/shard_keys/<end_key>
            # Value format: {tag, start_key}
            end_key = extract_end_key_from_shard_key(key)
            {tag, start_key} = decode_shard_value(value)
            {end_key, {tag, start_key}}
          end)

        {:ok, shard_layout}

      {:error, reason} ->
        {:error, {:shard_layout_query_failed, reason}}

      {:failure, reason, _ref} ->
        {:error, {:shard_layout_query_failed, reason}}
    end
  end

  defp extract_end_key_from_shard_key(key) do
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_shard_value(value) when is_binary(value) do
    :erlang.binary_to_term(value)
  end
end
