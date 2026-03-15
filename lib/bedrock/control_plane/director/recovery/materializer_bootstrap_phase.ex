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
  5. Wait for materializer to catch up to its durable baseline (60s timeout)
  6. Poll shard layout reads from persisted `\\xff/system/shards/*` metadata until the
     recovered layout version is actually readable

  Stalls if the materializer is unavailable and cannot be created, or if catchup
  times out. Transitions to CommitProxyStartupPhase with the materializer pid and
  shard layout.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Internal.LayoutRouting
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.ShardMetadata

  require Logger

  # Catchup timeout: 60 seconds before stalling and retrying
  @catchup_timeout_ms 60_000
  @catchup_poll_interval_ms 500
  @shard_layout_read_timeout_ms 5_000

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    if fresh_cluster?(context) do
      handle_fresh_cluster(recovery_attempt, context)
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

  defp handle_fresh_cluster(recovery_attempt, context) do
    Logger.debug("Fresh cluster detected, using default shard layout")

    shard_layout = default_shard_layout()
    shard_tags = extract_shard_tags(shard_layout)

    # Create materializers for all shards in the layout
    case create_materializers_for_shards(shard_tags, recovery_attempt, context) do
      {:ok, shard_materializers} ->
        # Get the system shard materializer as metadata_materializer for backward compat
        system_shard = RecoveryAttempt.system_shard_id()
        metadata_materializer = Map.get(shard_materializers, system_shard)

        updated_attempt =
          recovery_attempt
          |> Map.put(:metadata_materializer, metadata_materializer)
          |> Map.put(:shard_layout, shard_layout)
          |> Map.put(:shard_materializers, shard_materializers)

        {updated_attempt, CommitProxyStartupPhase}

      {:error, reason} ->
        Logger.warning("Failed to create materializers for fresh cluster: #{inspect(reason)}")
        {recovery_attempt, {:stalled, {:materializer_creation_failed, reason}}}
    end
  end

  # Extract unique shard tags from shard_layout
  defp extract_shard_tags(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
  end

  # Create materializers for multiple shards
  defp create_materializers_for_shards(shard_tags, recovery_attempt, context) do
    Enum.reduce_while(shard_tags, {:ok, %{}}, fn shard_tag, {:ok, acc} ->
      case create_and_start_materializer(shard_tag, recovery_attempt, context) do
        {:ok, pid} ->
          {:cont, {:ok, Map.put(acc, shard_tag, pid)}}

        {:error, reason} ->
          {:halt, {:error, {shard_tag, reason}}}
      end
    end)
  end

  # Create a materializer for a specific shard and start it pulling
  defp create_and_start_materializer(shard_tag, recovery_attempt, context) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {worker_ref, node}} <- create_materializer_worker(node, shard_tag, recovery_attempt, context),
         {:ok, pid} <-
           lock_new_materializer({:materializer, {worker_ref, node}, shard_tag}, recovery_attempt.epoch, context),
         :ok <- start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
      {:ok, pid}
    end
  end

  # Create worker via Foreman for a specific shard
  defp create_materializer_worker(node, shard_tag, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    case create_worker_fn.(foreman_ref, worker_id, :materializer, timeout: 30_000) do
      {:ok, worker_ref} -> {:ok, {worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, shard_tag}}
    end
  end

  # Lock a newly created materializer
  defp lock_new_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  # Start materializer pulling from logs for its shard
  defp start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
    shard_logs = logs_for_shard(recovery_attempt.logs, shard_tag, context)

    tsl = %{
      id: TransactionSystemLayout.random_id(),
      epoch: recovery_attempt.epoch,
      director: :unavailable,
      sequencer: recovery_attempt.sequencer,
      rate_keeper: nil,
      proxies: recovery_attempt.proxies,
      resolvers: recovery_attempt.resolvers,
      logs: shard_logs,
      services: recovery_attempt.transaction_services
    }

    # For fresh cluster, start from version zero
    durable_version = Bedrock.DataPlane.Version.zero()
    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(pid, durable_version, tsl) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason}}
    end
  end

  defp handle_existing_cluster(recovery_attempt, context) do
    {_oldest, read_version} = recovery_attempt.version_vector
    system_shard = RecoveryAttempt.system_shard_id()

    # Step 1-2: Find or create materializer for system shard
    with {:ok, materializer_service} <- find_or_create_materializer(recovery_attempt, context, system_shard),
         # Step 3: Lock for recovery
         {:ok, materializer_pid} <- lock_materializer(materializer_service, recovery_attempt.epoch, context),
         # Step 4: Unlock with logs to start pulling
         :ok <- unlock_and_start_pulling(materializer_pid, recovery_attempt, context),
         # Step 5: Wait for the durable baseline to become available
         :ok <- wait_for_materializer_catchup(materializer_pid, recovery_attempt.durable_version, context),
         # Step 6: Wait until shard layout reads succeed at the recovered read version
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, read_version, context) do
      resolvers = resolver_descriptors_for_shard_layout(shard_layout)

      recovery_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, materializer_pid)
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(:resolvers, resolvers)

      case ensure_shard_materializers(recovery_attempt, materializer_pid, context) do
        {:ok, shard_materializers} ->
          updated_attempt = Map.put(recovery_attempt, :shard_materializers, shard_materializers)
          {updated_attempt, CommitProxyStartupPhase}

        {:error, reason} ->
          {recovery_attempt, {:stalled, reason}}
      end
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  defp ensure_shard_materializers(recovery_attempt, metadata_materializer_pid, context) do
    system_shard = RecoveryAttempt.system_shard_id()

    recovery_attempt.shard_layout
    |> extract_shard_tags()
    |> Enum.reduce_while({:ok, %{system_shard => metadata_materializer_pid}}, fn shard_tag, {:ok, acc} ->
      if shard_tag == system_shard do
        {:cont, {:ok, acc}}
      else
        with {:ok, materializer_service} <- find_or_create_materializer(recovery_attempt, context, shard_tag),
             {:ok, materializer_pid} <- lock_materializer(materializer_service, recovery_attempt.epoch, context),
             :ok <-
               unlock_materializer_for_shard(
                 materializer_pid,
                 shard_tag,
                 recovery_attempt,
                 recovery_attempt.durable_version,
                 context
               ),
             :ok <- wait_for_materializer_catchup(materializer_pid, recovery_attempt.durable_version, context) do
          {:cont, {:ok, Map.put(acc, shard_tag, materializer_pid)}}
        else
          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end
    end)
  end

  defp find_or_create_materializer(recovery_attempt, context, shard_tag) do
    case find_materializer_service(context, shard_tag) do
      {:ok, service} ->
        {:ok, service}

      {:error, {:materializer_unavailable, :not_in_available_services}} ->
        Logger.info("#{shard_label(shard_tag)} materializer not found, creating new one")
        create_materializer(recovery_attempt, context, shard_tag)
    end
  end

  # Supports both shard-based lookup (new format) and legacy string-key lookup for tag 0.
  defp find_materializer_service(%{available_services: services}, shard_tag) do
    # First try shard-based lookup (new format: {kind, ref, shard_id})
    shard_based_result =
      Enum.find(services, fn
        {_id, {kind, _ref, shard_id}} when is_integer(shard_id) ->
          kind == :materializer and shard_id == shard_tag

        _ ->
          false
      end)

    case shard_based_result do
      {_id, service} ->
        {:ok, service}

      nil ->
        find_legacy_materializer_service(services, shard_tag)
    end
  end

  defp find_legacy_materializer_service(services, shard_tag) do
    if shard_tag == RecoveryAttempt.system_shard_id() do
      case Map.get(services, "metadata_materializer") do
        nil -> {:error, {:materializer_unavailable, :not_in_available_services}}
        service -> {:ok, service}
      end
    else
      {:error, {:materializer_unavailable, :not_in_available_services}}
    end
  end

  defp create_materializer(recovery_attempt, context, shard_tag) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {worker_ref, node}} <-
           create_materializer_on_node(node, shard_tag, recovery_attempt, context) do
      {:ok, {:materializer, {worker_ref, node}, shard_tag}}
    end
  end

  # Find a node that can host materializers
  defp find_materializer_capable_node(%{node_capabilities: caps}) do
    case Map.get(caps, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  defp create_materializer_on_node(node, shard_tag, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()

    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    case create_worker_fn.(foreman_ref, worker_id, :materializer, timeout: 30_000) do
      {:ok, worker_ref} -> {:ok, {worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, shard_tag}}
    end
  end

  defp logs_for_shard(logs, shard_id, context) do
    if consistent_hashing_logs?(logs) do
      LayoutRouting.log_subset_for_shard(logs, shard_id, desired_replication_factor(context, logs))
    else
      filter_logs_for_shard(logs, shard_id)
    end
  end

  defp consistent_hashing_logs?(logs) do
    Enum.all?(logs, fn {_log_id, descriptor} -> descriptor == [] end)
  end

  defp desired_replication_factor(context, logs) do
    get_in(context, [:cluster_config, :parameters, :desired_replication_factor]) || map_size(logs)
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
    system_shard = RecoveryAttempt.system_shard_id()

    unlock_materializer_for_shard(
      materializer_pid,
      system_shard,
      recovery_attempt,
      recovery_attempt.durable_version,
      context
    )
  end

  defp unlock_materializer_for_shard(materializer_pid, shard_tag, recovery_attempt, durable_version, context) do
    shard_logs = logs_for_shard(recovery_attempt.logs, shard_tag, context)

    tsl = %{
      id: TransactionSystemLayout.random_id(),
      epoch: recovery_attempt.epoch,
      director: :unavailable,
      sequencer: recovery_attempt.sequencer,
      rate_keeper: nil,
      proxies: recovery_attempt.proxies,
      resolvers: recovery_attempt.resolvers,
      logs: shard_logs,
      services: recovery_attempt.transaction_services
    }

    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(materializer_pid, durable_version, tsl) do
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
    timeout_ms = Map.get(context, :shard_layout_timeout_ms, @catchup_timeout_ms)
    poll_interval_ms = Map.get(context, :shard_layout_poll_interval_ms, @catchup_poll_interval_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    get_layout_fn = Map.get(context, :get_shard_layout_fn, &default_get_shard_layout/2)

    do_get_shard_layout(materializer_pid, read_version, get_layout_fn, deadline, poll_interval_ms)
  end

  defp do_get_shard_layout(materializer_pid, read_version, get_layout_fn, deadline, poll_interval_ms) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :shard_layout_timeout}
    else
      case get_layout_fn.(materializer_pid, read_version) do
        {:ok, shard_layout} when map_size(shard_layout) > 0 ->
          {:ok, shard_layout}

        {:error, reason} when reason in [:timeout, :version_too_new] ->
          Process.sleep(poll_interval_ms)
          do_get_shard_layout(materializer_pid, read_version, get_layout_fn, deadline, poll_interval_ms)

        {:error, {:shard_layout_not_found, _}} ->
          Process.sleep(poll_interval_ms)
          do_get_shard_layout(materializer_pid, read_version, get_layout_fn, deadline, poll_interval_ms)

        {:error, reason} ->
          {:error, reason}

        {:failure, reason, _ref} ->
          {:error, reason}
      end
    end
  end

  defp default_get_shard_layout(materializer_pid, read_version) do
    with {:ok, {metadata_entries, _more}} <- get_range(materializer_pid, SystemKeys.shards_prefix(), read_version),
         {:ok, shard_layout} <- build_shard_layout_from_entries(metadata_entries) do
      {:ok, shard_layout}
    else
      {:error, {:shard_layout_not_found, :empty_metadata}} ->
        get_legacy_shard_layout(materializer_pid, read_version)

      {:error, reason} ->
        {:error, reason}

      {:failure, reason, _ref} ->
        {:error, reason}
    end
  end

  defp get_legacy_shard_layout(materializer_pid, read_version) do
    with {:ok, {entries, _more}} <- get_range(materializer_pid, SystemKeys.shard_keys_prefix(), read_version),
         {:ok, shard_layout} <- build_legacy_shard_layout(entries) do
      {:ok, shard_layout}
    else
      {:error, reason} -> {:error, reason}
      {:failure, reason, _ref} -> {:error, reason}
    end
  end

  defp get_range(materializer_pid, prefix, version) do
    end_key = prefix <> <<0xFF, 0xFF, 0xFF, 0xFF>>

    Materializer.get_range(
      materializer_pid,
      prefix,
      end_key,
      version,
      limit: 1000,
      timeout: @shard_layout_read_timeout_ms
    )
  end

  defp build_shard_layout_from_entries([]), do: {:error, {:shard_layout_not_found, :empty_metadata}}

  defp build_shard_layout_from_entries(entries) do
    shard_layout =
      Enum.reduce(entries, %{}, fn {key, value}, acc ->
        case decode_shard_metadata_entry(key, value) do
          {:ok, {end_key, {tag, start_key}}} ->
            Map.put(acc, end_key, {tag, start_key})

          :ignore ->
            acc
        end
      end)

    if map_size(shard_layout) > 0 do
      {:ok, shard_layout}
    else
      {:error, {:shard_layout_not_found, :empty_metadata}}
    end
  end

  defp decode_shard_metadata_entry(key, value) do
    with {:shard, tag_string} <- SystemKeys.parse_key(key),
         {tag, ""} <- Integer.parse(tag_string),
         {:ok, metadata} <- ShardMetadata.read(value),
         0 <- metadata.ended_at do
      start_key = IO.iodata_to_binary(metadata.start_key)
      end_key = IO.iodata_to_binary(metadata.end_key)
      {:ok, {end_key, {tag, start_key}}}
    else
      {:ok, %{ended_at: _ended_at}} -> :ignore
      _ -> :ignore
    end
  end

  defp build_legacy_shard_layout([]), do: {:error, {:shard_layout_not_found, :empty_legacy_keys}}

  defp build_legacy_shard_layout(entries) do
    shard_layout =
      entries
      |> Enum.sort_by(fn {key, _value} -> extract_end_key_from_shard_key(key) end)
      |> Enum.reduce({%{}, <<>>}, fn {key, value}, {acc, start_key} ->
        end_key = extract_end_key_from_shard_key(key)

        next_entry =
          case decode_legacy_shard_value(value, start_key) do
            {:ok, {tag, resolved_start_key}} ->
              {Map.put(acc, end_key, {tag, resolved_start_key}), end_key}

            :ignore ->
              {acc, start_key}
          end

        next_entry
      end)
      |> elem(0)

    if map_size(shard_layout) > 0 do
      {:ok, shard_layout}
    else
      {:error, {:shard_layout_not_found, :empty_legacy_keys}}
    end
  end

  defp resolver_descriptors_for_shard_layout(shard_layout) when is_map(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {_tag, start_key} -> start_key end)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.with_index(1)
    |> Enum.map(fn {start_key, index} -> resolver_descriptor(start_key, {:vacancy, index}) end)
  end

  defp extract_end_key_from_shard_key(key) do
    prefix = SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_legacy_shard_value(value, inferred_start_key) when is_binary(value) do
    case :erlang.binary_to_term(value) do
      {tag, start_key} when is_integer(tag) and is_binary(start_key) ->
        {:ok, {tag, start_key}}

      tag when is_integer(tag) ->
        {:ok, {tag, inferred_start_key}}

      _ ->
        :ignore
    end
  end

  defp shard_label(shard_tag) do
    if shard_tag == RecoveryAttempt.system_shard_id() do
      "System shard"
    else
      "Shard #{shard_tag}"
    end
  end
end
