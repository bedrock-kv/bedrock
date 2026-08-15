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

  # Create worker via Foreman for a specific shard. The shard assignment
  # travels in the worker's params — it is how the materializer knows which
  # ShardServer stream is its own.
  defp create_materializer_worker(node, shard_tag, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    case create_worker_fn.(foreman_ref, worker_id, :materializer,
           timeout: 30_000,
           params: %{"shard_id" => shard_tag}
         ) do
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
    shard_logs = filter_logs_for_shard(recovery_attempt.logs, shard_tag)

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
    # Read at the newest version determined during log recovery planning;
    # this is also the cluster's rollback point, and therefore the version
    # materializers are unlocked with. (The recovery durable_version — the
    # WAL trim floor — is NOT a rollback target: it regresses to zero on
    # restart by design, and rolling a populated materializer back to it
    # would discard real state.)
    {_oldest, recovery_version} = recovery_attempt.version_vector

    # Materializers were locked during the locking phase; their recovery
    # info carries their shard assignments. Reuse them — they hold the
    # durable state, including the shard layout this phase exists to read.
    existing_by_shard = existing_materializers_by_shard(recovery_attempt)

    with {:ok, materializer_service} <- find_or_create_materializer(existing_by_shard, recovery_attempt, context),
         {:ok, materializer_pid} <- lock_materializer(materializer_service, recovery_attempt.epoch, context),
         # Unlock with logs so it streams the replayed WAL from the demux
         :ok <-
           unlock_and_start_pulling(
             materializer_pid,
             RecoveryAttempt.system_shard_id(),
             recovery_version,
             recovery_attempt,
             context
           ),
         # It must be able to SERVE the layout query: wait on the applied
         # position, which the stream advances (durability trails by design)
         :ok <- wait_for_materializer_catchup(materializer_pid, recovery_version, context),
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, recovery_version, context),
         {:ok, shard_materializers} <-
           ensure_materializers_for_shards(
             shard_layout,
             existing_by_shard,
             %{RecoveryAttempt.system_shard_id() => materializer_pid},
             recovery_version,
             recovery_attempt,
             context
           ) do
      updated_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, materializer_pid)
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(:shard_materializers, shard_materializers)

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  # The locking phase locked every advertised materializer and collected its
  # recovery info — including its shard assignment. Index the survivors by
  # shard, with their service refs from the transaction services map.
  defp existing_materializers_by_shard(recovery_attempt) do
    recovery_attempt.materializer_recovery_info_by_id
    |> Enum.flat_map(fn {id, info} ->
      with shard_id when is_integer(shard_id) <- Map.get(info, :shard_id),
           %{status: {:up, ref}} <- Map.get(recovery_attempt.transaction_services, id) do
        [{shard_id, {:materializer, ref}}]
      else
        _ -> []
      end
    end)
    |> Map.new()
  end

  # Reuse the locked system-shard materializer; create one only when none
  # survived (a genuinely lost materializer team).
  defp find_or_create_materializer(existing_by_shard, recovery_attempt, context) do
    case Map.fetch(existing_by_shard, RecoveryAttempt.system_shard_id()) do
      {:ok, service} ->
        {:ok, service}

      :error ->
        Logger.info("System shard materializer not found, creating new one")
        create_materializer(recovery_attempt, context)
    end
  end

  # Every shard in the layout needs a materializer in the new TSL: reuse the
  # survivors (unlocking each so it streams), create only the missing.
  defp ensure_materializers_for_shards(
         shard_layout,
         existing_by_shard,
         already_started,
         recovery_version,
         recovery_attempt,
         context
       ) do
    shard_layout
    |> extract_shard_tags()
    |> Enum.reduce_while({:ok, already_started}, fn shard_tag, {:ok, acc} ->
      case Map.fetch(acc, shard_tag) do
        {:ok, _pid} ->
          {:cont, {:ok, acc}}

        :error ->
          shard_tag
          |> start_materializer_for_shard(existing_by_shard, recovery_version, recovery_attempt, context)
          |> case do
            {:ok, pid} -> {:cont, {:ok, Map.put(acc, shard_tag, pid)}}
            {:error, reason} -> {:halt, {:error, {shard_tag, reason}}}
          end
      end
    end)
  end

  defp start_materializer_for_shard(shard_tag, existing_by_shard, recovery_version, recovery_attempt, context) do
    case Map.fetch(existing_by_shard, shard_tag) do
      {:ok, service} ->
        with {:ok, pid} <- lock_materializer(service, recovery_attempt.epoch, context),
             :ok <- unlock_and_start_pulling(pid, shard_tag, recovery_version, recovery_attempt, context) do
          {:ok, pid}
        end

      :error ->
        Logger.info("Materializer for shard #{shard_tag} not found, creating new one")
        create_and_start_materializer(shard_tag, recovery_attempt, context)
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
    case create_worker_fn.(foreman_ref, worker_id, :materializer,
           timeout: 30_000,
           params: %{"shard_id" => system_shard}
         ) do
      {:ok, worker_ref} -> {:ok, {worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, system_shard}}
    end
  end

  # Filter logs to only those relevant for the given shard (by tag)
  defp filter_logs_for_shard(logs, shard_id) do
    logs
    |> Enum.filter(fn {_log_id, tags} -> log_routes_to_shard?(tags, shard_id) end)
    |> Map.new()
  end

  defp log_routes_to_shard?([], _shard_id), do: true
  defp log_routes_to_shard?(tags, shard_id), do: shard_id in tags

  # Unlock materializer with only the logs it needs to start pulling. The
  # version is the recovery (rollback) version — vector last — never the
  # durable floor, which regresses to zero on restart by design.
  defp unlock_and_start_pulling(materializer_pid, shard_tag, recovery_version, recovery_attempt, context) do
    shard_logs = filter_logs_for_shard(recovery_attempt.logs, shard_tag)

    # TransactionSystemLayout is a type, not a struct, so we build a map
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

    case unlock_fn.(materializer_pid, recovery_version, tsl) do
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

      # The applied position, not the durable one: durability is clamped to
      # the known-committed version and trails by design; what matters here
      # is that the materializer can SERVE the layout query.
      case info_fn.(pid, [:current_version]) do
        {:ok, %{current_version: v}} when is_binary(v) and v >= target_version ->
          Logger.debug("Materializer caught up to version #{inspect(v)}")
          :ok

        {:ok, %{current_version: v}} ->
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
