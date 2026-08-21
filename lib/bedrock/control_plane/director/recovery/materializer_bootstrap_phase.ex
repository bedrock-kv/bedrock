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
  4. Unlock it with its replica set of pull sources to start pulling
  5. Wait for materializer to catch up (60s timeout)
  6. Query shard layout from `\\xff/system/shard_keys/*`

  Stalls if the materializer is unavailable and cannot be created, or if catchup
  times out. Transitions to CommitProxyStartupPhase with the materializer pid and
  shard layout.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys.Values

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
      {:ok, shard_materializers, created_services} ->
        updated_attempt =
          recovery_attempt
          |> Map.put(:shard_layout, shard_layout)
          |> Map.put(:shard_materializers, to_materializer_refs(shard_materializers))
          # Provenance for the persistence phase: this recovery INVENTED
          # the layout (fresh cluster), so it seeds the durable families;
          # the empty prior means every assignment writes.
          |> Map.put(:seeded_layout?, true)
          |> Map.put(:prior_materializer_refs, %{})
          |> Map.update!(:transaction_services, &Map.merge(&1, created_services))

        {updated_attempt, CommitProxyStartupPhase}

      {:error, reason} ->
        Logger.warning("Failed to create materializers for fresh cluster: #{inspect(reason)}")
        {recovery_attempt, {:stalled, {:materializer_creation_failed, reason}}}
    end
  end

  # The one projection, at the one boundary it belongs: the phase
  # orchestrates with live pids (lock, unlock, catchup), but the attempt
  # carries the refs exactly as every reader consumes them — the
  # keyspace-value shape {worker_id, node}, both strings. The persistence
  # writer and the routing-snapshot seed embed this map verbatim, so the
  # seed and the keyspace are the same map read twice; ghost pruning
  # takes its worker ids from it directly. A pid is phase-local
  # orchestration state, not a fact any reader needs — a future consumer
  # that wants one should ask where it lives (the directory, or the
  # worker), not have it carried speculatively.
  @spec to_materializer_refs(%{Bedrock.range_tag() => {Worker.id(), node(), pid()}}) ::
          %{Bedrock.range_tag() => {Worker.id(), String.t()}}
  defp to_materializer_refs(shard_materializers) do
    Map.new(shard_materializers, fn {tag, {worker_id, node, _pid}} ->
      {tag, {worker_id, Atom.to_string(node)}}
    end)
  end

  # Extract unique shard tags from shard_layout
  defp extract_shard_tags(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
  end

  # Create materializers for multiple shards, collecting both the pid map
  # and the service records of what was created — a created worker that
  # never reaches transaction_services never reaches the layout or the
  # materializers keyspace, and rejoin validation would retire the
  # workers recovery just made.
  defp create_materializers_for_shards(shard_tags, recovery_attempt, context) do
    Enum.reduce_while(shard_tags, {:ok, %{}, %{}}, fn shard_tag, {:ok, by_shard, services} ->
      case create_and_start_materializer(shard_tag, recovery_attempt, context) do
        {:ok, {_worker_id, _node, _pid} = assignment, {worker_id, descriptor}} ->
          {:cont, {:ok, Map.put(by_shard, shard_tag, assignment), Map.put(services, worker_id, descriptor)}}

        {:error, reason} ->
          {:halt, {:error, {shard_tag, reason}}}
      end
    end)
  end

  # Create a materializer for a specific shard and start it pulling.
  # Returns the {worker_id, node, pid} assignment (the worker id rides the
  # assignment from creation — it is never recovered by inverting a
  # services map) plus the service record ({id, descriptor}) that must
  # travel into transaction_services so the layout references the creation.
  defp create_and_start_materializer(shard_tag, recovery_attempt, context) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {worker_id, worker_ref, node}} <-
           create_materializer_worker(node, shard_tag, recovery_attempt, context),
         {:ok, pid} <-
           lock_new_materializer({:materializer, {worker_ref, node}, shard_tag}, recovery_attempt.epoch, context),
         :ok <- start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
      descriptor = %{kind: :materializer, last_seen: {worker_ref, node}, status: {:up, pid}}
      {:ok, {worker_id, node, pid}, {worker_id, descriptor}}
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
      {:ok, worker_ref} -> {:ok, {worker_id, worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, shard_tag}}
    end
  end

  # Lock a newly created materializer
  defp lock_new_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  # Start materializer pulling from its replica set of logs
  defp start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
    # For fresh cluster, start from version zero
    durable_version = Bedrock.DataPlane.Version.zero()
    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(pid, durable_version, pull_sources_for_shard(shard_tag, recovery_attempt)) do
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
    {_available_after, recovery_version} = recovery_attempt.version_vector

    # Materializers were locked during the locking phase; their recovery
    # info carries their shard assignments. Reuse them — they hold the
    # durable state, including the shard layout this phase exists to read.
    existing_by_shard = existing_materializers_by_shard(recovery_attempt)

    with {:ok, {system_worker_id, materializer_service}, created_system} <-
           find_or_create_materializer(existing_by_shard, recovery_attempt, context),
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
         :ok <-
           wait_for_materializer_catchup(
             materializer_pid,
             {RecoveryAttempt.system_shard_id(), recovery_attempt.attempt},
             recovery_version,
             context
           ),
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, recovery_version, context),
         {:ok, prior_refs} <- read_prior_refs(materializer_pid, recovery_version, context),
         {:ok, shard_materializers, created_services} <-
           ensure_materializers_for_shards(
             shard_layout,
             prefer_family_named(existing_by_shard, prior_refs, recovery_attempt),
             %{
               RecoveryAttempt.system_shard_id() => {system_worker_id, node(materializer_pid), materializer_pid}
             },
             recovery_version,
             recovery_attempt,
             context
           ) do
      # Every creation must reach transaction_services: the layout and the
      # materializers keyspace are built from it, and workers self-retire
      # when the committed state doesn't name them — including workers
      # recovery itself just made.
      all_created =
        Map.merge(created_services, system_service_entry(created_system, materializer_pid))

      updated_attempt =
        recovery_attempt
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(:shard_materializers, to_materializer_refs(shard_materializers))
        # Provenance for the persistence phase: the layout was READ from
        # the durable family (nothing to rewrite), and the prior refs are
        # the diff base for materializer writes.
        |> Map.put(:seeded_layout?, false)
        |> Map.put(:prior_materializer_refs, prior_refs)
        |> Map.put(:resolvers, resolver_descriptors_for_layout(shard_layout))
        |> Map.update!(:transaction_services, &Map.merge(&1, all_created))

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  defp system_service_entry(nil, _pid), do: %{}

  defp system_service_entry({worker_id, last_seen}, pid),
    do: %{worker_id => %{kind: :materializer, last_seen: last_seen, status: {:up, pid}}}

  # Resolvers are recreated each epoch, one per shard range in the
  # recovered layout — the same construction the fresh-cluster path uses,
  # driven by the recovered layout instead of the default one.
  defp resolver_descriptors_for_layout(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {_tag, start_key} -> start_key end)
    |> Enum.sort()
    |> Enum.with_index(1)
    |> Enum.map(fn {start_key, index} -> resolver_descriptor(start_key, {:vacancy, index}) end)
  end

  # The locking phase locked every advertised materializer and collected its
  # recovery info — including its shard assignment. Index the survivors by
  # shard, with their service refs from the transaction services map. When
  # several claim the same shard (e.g. empty strays created by earlier
  # failed recovery attempts), the most-advanced durable state wins.
  defp existing_materializers_by_shard(recovery_attempt) do
    recovery_attempt.materializer_recovery_info_by_id
    |> Enum.flat_map(fn {id, info} ->
      with shard_id when is_integer(shard_id) <- Map.get(info, :shard_id),
           %{status: {:up, ref}} <- Map.get(recovery_attempt.transaction_services, id) do
        [{shard_id, Map.get(info, :durable_version), {id, {:materializer, ref}}}]
      else
        _ -> []
      end
    end)
    |> Enum.group_by(fn {shard_id, _durable, _entry} -> shard_id end)
    |> Map.new(fn {shard_id, candidates} ->
      {_shard, _durable, entry} = Enum.max_by(candidates, fn {_shard, durable, _entry} -> durable end)
      {shard_id, entry}
    end)
  end

  # Reuse the locked system-shard materializer; create one only when none
  # survived (a genuinely lost materializer team). A creation also returns
  # {worker_id, last_seen} so the caller can record it in the transaction
  # services once the lock provides the pid.
  defp find_or_create_materializer(existing_by_shard, recovery_attempt, context) do
    case Map.fetch(existing_by_shard, RecoveryAttempt.system_shard_id()) do
      {:ok, {worker_id, service}} ->
        {:ok, {worker_id, service}, nil}

      :error ->
        Logger.info("System shard materializer not found, creating new one")

        with {:ok, node} <- find_materializer_capable_node(context),
             {:ok, {worker_id, worker_ref, node}} <-
               create_materializer_worker(node, RecoveryAttempt.system_shard_id(), recovery_attempt, context) do
          {:ok, {worker_id, {:materializer, {worker_ref, node}}}, {worker_id, {worker_ref, node}}}
        end
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
    |> Enum.reduce_while({:ok, already_started, %{}}, fn shard_tag, {:ok, acc, services} ->
      case Map.fetch(acc, shard_tag) do
        {:ok, _assignment} ->
          {:cont, {:ok, acc, services}}

        :error ->
          shard_tag
          |> start_materializer_for_shard(existing_by_shard, recovery_version, recovery_attempt, context)
          |> case do
            {:ok, assignment, created} ->
              {:cont, {:ok, Map.put(acc, shard_tag, assignment), Map.merge(services, created)}}

            {:error, reason} ->
              {:halt, {:error, {shard_tag, reason}}}
          end
      end
    end)
  end

  defp start_materializer_for_shard(shard_tag, existing_by_shard, recovery_version, recovery_attempt, context) do
    case Map.fetch(existing_by_shard, shard_tag) do
      {:ok, {worker_id, service}} ->
        with {:ok, pid} <- lock_materializer(service, recovery_attempt.epoch, context),
             :ok <- unlock_and_start_pulling(pid, shard_tag, recovery_version, recovery_attempt, context) do
          {:ok, {worker_id, node(pid), pid}, %{}}
        end

      :error ->
        Logger.info("Materializer for shard #{shard_tag} not found, creating new one")

        with {:ok, assignment, {worker_id, descriptor}} <-
               create_and_start_materializer(shard_tag, recovery_attempt, context) do
          {:ok, assignment, %{worker_id => descriptor}}
        end
    end
  end

  # The committed materializers/ family is the re-adoption authority: a
  # family-named worker that this epoch's locking phase actually locked
  # (and whose own shard assignment agrees) is preferred over the
  # most-advanced-durable contest, which remains the fallback for tags
  # the family does not name — and for tag 0, whose selection happens
  # before the family can be read (the family lives IN the system shard).
  defp prefer_family_named(existing_by_shard, prior_refs, recovery_attempt) do
    named =
      for {tag, {worker_id, _node}} <- prior_refs,
          match?(%{shard_id: ^tag}, Map.get(recovery_attempt.materializer_recovery_info_by_id, worker_id)),
          %{status: {:up, ref}} <- [Map.get(recovery_attempt.transaction_services, worker_id)],
          into: %{},
          do: {tag, {worker_id, {:materializer, ref}}}

    Map.merge(existing_by_shard, named)
  end

  # Read the durable materializers/ family at the recovery version: the
  # re-adoption input and the persistence phase's diff base. Read from
  # the same materializer, at the same version, as the shard layout — a
  # torn view is impossible (the families rewrite transactionally).
  defp read_prior_refs(materializer_pid, read_version, context) do
    read_fn = Map.get(context, :read_prior_refs_fn, &default_read_prior_refs/2)
    read_fn.(materializer_pid, read_version)
  end

  defp default_read_prior_refs(materializer_pid, read_version) do
    prefix = Bedrock.SystemKeys.materializers_prefix()
    {_range_start, range_end} = Bedrock.KeyRange.from_prefix(prefix)

    range_read_fn = fn start_key ->
      Materializer.get_range(materializer_pid, start_key, range_end, read_version, limit: 1000)
    end

    with {:ok, entries} <- page_entries(range_read_fn, prefix, :prior_refs_query_failed, []) do
      decode_prior_refs(entries)
    end
  end

  @doc false
  # Public for tests: the default in-recovery reader is injected away in
  # unit tests, so the decode contract is pinned directly.
  @spec decode_prior_refs([{Bedrock.key(), binary()}]) ::
          {:ok, %{Bedrock.range_tag() => {Worker.id(), String.t()}}}
          | {:error, {:invalid_materializer_entry, Bedrock.key()}}
  def decode_prior_refs(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      with {:materializer_key, tag} <- Bedrock.SystemKeys.parse_key(key),
           {:ok, ref} <- Values.decode_materializer_ref(value) do
        {:cont, {:ok, Map.put(acc, tag, ref)}}
      else
        _ -> {:halt, {:error, {:invalid_materializer_entry, key}}}
      end
    end)
  end

  # Find a node that can host materializers
  defp find_materializer_capable_node(%{node_capabilities: caps}) do
    case Map.get(caps, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  # The unlock seed: this shard's replica set as {log_id, log_ref} pairs,
  # resolved once here through the same ShardRouter walk the commit
  # proxies route with. The materializer receives exactly its own
  # sources — never the cluster's services map (FDB gives each storage
  # server its own tag and assignments, not ServerDBInfo's membership).
  @spec pull_sources_for_shard(non_neg_integer(), RecoveryAttempt.t()) :: Materializer.pull_sources()
  defp pull_sources_for_shard(shard_tag, recovery_attempt) do
    logs = recovery_attempt.logs
    services = recovery_attempt.transaction_services
    replica_set = ShardRouter.log_ids_for_tag(shard_tag, ShardRouter.log_map(Map.keys(logs)), max(1, map_size(logs)))

    sources =
      Enum.flat_map(replica_set, fn log_id ->
        case Map.get(services, log_id) do
          %{status: {:up, ref}} -> [{log_id, ref}]
          _ -> []
        end
      end)

    # Recruitment records every log with an up ref, so a shrunken seed
    # means the attempt's own bookkeeping disagrees with itself — worth a
    # trail, since the materializer would wait on the missing replicas
    # with no director-side symptom.
    if length(sources) < length(replica_set) do
      missing = replica_set -- Enum.map(sources, fn {log_id, _ref} -> log_id end)
      Logger.warning("Pull-source seed for shard #{shard_tag} is missing log refs: #{inspect(missing)}")
    end

    sources
  end

  # Unlock the materializer with its pull sources. The version is the
  # recovery (rollback) version — vector last — never the durable floor,
  # which regresses to zero on restart by design.
  defp unlock_and_start_pulling(materializer_pid, shard_tag, recovery_version, recovery_attempt, context) do
    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(materializer_pid, recovery_version, pull_sources_for_shard(shard_tag, recovery_attempt)) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason}}
    end
  end

  defp default_unlock_materializer(pid, durable_version, pull_sources) do
    Materializer.unlock_after_recovery(pid, durable_version, pull_sources, timeout_in_ms: 30_000)
  end

  # Poll until materializer reaches target version. The label — shard and
  # recovery attempt — makes repeated lines attributable: recovery retries
  # re-run this phase, and without identity in the log a burst of
  # "caught up" lines is indistinguishable from a bug.
  defp wait_for_materializer_catchup(pid, label, target_version, context) do
    timeout_ms = Map.get(context, :catchup_timeout_ms, @catchup_timeout_ms)
    poll_interval_ms = Map.get(context, :catchup_poll_interval_ms, @catchup_poll_interval_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_wait_for_catchup(pid, label, target_version, deadline, poll_interval_ms, context)
  end

  defp do_wait_for_catchup(pid, {shard_tag, attempt} = label, target_version, deadline, poll_interval_ms, context) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :catchup_timeout}
    else
      info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

      # The applied position, not the durable one: durability is clamped to
      # the known-committed version and trails by design; what matters here
      # is that the materializer can SERVE the layout query.
      case info_fn.(pid, [:current_version]) do
        {:ok, %{current_version: v}} when is_binary(v) and v >= target_version ->
          Logger.debug(
            "Materializer caught up to version #{inspect(v)} " <>
              "(shard #{shard_tag}, #{inspect(pid)}, recovery attempt ##{attempt})"
          )

          :ok

        {:ok, %{current_version: v}} ->
          Logger.debug(
            "Materializer at version #{inspect(v)}, waiting for #{inspect(target_version)} " <>
              "(shard #{shard_tag}, #{inspect(pid)}, recovery attempt ##{attempt})"
          )

          Process.sleep(poll_interval_ms)
          do_wait_for_catchup(pid, label, target_version, deadline, poll_interval_ms, context)

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

  # Read the shard_keys/ family via paged range reads. The family is
  # unbounded in shard count, and a truncated boundary map is not a
  # degraded layout — it is a WRONG one (missing shards read as holes in
  # the keyspace), so the continuation must be drained, never dropped.
  defp default_get_shard_layout(materializer_pid, read_version) do
    # The same bound construction the writer uses (persistence phase's
    # clear_prefix), so reader and writer ranges are definitionally
    # identical rather than two hand-rolled sentinels kept in agreement.
    {_range_start, range_end} = Bedrock.KeyRange.from_prefix(Bedrock.SystemKeys.shard_keys_prefix())

    range_read_fn = fn start_key ->
      Materializer.get_range(materializer_pid, start_key, range_end, read_version, limit: 1000)
    end

    case read_all_shard_entries(range_read_fn) do
      {:ok, entries} -> shard_layout_from_entries(entries)
      {:error, _} = error -> error
    end
  end

  @doc false
  # Pages a system-family range read to exhaustion, resuming each page
  # immediately after the last returned key. Any failure mid-continuation
  # fails the whole read: partial success would BE the silent truncation
  # this exists to preclude. An empty page claiming more is a broken read
  # contract and is surfaced as an error rather than looped on.
  @spec read_all_shard_entries((Bedrock.key() ->
                                  {:ok, {[{Bedrock.key(), binary()}], more :: boolean()}}
                                  | {:error, term()}
                                  | {:failure, term(), term()})) ::
          {:ok, [{Bedrock.key(), binary()}]} | {:error, {:shard_layout_query_failed, term()}}
  def read_all_shard_entries(range_read_fn),
    do: page_entries(range_read_fn, Bedrock.SystemKeys.shard_keys_prefix(), :shard_layout_query_failed, [])

  defp page_entries(range_read_fn, start_key, error_tag, pages) do
    case range_read_fn.(start_key) do
      {:ok, {entries, false}} ->
        {:ok, [entries | pages] |> Enum.reverse() |> Enum.concat()}

      {:ok, {[], true}} ->
        {:error, {error_tag, :empty_continuation_page}}

      {:ok, {entries, true}} ->
        {last_key, _value} = List.last(entries)
        page_entries(range_read_fn, Bedrock.Key.key_after(last_key), error_tag, [entries | pages])

      {:error, reason} ->
        {:error, {error_tag, reason}}

      {:failure, reason, _ref} ->
        {:error, {error_tag, reason}}
    end
  end

  @doc false
  # Key format: \xff/system/shard_keys/<end_key>; value: {tag, start_key}
  # (see Bedrock.SystemKeys.Values). The value CARRIES the start key and
  # this reader consumes it — the same meaning RoutingData.apply_mutation
  # gives it; two readers of one family must not disagree about what the
  # value means. Adjacency reconstruction (each shard starts where the
  # previous ends) survives only for legacy term_to_binary snapshots
  # that predate carried start keys. Recovery no longer rewrites the
  # family (read-and-heal, bedrock-q67.21.2), so a legacy family stays
  # legacy — encoding-uniform, which the any-legacy-falls-back-whole
  # rule covers — until bedrock-q67.20.7 retires the fallback with an
  # explicit one-time migration.
  @spec shard_layout_from_entries([{Bedrock.key(), binary()}]) ::
          {:ok, RecoveryAttempt.shard_layout()} | {:error, {:invalid_shard_value, Bedrock.key()}}
  def shard_layout_from_entries(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case decode_shard_entry(value) do
        {:ok, decoded} -> {:cont, {:ok, [{extract_end_key_from_shard_key(key), decoded} | acc]}}
        {:error, _} -> {:halt, {:error, {:invalid_shard_value, key}}}
      end
    end)
    |> case do
      {:ok, decoded_by_end_key} -> {:ok, build_shard_layout(decoded_by_end_key)}
      error -> error
    end
  end

  defp build_shard_layout(decoded_by_end_key) do
    if Enum.any?(decoded_by_end_key, &match?({_end_key, {:legacy, _tag}}, &1)) do
      rebuild_start_keys_by_adjacency(decoded_by_end_key)
    else
      Map.new(decoded_by_end_key, fn {end_key, {tag, start_key}} -> {end_key, {tag, start_key}} end)
    end
  end

  defp rebuild_start_keys_by_adjacency(decoded_by_end_key) do
    decoded_by_end_key
    |> Enum.sort_by(fn {end_key, _decoded} -> end_key end)
    |> Enum.map_reduce(<<>>, fn {end_key, decoded}, start_key ->
      tag =
        case decoded do
          {:legacy, tag} -> tag
          {tag, _carried_start} -> tag
        end

      {{end_key, {tag, start_key}}, end_key}
    end)
    |> elem(0)
    |> Map.new()
  end

  defp extract_end_key_from_shard_key(key) do
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_shard_entry(value) do
    case Values.decode_shard_key_entry(value) do
      {:ok, {tag, start_key}} -> {:ok, {tag, start_key}}
      {:error, _} -> decode_legacy_shard_tag(value)
    end
  end

  # Clusters created before Bedrock.SystemKeys.Values wrote shard_key
  # values with term_to_binary. Recovery no longer rewrites the family,
  # so these values persist AS-IS until bedrock-q67.20.7's explicit
  # migration retires them along with this fallback.
  defp decode_legacy_shard_tag(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      tag when is_integer(tag) -> {:ok, {:legacy, tag}}
      {tag, _start_key} when is_integer(tag) -> {:ok, {:legacy, tag}}
      _ -> {:error, :invalid_encoding}
    end
  rescue
    ArgumentError -> {:error, :invalid_encoding}
  end
end
