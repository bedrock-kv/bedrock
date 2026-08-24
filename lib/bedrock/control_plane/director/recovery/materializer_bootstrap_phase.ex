defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase do
  @moduledoc """
  Brings up the system shard, and stops there.

  Tag 0 holds the cluster's metadata — the shard layout and materializer
  membership — so recovery cannot read either until a tag-0 materializer
  is serving. That is this phase's entire job.

  ## Existing cluster

  1. Look up the system materializer named by the prior core state. It is
     LOOKED UP, never discovered and never invented: an unavailable named
     member stalls (bedrock-q67.21.12).
  2. Lock it into this epoch, unlock it with its replica set of pull
     sources, and wait for it to reach the recovery version.
  3. Read the shard layout from `\\xff/system/shard_keys/*` and the
     committed tag-0 members from `\\xff/system/materializers/0/*`.

  ## Fresh cluster

  No prior epoch's data exists, so recovery seeds: the default two-shard
  layout (tag 0 for system keys, tag 1 for user keys) and one created
  materializer for tag 0.

  ## What it deliberately does not do

  The layout names data tags, and the locking phase locked their
  materializers into this epoch — but recovery does not seat, unlock, or
  record any of them. That belongs to the distributor, whose startup
  sweep verifies every committed member is in service, adopts the ones
  that are not, heals what it cannot adopt, and covers gaps with the
  placeholder, after every recovery regardless (bedrock-q67.21.13).

  This is FDB's division. Its recovery touches storage servers through a
  single key — `\\xff/lastEpochEnd`
  (`ClusterRecovery.actor.cpp:1692-1698`) — and storage learns the epoch
  in-band from the mutation stream; `RecoveryState::STORAGE_RECOVERED`
  (`:519-520`) is a status label recovery fires when the old tlog
  generations drop away. Recovery observes storage. It does not drive it.

  Stalls if the named system materializer is unavailable, if catchup
  times out, or if either family read fails. Transitions to
  `CommitProxyStartupPhase`.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys.Reader

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

  # A prior record naming no logs means there is no prior epoch's data to
  # recover, so recovery seeds instead of reading. Read the key
  # STRICTLY: the context type declares it required, and the two answers
  # here are "invent a default layout" and "read the committed one" — a
  # missing key must raise, not silently pick the destructive one.
  defp fresh_cluster?(context), do: CoreState.fresh?(context.prior_core_state)

  defp handle_fresh_cluster(recovery_attempt, context) do
    Logger.debug("Fresh cluster detected, using default shard layout")

    shard_layout = default_shard_layout()

    # The system shard is the only one recovery creates. The layout it
    # just invented names a data tag too, but that gap is the
    # distributor's to cover — its sweep places the placeholder over it
    # and demand recruits a real worker. Recovery does not manufacture
    # data-plane coverage it has no use for.
    case create_and_start_materializer(RecoveryAttempt.system_shard_id(), recovery_attempt, context) do
      {:ok, {worker_id, node, _pid}, {worker_id, descriptor}} ->
        updated_attempt =
          recovery_attempt
          |> Map.put(:shard_layout, shard_layout)
          |> Map.put(
            :shard_materializers,
            %{RecoveryAttempt.system_shard_id() => %{worker_id => Atom.to_string(node)}}
          )
          # Provenance for the persistence phase: this recovery INVENTED
          # the layout (fresh cluster), so it seeds the durable families;
          # the empty prior means every assignment writes.
          |> Map.put(:seeded_layout?, true)
          |> Map.put(:prior_materializer_refs, %{})
          # The creation must reach transaction_services: the layout and
          # the materializers keyspace are built from it, and a worker the
          # committed state does not name retires itself.
          |> Map.update!(:transaction_services, &Map.put(&1, worker_id, descriptor))

        {updated_attempt, CommitProxyStartupPhase}

      {:error, reason} ->
        Logger.warning("Failed to create the system materializer for a fresh cluster: #{inspect(reason)}")

        {recovery_attempt, {:stalled, {:materializer_creation_failed, {RecoveryAttempt.system_shard_id(), reason}}}}
    end
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

    with {:ok, {system_worker_id, materializer_service}} <-
           resolve_system_materializer(recovery_attempt, context),
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
         {:ok, prior_refs} <- read_prior_refs(materializer_pid, recovery_version, context) do
      # Recovery stops here. The layout names data tags, and the locking
      # phase locked their materializers into this epoch, but seating them
      # is the distributor's job — its startup sweep verifies every
      # committed member is in service, adopts the ones that are not,
      # heals what it cannot adopt, and covers gaps with the placeholder,
      # after every recovery regardless. The system shard needs no
      # synthetic services entry: it is looked up, never created, so it is
      # already there from the locking phase.
      updated_attempt =
        recovery_attempt
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(
          :shard_materializers,
          %{RecoveryAttempt.system_shard_id() => %{system_worker_id => Atom.to_string(node(materializer_pid))}}
        )
        # Provenance for the persistence phase: the layout was READ from
        # the durable family (nothing to rewrite), and the prior members
        # are the diff base for the tag-0 write.
        |> Map.put(:seeded_layout?, false)
        |> Map.put(:prior_materializer_refs, prior_refs)
        |> Map.put(:resolvers, resolver_descriptors_for_layout(shard_layout))

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

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

  # The system shard is LOOKED UP, never discovered and never invented.
  #
  # The prior core state names its members, because both durable
  # families — the shard layout and materializer membership — are served
  # from tag 0, and recovery cannot read either until it knows who holds
  # it. FDB has the same indirection and the same discipline: it builds
  # its log system from exactly the servers the coordinated state names
  # (TagPartitionedLogSystem.actor.cpp:2549-2585) and waits for a quorum
  # of THOSE, never substituting another server and never fabricating
  # one.
  #
  # So an unavailable named member is a STALL, not a fallback. Recovery
  # manufacturing the store its own metadata lives in would come up
  # "successfully" on an empty layout and orphan the cluster's data;
  # stalling is retried by the director, and an operator can see why.
  defp resolve_system_materializer(recovery_attempt, context) do
    named = CoreState.system_materializers(context.prior_core_state)

    case Enum.min(available_named_members(named, recovery_attempt), fn -> nil end) do
      {worker_id, service} ->
        {:ok, {worker_id, service}}

      # Two different situations, told apart so an operator is not left
      # guessing. A record that names members means they are unreachable
      # — retry, and the nodes they were last on say where to look. A
      # record that names NONE on a non-fresh cluster means the bootstrap
      # predates this field: recovery cannot learn where the metadata
      # lives, and no retry will change that.
      nil when named == %{} ->
        {:error, :bootstrap_names_no_system_materializers}

      nil ->
        {:error, {:system_materializers_unavailable, named}}
    end
  end

  # A named member counts only if this epoch actually locked it and it
  # reports itself as serving the system shard. The pick among several is
  # the lowest worker id — the same deterministic rule the client-facing
  # pick uses (RoutingData.pick_member/1), so recovery unlocks the member
  # clients will be routed to.
  defp available_named_members(named, recovery_attempt) do
    system_shard = RecoveryAttempt.system_shard_id()

    for {worker_id, _node} <- named,
        match?(%{shard_id: ^system_shard}, Map.get(recovery_attempt.materializer_recovery_info_by_id, worker_id)),
        %{status: {:up, ref}} <- [Map.get(recovery_attempt.transaction_services, worker_id)],
        into: %{},
        do: {worker_id, {:materializer, ref}}
  end

  # The committed tag-0 members at the recovery version: the diff base
  # for the keyspace write and the set recorded in the durable pointer
  # (the cluster bootstrap), so losing the one member this recovery
  # adopted does not leave the next one with nowhere to look. Read from
  # the same materializer, at the same version, as the shard layout — a
  # torn view is impossible (the families rewrite transactionally).
  #
  # Scoped to tag 0. Data-tag membership belongs to the distributor and
  # is not recovery's input; reading the whole family would drag it back
  # in as a standing invitation to start consuming it again.
  defp read_prior_refs(materializer_pid, read_version, context) do
    read_fn = Map.get(context, :read_prior_refs_fn, &default_read_prior_refs/2)
    read_fn.(materializer_pid, read_version)
  end

  defp default_read_prior_refs(materializer_pid, read_version) do
    prefix = Bedrock.SystemKeys.materializer_tag_prefix(RecoveryAttempt.system_shard_id())
    {_range_start, range_end} = Bedrock.KeyRange.from_prefix(prefix)

    range_read_fn = fn start_key ->
      Materializer.get_range(materializer_pid, start_key, range_end, read_version, limit: 1000)
    end

    with {:ok, entries} <- Reader.read_family(range_read_fn, prefix, :prior_refs_query_failed) do
      decode_prior_refs(entries)
    end
  end

  @doc false
  @spec decode_prior_refs([{Bedrock.key(), binary()}]) ::
          {:ok, %{Bedrock.range_tag() => %{Worker.id() => String.t()}}}
          | {:error, {:invalid_materializer_entry, Bedrock.key()}}
  defdelegate decode_prior_refs(entries), to: Reader, as: :decode_materializer_members

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
  # Delegates to the shared reader (Bedrock.SystemKeys.Reader): one
  # pager, one decode home, so recovery and the Distributor cannot
  # disagree about the families.
  @spec read_all_shard_entries(Reader.range_read_fn()) ::
          {:ok, [{Bedrock.key(), binary()}]} | {:error, {:shard_layout_query_failed, term()}}
  def read_all_shard_entries(range_read_fn),
    do: Reader.read_family(range_read_fn, Bedrock.SystemKeys.shard_keys_prefix(), :shard_layout_query_failed)

  @doc false
  @spec shard_layout_from_entries([{Bedrock.key(), binary()}]) ::
          {:ok, RecoveryAttempt.shard_layout()} | {:error, {:invalid_shard_value, Bedrock.key()}}
  defdelegate shard_layout_from_entries(entries), to: Reader
end
