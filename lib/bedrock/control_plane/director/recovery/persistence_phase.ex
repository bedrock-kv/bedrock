defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhase do
  @moduledoc """
  Persists cluster configuration through a complete system transaction.

  Constructs a system transaction containing the full cluster configuration and
  submits it through the entire data plane pipeline. This simultaneously persists
  the new configuration and validates that all transaction components work correctly.

  Stores configuration in both monolithic and decomposed formats. Monolithic keys
  support coordinator handoff while decomposed keys allow targeted component access.

  If the system transaction fails, the director exits immediately rather than
  retrying. System transaction failure indicates fundamental problems that require
  coordinator restart with a new epoch.

  Transitions to monitoring on success or exits the director on failure.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.ClusterBootstrap.Discovery
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Id
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.ClusterBootstrap
  alias Bedrock.SystemKeys.Values

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    transaction_system_layout = recovery_attempt.transaction_system_layout

    system_transaction = build_system_transaction(recovery_attempt)

    case submit_system_transaction(system_transaction, recovery_attempt.proxies, recovery_attempt.epoch, context) do
      {:ok, _version, _sequence} ->
        trace_recovery_system_state_persisted()

        case write_state_to_object_storage(recovery_attempt, context.cluster_config, transaction_system_layout) do
          :ok ->
            {recovery_attempt, :completed}

          {:error, :version_mismatch} ->
            {recovery_attempt, {:stalled, {:recovery_system_failed, :bootstrap_version_mismatch}}}

          {:error, reason} ->
            {recovery_attempt, {:stalled, {:recovery_system_failed, {:bootstrap_write_failed, reason}}}}
        end

      {:error, reason} ->
        trace_recovery_system_transaction_failed(reason)
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  defp write_state_to_object_storage(recovery_attempt, config, transaction_system_layout) do
    cluster = recovery_attempt.cluster

    case get_object_storage_backend(cluster) do
      {:ok, backend} ->
        # ClusterBootstrap is the sole source of truth for coordinator cold boot
        do_write_bootstrap(backend, "bootstrap", recovery_attempt, config, transaction_system_layout)

      {:error, :no_object_storage} ->
        # No object storage configured - skip bootstrap write
        :ok
    end
  end

  # Get object_storage backend from cluster's node config
  defp get_object_storage_backend(cluster) do
    node_config = cluster.node_config()

    # Check for explicit object_storage config
    case Keyword.fetch(node_config, :object_storage) do
      {:ok, backend} ->
        {:ok, backend}

      :error ->
        # Derive from path config (same logic as cluster_supervisor)
        derive_object_storage_from_path(node_config)
    end
  end

  defp derive_object_storage_from_path(node_config) do
    # Try to find a path from any capability config
    path =
      Enum.find_value([:log, :storage, :materializer, :coordination], fn capability ->
        node_config
        |> Keyword.get(capability, [])
        |> Keyword.get(:path)
      end)

    if path do
      object_storage_root = Path.join(path, "object_storage")
      backend = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)
      {:ok, backend}
    else
      {:error, :no_object_storage}
    end
  end

  defp do_write_bootstrap(backend, bootstrap_key, recovery_attempt, config, transaction_system_layout) do
    case ObjectStorage.get_with_version(backend, bootstrap_key) do
      {:ok, data, version_token} ->
        {:ok, current_bootstrap} = ClusterBootstrap.read(data)

        updated_bootstrap =
          build_updated_bootstrap(current_bootstrap, recovery_attempt, config, transaction_system_layout)

        Discovery.write_bootstrap(backend, bootstrap_key, version_token, updated_bootstrap)

      {:error, :not_found} ->
        # First boot - create new bootstrap
        bootstrap = build_initial_bootstrap(recovery_attempt, config, transaction_system_layout)
        data = ClusterBootstrap.to_binary(bootstrap)
        ObjectStorage.put_if_not_exists(backend, bootstrap_key, data)
    end
  end

  # MERGE, not %{record | ...}. The update operator raises badkey for a
  # key the map does not already have, and a record written before a
  # field existed decodes without it — so updating in place crashes the
  # director on exactly the clusters that most need to be upgraded. Merge
  # adds what is missing and overwrites what is not, which is what
  # "rewrite these fields" actually means.
  @doc false
  @spec build_updated_bootstrap(map(), RecoveryAttempt.t(), map(), map()) :: map()
  def build_updated_bootstrap(current_bootstrap, recovery_attempt, config, transaction_system_layout) do
    Map.merge(current_bootstrap, %{
      epoch: recovery_attempt.epoch,
      logs: build_log_entries(transaction_system_layout),
      system_materializers: build_system_materializer_entries(recovery_attempt),
      parameters: build_parameters(config),
      policies: build_policies(config)
    })
  end

  # Where the metadata lives, recorded out of band. Both durable families
  # — the shard layout and materializer membership — are served from tag
  # 0, so the next recovery cannot read either until it knows which
  # workers hold it. FDB records the same indirection: its coordinated
  # state names the tlogs that hold the txnStateStore.
  # The full committed member SET, not just the one member this recovery
  # adopted. FDB names a set here too and waits for a replication-policy
  # quorum over it (TagPartitionedLogSystem.actor.cpp:2549-2585 locks
  # every named tlog; getDurableVersion at :2070-2082 decides on a
  # quorum of THOSE) — it never substitutes a server, but it also never
  # depends on one specific server. Recording only the adopted member
  # would mean losing that single node stalls recovery forever, with a
  # healthy committed replica sitting right there.
  defp build_system_materializer_entries(recovery_attempt) do
    system_shard = RecoveryAttempt.system_shard_id()
    adopted = Map.get(recovery_attempt.shard_materializers, system_shard, %{})
    committed = recovery_attempt |> Map.get(:prior_materializer_refs) |> Kernel.||(%{}) |> Map.get(system_shard, %{})

    committed
    |> Map.merge(adopted)
    |> Enum.map(fn {worker_id, node} -> %{id: worker_id, node: node} end)
  end

  defp build_initial_bootstrap(recovery_attempt, config, transaction_system_layout) do
    %{
      cluster_id: Id.random(),
      epoch: recovery_attempt.epoch,
      logs: build_log_entries(transaction_system_layout),
      system_materializers: build_system_materializer_entries(recovery_attempt),
      coordinators: [%{node: Atom.to_string(node())}],
      parameters: build_parameters(config),
      policies: build_policies(config)
    }
  end

  defp build_parameters(config) do
    params = config.parameters

    %{
      desired_logs: params.desired_logs,
      desired_replication_factor: params.desired_replication_factor,
      desired_commit_proxies: params.desired_commit_proxies,
      desired_coordinators: params.desired_coordinators,
      desired_read_version_proxies: params.desired_read_version_proxies,
      ping_rate_in_hz: params.ping_rate_in_hz,
      retransmission_rate_in_hz: params.retransmission_rate_in_hz,
      transaction_window_in_ms: params.transaction_window_in_ms,
      empty_transaction_timeout_ms: Map.get(params, :empty_transaction_timeout_ms, 1_000)
    }
  end

  defp build_policies(config) do
    policies = config.policies

    %{
      allow_volunteer_nodes_to_join: policies.allow_volunteer_nodes_to_join || false
    }
  end

  defp build_log_entries(transaction_system_layout) do
    Enum.map(transaction_system_layout.logs, fn {log_id, _descriptor} ->
      # With consistent hashing, shard→log mapping is computed at runtime via ShardRouter.
      # The shard_tags field is kept for backward compatibility but is always empty.
      %{id: log_id, otp_ref: nil, shard_tags: []}
    end)
  end

  @spec build_system_transaction(RecoveryAttempt.t()) :: Transaction.encoded()
  defp build_system_transaction(recovery_attempt) do
    tx = Tx.new()
    tx = build_readable_keys(tx, recovery_attempt)

    Tx.commit(tx, nil)
  end

  # Every key written here has a named purpose: shard_keys/ feeds both
  # RoutingData and the next recovery's materializer bootstrap, and
  # materializers/ refs feed the client-facing routing projection (FDB's
  # serverList analogue - runtime hints, never recovery input) and worker
  # rejoin validation. Nothing else is written (config and policy travel
  # via the object-storage cluster bootstrap, which the coordinator
  # actually reads; services are rebuilt each recovery from foreman
  # discovery). Families return to the keyspace when their readers do
  # (bedrock-q67.9, q67.25) - and only then. FDB does keep a keyspace
  # copy of its log set (`\xff/logs`, SystemData.cpp:1171, written by the
  # recovery transaction at ClusterRecovery.actor.cpp:1728), so the
  # deleted layout/logs family was its analogue - but FDB's copy has
  # three readers we have no equivalent of: recovery's stale-master
  # fence (ClusterRecovery.actor.cpp:770-801), exclusion safety
  # (ManagementAPI.actor.cpp:2394), and the in-progress-exclusion
  # special-key module (SpecialKeySpace.actor.cpp:1294). Ours had none,
  # and the tag mapping it held survives in the cluster bootstrap the
  # coordinator actually loads. The family comes back when one of those
  # readers does (bedrock-q67.21.10).
  @spec build_readable_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp build_readable_keys(tx, recovery_attempt) do
    # The mapping families are durable, distributor-era state: recovery
    # reads and heals, never blanket-clears (bedrock-q67.21.2).
    tx = build_shard_keys(tx, recovery_attempt)

    tx
    |> build_materializer_keys(recovery_attempt)
    |> migrate_legacy_materializer_keys(recovery_attempt)
  end

  # Creates materializer_key(tag, worker_id) -> node entries as a DIFF
  # against the prior family (read by bootstrap): only assignments this
  # recovery changed are written; unchanged entries are left in place,
  # and entries for tags outside this layout are not recovery's to clean
  # — read-and-heal means stale reconciliation belongs to the
  # distributor (bedrock-q67.21.4). A nil prior means the family was not
  # read (fresh cluster, legacy path): every assignment writes, the safe
  # direction. The attempt carries refs in the family's member shape, so
  # keyspace and routing-snapshot seed remain one map read twice. Gated
  # on the same INPUT as before: shard_materializers absent/empty means
  # shard management is not active.
  @spec build_materializer_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp build_materializer_keys(tx, recovery_attempt) do
    case Map.get(recovery_attempt, :shard_materializers) do
      nil ->
        tx

      materializers when map_size(materializers) == 0 ->
        tx

      materializers ->
        prior = Map.get(recovery_attempt, :prior_materializer_refs) || %{}

        # Recovery writes the members it decided on and nothing else: an
        # entry already naming this worker on this node is left alone,
        # and members recovery never touched (other replicas of the same
        # shard) keep their keys — the family is a set, so writing one
        # member never implies removing another.
        for {tag, members} <- materializers, {worker_id, node} <- members, reduce: tx do
          tx ->
            if prior |> Map.get(tag, %{}) |> Map.get(worker_id) == node do
              tx
            else
              Tx.set(tx, SystemKeys.materializer_key(tag, worker_id), Values.encode_materializer_node(node))
            end
        end
    end
  end

  # Completes the pre-q67.21.9 migration for the keys this recovery's
  # read folded in: every member the legacy key held is rewritten in the
  # set-valued shape, and only then is the legacy key cleared. Both in
  # ONE transaction, so no reader ever sees the tag unrepresented.
  #
  # Rewriting every member matters: the legacy key may name a worker
  # recovery did not seat, and clearing without writing it would drop a
  # live member. Leaving the key instead would be worse — a legacy member
  # can never be retired, because retirement clears the new-shape key it
  # does not have.
  @spec migrate_legacy_materializer_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp migrate_legacy_materializer_keys(tx, recovery_attempt) do
    prior = Map.get(recovery_attempt, :prior_materializer_refs) || %{}

    recovery_attempt
    |> Map.get(:legacy_materializer_keys)
    |> Kernel.||([])
    |> Enum.reduce(tx, fn legacy_key, tx ->
      {:legacy_materializer_key, tag} = SystemKeys.parse_key(legacy_key)

      prior
      |> Map.get(tag, %{})
      |> Enum.reduce(tx, fn {worker_id, node}, tx ->
        Tx.set(tx, SystemKeys.materializer_key(tag, worker_id), Values.encode_materializer_node(node))
      end)
      |> Tx.clear(legacy_key)
    end)
  end

  # Creates shard_key(end_key) -> {tag, start_key} entries (ceiling
  # search) ONLY when this recovery seeded the layout (fresh cluster —
  # FDB's seedShardServers analogue). An existing cluster's layout was
  # READ from the family, and boundaries never change without splits, so
  # there is nothing to write; the family is durable across epochs. The
  # seed writes into a definitionally empty family (a fresh cluster has
  # no committed data), so no clear is needed.
  @spec build_shard_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp build_shard_keys(tx, recovery_attempt) do
    shard_layout = recovery_attempt.shard_layout

    if Map.get(recovery_attempt, :seeded_layout?, false) and is_map(shard_layout) and map_size(shard_layout) > 0 do
      Enum.reduce(shard_layout, tx, fn {end_key, {tag, start_key}}, tx ->
        Tx.set(tx, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key))
      end)
    else
      tx
    end
  end

  @spec submit_system_transaction(Transaction.encoded(), [pid()], Bedrock.epoch(), map()) ::
          {:ok, Bedrock.version(), sequence :: non_neg_integer()}
          | {:error, :no_commit_proxies | :timeout | :unavailable}
  defp submit_system_transaction(_system_transaction, [], _epoch, _context), do: {:error, :no_commit_proxies}

  defp submit_system_transaction(encoded_transaction, proxies, epoch, context) when is_list(proxies) do
    commit_fn = Map.get(context, :commit_transaction_fn, &commit_in_system_mode/3)

    proxies
    |> Enum.random()
    |> commit_fn.(epoch, encoded_transaction)
  end

  # Recovery is a system writer: user-mode commits cannot touch \xFF keys.
  defp commit_in_system_mode(proxy, epoch, encoded_transaction),
    do: CommitProxy.commit(proxy, epoch, encoded_transaction, mode: :system)
end
