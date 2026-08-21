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
  alias Bedrock.KeyRange
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

  defp build_updated_bootstrap(current_bootstrap, recovery_attempt, config, transaction_system_layout) do
    %{
      current_bootstrap
      | epoch: recovery_attempt.epoch,
        logs: build_log_entries(transaction_system_layout),
        parameters: build_parameters(config),
        policies: build_policies(config)
    }
  end

  defp build_initial_bootstrap(recovery_attempt, config, transaction_system_layout) do
    %{
      cluster_id: Id.random(),
      epoch: recovery_attempt.epoch,
      logs: build_log_entries(transaction_system_layout),
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
  # RoutingData and the next recovery's materializer bootstrap,
  # and materializers/ refs feed the client-facing routing projection
  # (FDB's serverList analogue - runtime hints, never recovery input) and
  # worker rejoin validation. layout/logs/ has no code reader by design:
  # log wiring is epoch-constant and rides the unlock seed
  # (bedrock-q67.41); the family stays durable for other consumers and
  # cluster-introspection tools. Nothing else is written (config and
  # policy travel via the object-storage cluster bootstrap, which the
  # coordinator actually reads; services are rebuilt each recovery from
  # foreman discovery). Families return to the keyspace when their
  # readers do (bedrock-q67.9, q67.25).
  @spec build_readable_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp build_readable_keys(tx, recovery_attempt) do
    tx = clear_prefix(tx, SystemKeys.layout_logs_prefix())

    tx =
      Enum.reduce(recovery_attempt.transaction_system_layout.logs, tx, fn {log_id, log_descriptor}, tx ->
        Tx.set(tx, SystemKeys.layout_log(log_id), Values.encode_tag_list(log_descriptor))
      end)

    tx = build_shard_keys(tx, recovery_attempt.shard_layout)
    build_materializer_keys(tx, recovery_attempt)
  end

  # Creates materializer_key(tag) -> {worker_id, node} entries: the
  # attempt already carries the refs in the keyspace-value shape (both
  # strings; the reader derives the callable ref), so they are written
  # verbatim — the routing-snapshot seed embeds the same map, so keyspace
  # and seed are one map read twice. Gated like shard_keys, on the same
  # INPUT: shard_materializers absent/empty means shard management is not
  # active, so existing entries are untouched.
  @spec build_materializer_keys(Tx.t(), RecoveryAttempt.t()) :: Tx.t()
  defp build_materializer_keys(tx, recovery_attempt) do
    case Map.get(recovery_attempt, :shard_materializers) do
      nil ->
        tx

      materializers when map_size(materializers) == 0 ->
        tx

      materializers ->
        tx = clear_prefix(tx, SystemKeys.materializers_prefix())

        Enum.reduce(materializers, tx, fn {tag, {worker_id, node}}, tx ->
          Tx.set(tx, SystemKeys.materializer_key(tag), Values.encode_materializer_ref(worker_id, node))
        end)
    end
  end

  # Rewritten-every-recovery keyed families are cleared before their entries
  # are written so a shrinking layout leaves no stale entries behind. The clear
  # and the rewrite land in the same system transaction, so readers never
  # observe a gap.
  @spec clear_prefix(Tx.t(), Bedrock.key()) :: Tx.t()
  defp clear_prefix(tx, prefix) do
    {start_key, end_key} = KeyRange.from_prefix(prefix)
    Tx.clear_range(tx, start_key, end_key)
  end

  # Creates shard_key(end_key) -> {tag, start_key} entries (ceiling search).
  # shard_layout format: %{end_key => {tag, start_key}}
  #
  # A nil or empty layout means shard management is not active for this
  # recovery, so existing entries are left untouched rather than cleared.
  @spec build_shard_keys(Tx.t(), RecoveryAttempt.shard_layout() | nil) :: Tx.t()
  defp build_shard_keys(tx, nil), do: tx
  defp build_shard_keys(tx, shard_layout) when map_size(shard_layout) == 0, do: tx

  defp build_shard_keys(tx, shard_layout) when is_map(shard_layout) do
    tx = clear_prefix(tx, SystemKeys.shard_keys_prefix())

    Enum.reduce(shard_layout, tx, fn {end_key, {tag, start_key}}, tx ->
      Tx.set(tx, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key))
    end)
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
