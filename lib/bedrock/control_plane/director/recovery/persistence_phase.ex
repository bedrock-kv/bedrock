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
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Id
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.ClusterBootstrap
  alias Bedrock.SystemKeys.ShardMetadata
  alias Bedrock.SystemKeys.Values

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    transaction_system_layout = recovery_attempt.transaction_system_layout

    system_transaction =
      build_system_transaction(
        recovery_attempt.epoch,
        context.cluster_config,
        transaction_system_layout,
        recovery_attempt.cluster
      )

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

  @spec build_system_transaction(
          epoch :: non_neg_integer(),
          cluster_config :: Config.t(),
          transaction_system_layout :: TransactionSystemLayout.t(),
          cluster :: module()
        ) :: Transaction.encoded()
  defp build_system_transaction(epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_config = Persistence.encode_for_storage(cluster_config, cluster)

    tx = Tx.new()
    tx = build_monolithic_keys(tx, epoch, encoded_config)
    tx = build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, cluster)

    Tx.commit(tx, nil)
  end

  @spec build_monolithic_keys(Tx.t(), Bedrock.epoch(), map()) :: Tx.t()
  defp build_monolithic_keys(tx, epoch, encoded_config) do
    tx
    |> Tx.set(SystemKeys.config_monolithic(), Values.encode_structured({epoch, encoded_config}))
    |> Tx.set(SystemKeys.epoch_legacy(), Values.encode_integer(epoch))
    |> Tx.set(
      SystemKeys.last_recovery_legacy(),
      Values.encode_integer(System.system_time(:millisecond))
    )
  end

  @spec build_decomposed_keys(
          Tx.t(),
          Bedrock.epoch(),
          Config.t(),
          TransactionSystemLayout.t(),
          module()
        ) ::
          Tx.t()
  defp build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, _cluster) do
    encoded_services = encode_services_for_storage(transaction_system_layout.services)

    tx =
      tx
      |> Tx.set(
        SystemKeys.cluster_coordinators(),
        Values.encode_node_list(cluster_config.coordinators)
      )
      |> Tx.set(SystemKeys.cluster_epoch(), Values.encode_integer(epoch))
      |> Tx.set(
        SystemKeys.cluster_policies_volunteer_nodes(),
        Values.encode_boolean(cluster_config.policies.allow_volunteer_nodes_to_join || false)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_logs(),
        Values.encode_integer(cluster_config.parameters.desired_logs)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_replication(),
        Values.encode_integer(cluster_config.parameters.desired_replication_factor)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_commit_proxies(),
        Values.encode_integer(cluster_config.parameters.desired_commit_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_coordinators(),
        Values.encode_integer(cluster_config.parameters.desired_coordinators)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_read_version_proxies(),
        Values.encode_integer(cluster_config.parameters.desired_read_version_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_empty_transaction_timeout_ms(),
        Values.encode_integer(Map.get(cluster_config.parameters, :empty_transaction_timeout_ms, 1_000))
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_ping_rate_in_hz(),
        Values.encode_integer(cluster_config.parameters.ping_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_retransmission_rate_in_hz(),
        Values.encode_integer(cluster_config.parameters.retransmission_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_transaction_window_in_ms(),
        Values.encode_integer(cluster_config.parameters.transaction_window_in_ms)
      )

    # Only durable layout config (services and id)
    tx =
      tx
      |> Tx.set(SystemKeys.layout_services(), Values.encode_structured(encoded_services))
      |> Tx.set(SystemKeys.layout_id(), Values.encode_id(transaction_system_layout.id))

    tx =
      Enum.reduce(transaction_system_layout.logs, tx, fn {log_id, log_descriptor}, tx ->
        Tx.set(tx, SystemKeys.layout_log(log_id), Values.encode_tag_list(log_descriptor))
      end)

    # Shard keys use ceiling-search pattern
    tx = build_shard_keys(tx, transaction_system_layout.shard_layout)

    tx
    |> Tx.set(SystemKeys.recovery_attempt(), Values.encode_integer(1))
    |> Tx.set(
      SystemKeys.recovery_last_completed(),
      Values.encode_integer(System.system_time(:millisecond))
    )
  end

  # Runtime PIDs are meaningless in durable storage: a later epoch cannot use
  # them, so live statuses are stored as :unknown. Service refs are
  # re-populated at runtime by the director.
  defp encode_services_for_storage(services) when is_map(services) do
    Map.new(services, fn {service_id, descriptor} ->
      case descriptor do
        %{status: {:up, pid}} when is_pid(pid) -> {service_id, %{descriptor | status: :unknown}}
        _ -> {service_id, descriptor}
      end
    end)
  end

  # Creates shard_key(end_key) -> {tag, start_key} and shard(tag) -> ShardMetadata entries
  # shard_layout format: %{end_key => {tag, start_key}}
  @spec build_shard_keys(Tx.t(), TransactionSystemLayout.shard_layout() | nil) :: Tx.t()
  defp build_shard_keys(tx, nil), do: tx
  defp build_shard_keys(tx, shard_layout) when map_size(shard_layout) == 0, do: tx

  defp build_shard_keys(tx, shard_layout) when is_map(shard_layout) do
    Enum.reduce(shard_layout, tx, fn {end_key, {tag, start_key}}, tx ->
      # Write shard_key(end_key) -> {tag, start_key} (for ceiling search)
      tx = Tx.set(tx, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key))

      # Write shard(tag) -> ShardMetadata (FlatBuffer encoded)
      # born_at is 0 for now - will be set properly once we track shard versions
      metadata = ShardMetadata.new(start_key, end_key, 0)
      Tx.set(tx, SystemKeys.shard(tag), metadata)
    end)
  end

  @spec submit_system_transaction(Transaction.encoded(), [pid()], Bedrock.epoch(), map()) ::
          {:ok, Bedrock.version(), sequence :: non_neg_integer()}
          | {:error, :no_commit_proxies | :timeout | :unavailable}
  defp submit_system_transaction(_system_transaction, [], _epoch, _context), do: {:error, :no_commit_proxies}

  defp submit_system_transaction(encoded_transaction, proxies, epoch, context) when is_list(proxies) do
    commit_fn = Map.get(context, :commit_transaction_fn, &CommitProxy.commit/3)

    proxies
    |> Enum.random()
    |> commit_fn.(epoch, encoded_transaction)
  end
end
