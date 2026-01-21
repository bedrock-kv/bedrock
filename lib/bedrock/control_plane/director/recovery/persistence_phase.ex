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

        case write_bootstrap_to_object_storage(recovery_attempt, transaction_system_layout) do
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

  defp write_bootstrap_to_object_storage(recovery_attempt, transaction_system_layout) do
    cluster = recovery_attempt.cluster

    case get_object_storage_backend(cluster) do
      {:ok, backend} ->
        # Bootstrap key is just "state" since object_storage root is already cluster-scoped
        do_write_bootstrap(backend, "state", recovery_attempt, transaction_system_layout)

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

  defp do_write_bootstrap(backend, bootstrap_key, recovery_attempt, transaction_system_layout) do
    case ObjectStorage.get_with_version(backend, bootstrap_key) do
      {:ok, data, version_token} ->
        {:ok, current_bootstrap} = ClusterBootstrap.read(data)
        updated_bootstrap = build_updated_bootstrap(current_bootstrap, recovery_attempt, transaction_system_layout)
        Discovery.write_bootstrap(backend, bootstrap_key, version_token, updated_bootstrap)

      {:error, :not_found} ->
        # First boot - create new bootstrap
        bootstrap = build_initial_bootstrap(recovery_attempt, transaction_system_layout)
        data = ClusterBootstrap.to_binary(bootstrap)
        ObjectStorage.put_if_not_exists(backend, bootstrap_key, data)
    end
  end

  defp build_updated_bootstrap(current_bootstrap, recovery_attempt, transaction_system_layout) do
    %{
      current_bootstrap
      | epoch: recovery_attempt.epoch,
        logs: build_log_entries(transaction_system_layout)
    }
  end

  defp build_initial_bootstrap(recovery_attempt, transaction_system_layout) do
    %{
      cluster_id: Id.random(),
      epoch: recovery_attempt.epoch,
      logs: build_log_entries(transaction_system_layout),
      coordinators: [%{node: Atom.to_string(node())}]
    }
  end

  defp build_log_entries(transaction_system_layout) do
    Enum.map(transaction_system_layout.logs, fn {log_id, _descriptor} ->
      # For now, log entries don't include OTP refs - that requires service lookup
      %{id: log_id, otp_ref: nil}
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
    |> Tx.set(SystemKeys.config_monolithic(), :erlang.term_to_binary({epoch, encoded_config}))
    |> Tx.set(SystemKeys.epoch_legacy(), :erlang.term_to_binary(epoch))
    |> Tx.set(
      SystemKeys.last_recovery_legacy(),
      :erlang.term_to_binary(System.system_time(:millisecond))
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
        :erlang.term_to_binary(cluster_config.coordinators)
      )
      |> Tx.set(SystemKeys.cluster_epoch(), :erlang.term_to_binary(epoch))
      |> Tx.set(
        SystemKeys.cluster_policies_volunteer_nodes(),
        :erlang.term_to_binary(cluster_config.policies.allow_volunteer_nodes_to_join)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_logs(),
        :erlang.term_to_binary(cluster_config.parameters.desired_logs)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_replication(),
        :erlang.term_to_binary(cluster_config.parameters.desired_replication_factor)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_commit_proxies(),
        :erlang.term_to_binary(cluster_config.parameters.desired_commit_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_coordinators(),
        :erlang.term_to_binary(cluster_config.parameters.desired_coordinators)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_read_version_proxies(),
        :erlang.term_to_binary(cluster_config.parameters.desired_read_version_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_empty_transaction_timeout_ms(),
        :erlang.term_to_binary(Map.get(cluster_config.parameters, :empty_transaction_timeout_ms, 1_000))
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_ping_rate_in_hz(),
        :erlang.term_to_binary(cluster_config.parameters.ping_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_retransmission_rate_in_hz(),
        :erlang.term_to_binary(cluster_config.parameters.retransmission_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_transaction_window_in_ms(),
        :erlang.term_to_binary(cluster_config.parameters.transaction_window_in_ms)
      )

    # Only durable layout config (services and id)
    tx =
      tx
      |> Tx.set(SystemKeys.layout_services(), :erlang.term_to_binary(encoded_services))
      |> Tx.set(SystemKeys.layout_id(), :erlang.term_to_binary(transaction_system_layout.id))

    tx =
      Enum.reduce(transaction_system_layout.logs, tx, fn {log_id, log_descriptor}, tx ->
        encoded_descriptor =
          log_descriptor
          |> encode_log_descriptor_for_storage()
          |> :erlang.term_to_binary()

        Tx.set(tx, SystemKeys.layout_log(log_id), encoded_descriptor)
      end)

    # Shard keys use ceiling-search pattern
    tx = build_shard_keys(tx, transaction_system_layout.shard_layout)

    tx
    |> Tx.set(SystemKeys.recovery_attempt(), :erlang.term_to_binary(1))
    |> Tx.set(
      SystemKeys.recovery_last_completed(),
      :millisecond |> System.system_time() |> :erlang.term_to_binary()
    )
  end

  defp encode_services_for_storage(services) when is_map(services), do: services

  @spec encode_log_descriptor_for_storage([term()]) :: [term()]
  defp encode_log_descriptor_for_storage(log_descriptor) do
    # Log descriptors typically don't contain PIDs directly
    log_descriptor
  end

  # Creates shard_key(end_key) -> tag and shard(tag) -> ShardMetadata entries
  # shard_layout format: %{end_key => {tag, start_key}}
  @spec build_shard_keys(Tx.t(), TransactionSystemLayout.shard_layout() | nil) :: Tx.t()
  defp build_shard_keys(tx, nil), do: tx
  defp build_shard_keys(tx, shard_layout) when map_size(shard_layout) == 0, do: tx

  defp build_shard_keys(tx, shard_layout) when is_map(shard_layout) do
    Enum.reduce(shard_layout, tx, fn {end_key, {tag, start_key}}, tx ->
      # Write shard_key(end_key) -> tag (for ceiling search)
      tx = Tx.set(tx, SystemKeys.shard_key(end_key), :erlang.term_to_binary(tag))

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
