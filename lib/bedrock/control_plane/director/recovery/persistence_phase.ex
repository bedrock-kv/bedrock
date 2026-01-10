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

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.SystemKeys
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

        {recovery_attempt, :completed}

      {:error, reason} ->
        trace_recovery_system_transaction_failed(reason)
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  @spec build_system_transaction(
          epoch :: non_neg_integer(),
          cluster_config :: Config.t(),
          transaction_system_layout :: TransactionSystemLayout.t(),
          cluster :: module()
        ) :: Transaction.encoded()
  defp build_system_transaction(epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_config = Persistence.encode_for_storage(cluster_config, cluster)

    encoded_layout =
      Persistence.encode_transaction_system_layout_for_storage(transaction_system_layout, cluster)

    tx = Tx.new()
    tx = build_monolithic_keys(tx, epoch, encoded_config, encoded_layout)
    tx = build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, cluster)

    Tx.commit(tx, nil)
  end

  @spec build_monolithic_keys(Tx.t(), Bedrock.epoch(), map(), map()) :: Tx.t()
  defp build_monolithic_keys(tx, epoch, encoded_config, encoded_layout) do
    tx
    |> Tx.set(SystemKeys.config_monolithic(), :erlang.term_to_binary({epoch, encoded_config}))
    |> Tx.set(SystemKeys.epoch_legacy(), :erlang.term_to_binary(epoch))
    |> Tx.set(
      SystemKeys.last_recovery_legacy(),
      :erlang.term_to_binary(System.system_time(:millisecond))
    )
    |> Tx.set(SystemKeys.layout_monolithic(), :erlang.term_to_binary(encoded_layout))
  end

  @spec build_decomposed_keys(
          Tx.t(),
          Bedrock.epoch(),
          Config.t(),
          TransactionSystemLayout.t(),
          module()
        ) ::
          Tx.t()
  defp build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_sequencer = encode_component_for_storage(transaction_system_layout.sequencer, cluster)
    encoded_proxies = encode_components_for_storage(transaction_system_layout.proxies, cluster)

    encoded_resolvers =
      encode_components_for_storage(transaction_system_layout.resolvers, cluster)

    encoded_services = encode_services_for_storage(transaction_system_layout.services, cluster)

    # Set cluster keys directly
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

    # Set layout keys directly
    tx =
      tx
      |> Tx.set(SystemKeys.layout_sequencer(), :erlang.term_to_binary(encoded_sequencer))
      |> Tx.set(SystemKeys.layout_proxies(), :erlang.term_to_binary(encoded_proxies))
      |> Tx.set(SystemKeys.layout_services(), :erlang.term_to_binary(encoded_services))
      |> Tx.set(
        SystemKeys.layout_director(),
        :erlang.term_to_binary(encode_component_for_storage(self(), cluster))
      )
      |> Tx.set(SystemKeys.layout_rate_keeper(), :erlang.term_to_binary(nil))
      |> Tx.set(SystemKeys.layout_id(), :erlang.term_to_binary(transaction_system_layout.id))

    # Set resolver keys using ceiling-search pattern (keyed by end_key)
    tx = build_resolver_keys(tx, encoded_resolvers)

    # Set log keys directly
    tx =
      Enum.reduce(transaction_system_layout.logs, tx, fn {log_id, log_descriptor}, tx ->
        encoded_descriptor =
          log_descriptor
          |> encode_log_descriptor_for_storage(cluster)
          |> :erlang.term_to_binary()

        Tx.set(tx, SystemKeys.layout_log(log_id), encoded_descriptor)
      end)

    # Set shard keys using ceiling-search pattern
    tx = build_shard_keys(tx, transaction_system_layout.storage_teams)

    # Set recovery keys directly and return tx
    tx
    |> Tx.set(SystemKeys.recovery_attempt(), :erlang.term_to_binary(1))
    |> Tx.set(
      SystemKeys.recovery_last_completed(),
      :millisecond |> System.system_time() |> :erlang.term_to_binary()
    )
  end

  @spec encode_component_for_storage(nil | pid() | {Bedrock.key(), pid()}, module()) ::
          nil | pid() | {Bedrock.key(), pid()}
  defp encode_component_for_storage(nil, _cluster), do: nil
  defp encode_component_for_storage(pid, _cluster) when is_pid(pid), do: pid

  defp encode_component_for_storage({start_key, pid}, _cluster) when is_pid(pid), do: {start_key, pid}

  defp encode_components_for_storage(components, cluster) when is_list(components),
    do: Enum.map(components, &encode_component_for_storage(&1, cluster))

  defp encode_services_for_storage(services, _cluster) when is_map(services), do: services

  @spec encode_log_descriptor_for_storage([term()], module()) :: [term()]
  defp encode_log_descriptor_for_storage(log_descriptor, _cluster) do
    # Log descriptors typically don't contain PIDs directly
    log_descriptor
  end

  # Build resolver keys using ceiling-search pattern
  # Resolvers are stored as {start_key, pid} tuples
  # We convert to {end_key -> resolver} format where end_key is the next resolver's start_key or \xff
  @spec build_resolver_keys(Tx.t(), [{Bedrock.key(), pid()}]) :: Tx.t()
  defp build_resolver_keys(tx, []), do: tx

  defp build_resolver_keys(tx, resolvers) when is_list(resolvers) do
    # Sort by start_key
    sorted = Enum.sort_by(resolvers, fn {start_key, _pid} -> start_key end)

    # Build ceiling-search keys: each resolver is keyed by the END of its range
    # The last resolver covers up to \xff
    sorted
    |> Enum.with_index()
    |> Enum.reduce(tx, fn {{_start_key, pid}, index}, tx ->
      # End key is either the next resolver's start_key, or \xff for the last one
      end_key =
        case Enum.at(sorted, index + 1) do
          {next_start, _} -> next_start
          nil -> "\xff"
        end

      # Still use term_to_binary for pid values until services use named processes
      Tx.set(tx, SystemKeys.layout_resolver(end_key), :erlang.term_to_binary(pid))
    end)
  end

  # Build shard keys from storage_teams
  # Creates both shard_key(end_key) -> tag and shard(tag) -> ShardMetadata entries
  @spec build_shard_keys(Tx.t(), [map()]) :: Tx.t()
  defp build_shard_keys(tx, []), do: tx

  defp build_shard_keys(tx, storage_teams) when is_list(storage_teams) do
    Enum.reduce(storage_teams, tx, fn storage_team, tx ->
      %{tag: tag, key_range: {start_key, end_key}} = storage_team

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
