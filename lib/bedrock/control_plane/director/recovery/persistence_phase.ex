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

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.SystemKeys

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    transaction_system_layout = recovery_attempt.transaction_system_layout

    with system_transaction <-
           build_system_transaction(
             recovery_attempt.epoch,
             context.cluster_config,
             transaction_system_layout,
             recovery_attempt.cluster
           ),
         {:ok, _version} <-
           submit_system_transaction(system_transaction, recovery_attempt.proxies, context) do
      trace_recovery_system_state_persisted()

      {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MonitoringPhase}
    else
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
        ) :: Bedrock.transaction()
  defp build_system_transaction(epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_config = Persistence.encode_for_storage(cluster_config, cluster)

    encoded_layout =
      Persistence.encode_transaction_system_layout_for_storage(transaction_system_layout, cluster)

    build_monolithic_keys(epoch, encoded_config, encoded_layout)
    |> Map.merge(build_decomposed_keys(epoch, cluster_config, transaction_system_layout, cluster))
    |> then(&{nil, &1})
  end

  @spec build_monolithic_keys(Bedrock.epoch(), map(), map()) :: %{Bedrock.key() => binary()}
  defp build_monolithic_keys(epoch, encoded_config, encoded_layout) do
    %{
      SystemKeys.config_monolithic() => :erlang.term_to_binary({epoch, encoded_config}),
      SystemKeys.epoch_legacy() => :erlang.term_to_binary(epoch),
      SystemKeys.last_recovery_legacy() =>
        :erlang.term_to_binary(System.system_time(:millisecond)),
      SystemKeys.layout_monolithic() => :erlang.term_to_binary(encoded_layout)
    }
  end

  @spec build_decomposed_keys(Bedrock.epoch(), Config.t(), TransactionSystemLayout.t(), module()) ::
          %{Bedrock.key() => binary()}
  defp build_decomposed_keys(epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_sequencer = encode_component_for_storage(transaction_system_layout.sequencer, cluster)
    encoded_proxies = encode_components_for_storage(transaction_system_layout.proxies, cluster)

    encoded_resolvers =
      encode_components_for_storage(transaction_system_layout.resolvers, cluster)

    encoded_services = encode_services_for_storage(transaction_system_layout.services, cluster)

    cluster_keys = %{
      SystemKeys.cluster_coordinators() => :erlang.term_to_binary(cluster_config.coordinators),
      SystemKeys.cluster_epoch() => :erlang.term_to_binary(epoch),
      SystemKeys.cluster_policies_volunteer_nodes() =>
        :erlang.term_to_binary(cluster_config.policies.allow_volunteer_nodes_to_join),
      SystemKeys.cluster_parameters_desired_logs() =>
        :erlang.term_to_binary(cluster_config.parameters.desired_logs),
      SystemKeys.cluster_parameters_desired_replication() =>
        :erlang.term_to_binary(cluster_config.parameters.desired_replication_factor),
      SystemKeys.cluster_parameters_desired_commit_proxies() =>
        :erlang.term_to_binary(cluster_config.parameters.desired_commit_proxies),
      SystemKeys.cluster_parameters_desired_coordinators() =>
        :erlang.term_to_binary(cluster_config.parameters.desired_coordinators),
      SystemKeys.cluster_parameters_desired_read_version_proxies() =>
        :erlang.term_to_binary(cluster_config.parameters.desired_read_version_proxies),
      SystemKeys.cluster_parameters_empty_transaction_timeout_ms() =>
        :erlang.term_to_binary(
          Map.get(cluster_config.parameters, :empty_transaction_timeout_ms, 1_000)
        ),
      SystemKeys.cluster_parameters_ping_rate_in_hz() =>
        :erlang.term_to_binary(cluster_config.parameters.ping_rate_in_hz),
      SystemKeys.cluster_parameters_retransmission_rate_in_hz() =>
        :erlang.term_to_binary(cluster_config.parameters.retransmission_rate_in_hz),
      SystemKeys.cluster_parameters_transaction_window_in_ms() =>
        :erlang.term_to_binary(cluster_config.parameters.transaction_window_in_ms)
    }

    layout_keys = %{
      SystemKeys.layout_sequencer() => :erlang.term_to_binary(encoded_sequencer),
      SystemKeys.layout_proxies() => :erlang.term_to_binary(encoded_proxies),
      SystemKeys.layout_resolvers() => :erlang.term_to_binary(encoded_resolvers),
      SystemKeys.layout_services() => :erlang.term_to_binary(encoded_services),
      SystemKeys.layout_director() =>
        :erlang.term_to_binary(encode_component_for_storage(self(), cluster)),
      SystemKeys.layout_rate_keeper() => :erlang.term_to_binary(nil),
      SystemKeys.layout_id() => :erlang.term_to_binary(transaction_system_layout.id)
    }

    log_keys =
      transaction_system_layout.logs
      |> Enum.into(%{}, fn {log_id, log_descriptor} ->
        encoded_descriptor =
          log_descriptor
          |> encode_log_descriptor_for_storage(cluster)
          |> :erlang.term_to_binary()

        {SystemKeys.layout_log(log_id), encoded_descriptor}
      end)

    storage_keys =
      transaction_system_layout.storage_teams
      |> Enum.with_index()
      |> Enum.into(%{}, fn {storage_team, index} ->
        team_id = "team_#{index}"

        encoded_team =
          storage_team
          |> encode_storage_team_for_storage(cluster)
          |> :erlang.term_to_binary()

        {SystemKeys.layout_storage_team(team_id), encoded_team}
      end)

    recovery_keys = %{
      SystemKeys.recovery_attempt() => :erlang.term_to_binary(1),
      SystemKeys.recovery_last_completed() =>
        System.system_time(:millisecond) |> :erlang.term_to_binary()
    }

    [cluster_keys, layout_keys, log_keys, storage_keys, recovery_keys]
    |> Enum.reduce(%{}, &Map.merge/2)
  end

  @spec encode_component_for_storage(nil | pid() | {Bedrock.key(), pid()}, module()) ::
          nil | pid() | {Bedrock.key(), pid()}
  defp encode_component_for_storage(nil, _cluster), do: nil
  defp encode_component_for_storage(pid, _cluster) when is_pid(pid), do: pid

  defp encode_component_for_storage({start_key, pid}, _cluster) when is_pid(pid),
    do: {start_key, pid}

  defp encode_components_for_storage(components, cluster) when is_list(components),
    do: Enum.map(components, &encode_component_for_storage(&1, cluster))

  defp encode_services_for_storage(services, _cluster) when is_map(services), do: services

  @spec encode_log_descriptor_for_storage([term()], module()) :: [term()]
  defp encode_log_descriptor_for_storage(log_descriptor, _cluster) do
    # Log descriptors typically don't contain PIDs directly
    log_descriptor
  end

  @spec encode_storage_team_for_storage(map(), module()) :: map()
  defp encode_storage_team_for_storage(storage_team, _cluster) do
    # Storage team descriptors typically don't contain PIDs directly
    storage_team
  end

  @spec submit_system_transaction(Bedrock.transaction(), [pid()], map()) ::
          {:ok, Bedrock.version()} | {:error, :no_commit_proxies | :timeout | :unavailable}
  defp submit_system_transaction(_system_transaction, [], _context),
    do: {:error, :no_commit_proxies}

  defp submit_system_transaction(system_transaction, proxies, context) when is_list(proxies) do
    commit_fn = Map.get(context, :commit_transaction_fn, &CommitProxy.commit/2)

    proxies
    |> Enum.random()
    |> then(&commit_fn.(&1, system_transaction))
  end
end
