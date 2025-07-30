defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhase do
  @moduledoc """
  Persists cluster configuration and tests the complete transaction system.

  Constructs a system transaction containing the full cluster configuration and
  submits it through the entire data plane pipeline. This simultaneously persists
  the new configuration and validates that all transaction components work correctly.

  Unlocks services before submitting the transaction since the transaction itself
  requires unlocked services to process. Commit proxies are configured with the
  new layout and storage servers are unlocked with the durable version.

  Stores configuration in both monolithic and decomposed formats. Monolithic keys
  support coordinator handoff while decomposed keys allow targeted component access.

  If the system transaction fails, the director exits immediately rather than
  retrying. System transaction failure indicates fundamental problems that require
  coordinator restart with a new epoch.

  Transitions to monitoring on success or exits the director on failure.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Storage
  alias Bedrock.SystemKeys

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    with {:ok, transaction_system_layout} <-
           build_transaction_system_layout(recovery_attempt, context),
         system_transaction <-
           build_system_transaction(
             recovery_attempt.epoch,
             context.cluster_config,
             transaction_system_layout,
             recovery_attempt.cluster
           ),
         :ok <-
           unlock_services(
             recovery_attempt,
             transaction_system_layout,
             context.lock_token,
             context
           ),
         {:ok, _version} <-
           submit_system_transaction(system_transaction, recovery_attempt.proxies, context) do
      trace_recovery_system_state_persisted()

      updated_recovery_attempt =
        %{recovery_attempt | transaction_system_layout: transaction_system_layout}

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MonitoringPhase}
    else
      {:error, reason} ->
        trace_recovery_system_transaction_failed(reason)
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  defp build_transaction_system_layout(recovery_attempt, context) do
    {:ok,
     %{
       id: TransactionSystemLayout.random_id(),
       epoch: recovery_attempt.epoch,
       director: self(),
       sequencer: recovery_attempt.sequencer,
       rate_keeper: nil,
       proxies: recovery_attempt.proxies,
       resolvers: recovery_attempt.resolvers,
       logs: recovery_attempt.logs,
       storage_teams: recovery_attempt.storage_teams,
       services:
         build_services_for_layout(
           recovery_attempt,
           context
         )
     }}
  end

  @spec build_services_for_layout(RecoveryAttempt.t(), RecoveryPhase.context()) ::
          %{String.t() => ServiceDescriptor.t()}
  defp build_services_for_layout(recovery_attempt, context) do
    recovery_attempt
    |> extract_service_ids()
    |> Enum.map(fn service_id ->
      {service_id, build_service_descriptor(service_id, recovery_attempt, context)}
    end)
    |> Enum.reject(fn {_id, descriptor} -> is_nil(descriptor) end)
    |> Map.new()
  end

  defp extract_service_ids(recovery_attempt) do
    [extract_log_service_ids(recovery_attempt), extract_storage_service_ids(recovery_attempt)]
    |> Enum.reduce(MapSet.new(), &MapSet.union/2)
  end

  defp extract_log_service_ids(recovery_attempt) do
    recovery_attempt.logs
    |> Map.keys()
    |> MapSet.new()
  end

  defp extract_storage_service_ids(recovery_attempt) do
    recovery_attempt.storage_teams
    |> Enum.flat_map(& &1.storage_ids)
    |> MapSet.new()
  end

  @spec build_service_descriptor(
          String.t(),
          RecoveryAttempt.t(),
          RecoveryPhase.context()
        ) :: ServiceDescriptor.t() | nil
  defp build_service_descriptor(service_id, recovery_attempt, context) do
    case Map.get(context.available_services, service_id) do
      {kind, last_seen} = _service ->
        status = determine_service_status(service_id, recovery_attempt.service_pids)

        %{
          kind: kind,
          last_seen: last_seen,
          status: status
        }

      _ ->
        # Service not found in available services
        nil
    end
  end

  @spec determine_service_status(String.t(), %{String.t() => pid()}) :: ServiceDescriptor.status()
  defp determine_service_status(service_id, service_pids) do
    case Map.get(service_pids, service_id) do
      pid when is_pid(pid) -> {:up, pid}
      _ -> :down
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

  # Unlock commit proxies and storage servers before exercising the transaction system
  @spec unlock_services(
          RecoveryAttempt.t(),
          TransactionSystemLayout.t(),
          Bedrock.lock_token(),
          map()
        ) ::
          :ok | {:error, {:unlock_failed, :timeout | :unavailable}}
  defp unlock_services(
         recovery_attempt,
         transaction_system_layout,
         lock_token,
         context
       )
       when is_binary(lock_token) do
    with :ok <-
           unlock_commit_proxies(
             recovery_attempt.proxies,
             transaction_system_layout,
             lock_token,
             context
           ),
         :ok <-
           unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
      :ok
    else
      {:error, reason} -> {:error, {:unlock_failed, reason}}
    end
  end

  @spec unlock_commit_proxies([pid()], TransactionSystemLayout.t(), Bedrock.lock_token(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_commit_proxies(proxies, transaction_system_layout, lock_token, context)
       when is_list(proxies) do
    unlock_fn = Map.get(context, :unlock_commit_proxy_fn, &CommitProxy.recover_from/3)

    proxies
    |> Task.async_stream(
      &unlock_fn.(&1, lock_token, transaction_system_layout),
      ordered: false
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, {:commit_proxy_unlock_failed, reason}}}
      {:exit, reason}, _ -> {:halt, {:error, {:commit_proxy_unlock_crashed, reason}}}
    end)
  end

  @spec unlock_storage_servers(RecoveryAttempt.t(), TransactionSystemLayout.t(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
    durable_version = recovery_attempt.durable_version
    unlock_fn = Map.get(context, :unlock_storage_fn, &Storage.unlock_after_recovery/3)

    transaction_system_layout.storage_teams
    |> Enum.flat_map(fn %{storage_ids: storage_ids} -> storage_ids end)
    |> Enum.uniq()
    |> Enum.map(fn storage_id ->
      recovery_attempt.transaction_services
      |> Map.fetch!(storage_id)
      |> then(fn %{status: {:up, pid}} -> {storage_id, pid} end)
    end)
    |> Task.async_stream(
      fn {storage_id, storage_pid} ->
        {storage_id, unlock_fn.(storage_pid, durable_version, transaction_system_layout)}
      end,
      ordered: false,
      timeout: 5000
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, {storage_id, :ok}}, :ok ->
        trace_recovery_storage_unlocking(storage_id)
        {:cont, :ok}

      {:ok, {_storage_id, {:error, reason}}}, _ ->
        {:halt, {:error, {:storage_unlock_failed, reason}}}

      {:exit, reason}, _ ->
        {:halt, {:error, {:storage_unlock_crashed, reason}}}
    end)
  end
end
