defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhase do
  @moduledoc """
  Handles the :persist_system_state phase of recovery.

  This phase is responsible for persisting cluster state via a system
  transaction that serves as both persistence and comprehensive system test.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys

  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  @behaviour RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the persistence phase of recovery.

  Validates recovery state, builds cluster configuration, creates system
  transaction, and submits it to test the entire transaction pipeline.
  """
  @impl true
  def execute(%RecoveryAttempt{state: :persist_system_state} = recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    with :ok <- validate_recovery_state(recovery_attempt, context.available_services),
         {:ok, transaction_system_layout} <- build_transaction_system_layout(recovery_attempt),
         system_transaction <-
           build_system_transaction(
             recovery_attempt.epoch,
             context.cluster_config,
             recovery_attempt.cluster
           ),
         :ok <-
           unlock_services(
             recovery_attempt,
             transaction_system_layout,
             context.lock_token
           ),
         {:ok, _version} <-
           submit_system_transaction(system_transaction, recovery_attempt.proxies) do
      trace_recovery_system_state_persisted()
      %{recovery_attempt | state: :persist_coordinator_config}
    else
      {:error, reason} ->
        trace_recovery_system_transaction_failed(reason)
        # Fail fast - exit director and let coordinator retry
        exit({:recovery_system_test_failed, reason})
    end
  end

  defp build_transaction_system_layout(recovery_attempt) do
    # Build service descriptors from the actual service PIDs collected during recruitment
    services = build_service_descriptors(recovery_attempt.service_pids)

    {:ok,
     %{
       id: TransactionSystemLayout.random_id(),
       director: self(),
       sequencer: recovery_attempt.sequencer,
       rate_keeper: nil,
       proxies: recovery_attempt.proxies,
       resolvers: recovery_attempt.resolvers,
       logs: recovery_attempt.logs,
       storage_teams: recovery_attempt.storage_teams,
       services: services
     }}
  end

  @spec build_service_descriptors(%{Worker.id() => pid()}) :: %{
          Worker.id() => ServiceDescriptor.t()
        }
  defp build_service_descriptors(service_pids) do
    service_pids
    |> Enum.map(fn {service_id, pid} ->
      # Build service descriptor from PID
      service_descriptor = %{
        # All services in service_pids are logs from recruitment
        kind: :log,
        # Could be enhanced later with actual tracking
        last_seen: nil,
        status: {:up, pid}
      }

      {service_id, service_descriptor}
    end)
    |> Map.new()
  end

  @spec build_system_transaction(
          epoch :: non_neg_integer(),
          cluster_config :: Config.t(),
          cluster :: module()
        ) :: Bedrock.transaction()
  defp build_system_transaction(epoch, cluster_config, cluster) do
    encoded_config = Persistence.encode_for_storage(cluster_config, cluster)
    transaction_system_layout = Map.get(cluster_config, :transaction_system_layout)

    encoded_layout =
      Persistence.encode_transaction_system_layout_for_storage(transaction_system_layout, cluster)

    monolithic_keys = build_monolithic_keys(epoch, encoded_config, encoded_layout)
    decomposed_keys = build_decomposed_keys(epoch, cluster_config, cluster)

    all_keys = Map.merge(monolithic_keys, decomposed_keys)

    {nil, all_keys}
  end

  # Build monolithic keys for backward compatibility and coordinator handoff
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

  # Build decomposed keys for targeted component consumption
  @spec build_decomposed_keys(Bedrock.epoch(), Config.t(), module()) :: %{
          Bedrock.key() => binary()
        }
  defp build_decomposed_keys(epoch, cluster_config, cluster) do
    transaction_system_layout = Map.get(cluster_config, :transaction_system_layout)

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
        encoded_log = encode_log_descriptor_for_storage(log_descriptor, cluster)
        {SystemKeys.layout_log(log_id), :erlang.term_to_binary(encoded_log)}
      end)

    storage_keys =
      transaction_system_layout.storage_teams
      |> Enum.with_index()
      |> Enum.into(%{}, fn {storage_team, index} ->
        team_id = "team_#{index}"
        encoded_team = encode_storage_team_for_storage(storage_team, cluster)
        {SystemKeys.layout_storage_team(team_id), :erlang.term_to_binary(encoded_team)}
      end)

    recovery_keys = %{
      SystemKeys.recovery_attempt() => :erlang.term_to_binary(1),
      SystemKeys.recovery_last_completed() =>
        :erlang.term_to_binary(System.system_time(:millisecond))
    }

    Map.merge(cluster_keys, layout_keys)
    |> Map.merge(log_keys)
    |> Map.merge(storage_keys)
    |> Map.merge(recovery_keys)
  end

  @spec encode_component_for_storage(nil | pid() | {Bedrock.key(), pid()} | map(), module()) ::
          nil | pid() | {Bedrock.key(), pid()} | map()
  defp encode_component_for_storage(nil, _cluster), do: nil
  defp encode_component_for_storage(pid, _cluster) when is_pid(pid), do: pid

  defp encode_component_for_storage({start_key, pid}, _cluster) when is_pid(pid) do
    # Handle resolver tuples {start_key, pid}
    {start_key, pid}
  end

  defp encode_component_for_storage(%{resolver: pid} = resolver_map, _cluster) when is_pid(pid) do
    # Handle resolver maps %{resolver: pid, start_key: key}
    resolver_map
  end

  defp encode_component_for_storage(%{resolver: nil} = resolver_map, _cluster) do
    # Handle resolver maps with nil resolver
    resolver_map
  end

  defp encode_component_for_storage(other, _cluster) do
    # Pass through other formats as-is
    other
  end

  @spec encode_components_for_storage([pid() | {Bedrock.key(), pid()} | map()], module()) :: [
          pid() | {Bedrock.key(), pid()} | map()
        ]
  defp encode_components_for_storage(components, cluster) when is_list(components) do
    Enum.map(components, &encode_component_for_storage(&1, cluster))
  end

  @spec encode_services_for_storage(%{Worker.id() => ServiceDescriptor.t()}, module()) :: %{
          Worker.id() => ServiceDescriptor.t()
        }
  defp encode_services_for_storage(services, _cluster) when is_map(services) do
    # For decomposed keys, store services as-is for now
    # The monolithic keys already handle proper encoding
    services
  end

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

  # Validate that recovery state is ready for system transaction
  @spec validate_recovery_state(
          RecoveryAttempt.t(),
          available_services :: %{Worker.id() => ServiceDescriptor.t()}
        ) ::
          :ok | {:error, {:invalid_recovery_state, atom() | {atom(), [binary()]}}}
  defp validate_recovery_state(recovery_attempt, available_services) do
    with :ok <- validate_sequencer(recovery_attempt.sequencer),
         :ok <- validate_commit_proxies(recovery_attempt.proxies),
         :ok <- validate_resolvers(recovery_attempt.resolvers),
         :ok <- validate_logs(recovery_attempt.logs, available_services) do
      :ok
    else
      {:error, reason} -> {:error, {:invalid_recovery_state, reason}}
    end
  end

  @spec validate_sequencer(nil | pid()) :: :ok | {:error, atom()}
  defp validate_sequencer(nil), do: {:error, :no_sequencer}
  defp validate_sequencer(sequencer) when is_pid(sequencer), do: :ok
  defp validate_sequencer(_), do: {:error, :invalid_sequencer}

  @spec validate_commit_proxies([pid()]) :: :ok | {:error, atom()}
  defp validate_commit_proxies([]), do: {:error, :no_commit_proxies}

  defp validate_commit_proxies(proxies) when is_list(proxies) do
    if Enum.all?(proxies, &is_pid/1) do
      :ok
    else
      {:error, :invalid_commit_proxies}
    end
  end

  defp validate_commit_proxies(_), do: {:error, :invalid_commit_proxies}

  @spec validate_resolvers([pid() | {Bedrock.key(), pid()} | map()]) :: :ok | {:error, atom()}
  defp validate_resolvers([]), do: {:error, :no_resolvers}

  defp validate_resolvers(resolvers) when is_list(resolvers) do
    # Resolvers can be either PIDs directly or tuples of {start_key, pid}
    valid_resolvers =
      Enum.all?(resolvers, fn
        pid when is_pid(pid) -> true
        {_start_key, pid} when is_pid(pid) -> true
        %{resolver: pid} when is_pid(pid) -> true
        # Invalid resolver descriptor
        %{resolver: nil} -> false
        _ -> false
      end)

    if valid_resolvers do
      :ok
    else
      {:error, :invalid_resolvers}
    end
  end

  @spec validate_logs(%{Log.id() => LogDescriptor.t()}, %{Worker.id() => ServiceDescriptor.t()}) ::
          :ok | {:error, {:missing_log_services, [binary()]}}
  defp validate_logs(logs, available_services) when is_map(logs) do
    log_ids = Map.keys(logs)

    # Check that all log IDs have corresponding services
    missing_services =
      log_ids
      |> Enum.reject(fn log_id ->
        case Map.get(available_services, log_id) do
          %{kind: :log, status: {:up, _pid}} -> true
          _ -> false
        end
      end)

    case missing_services do
      [] -> :ok
      missing -> {:error, {:missing_log_services, missing}}
    end
  end

  @spec submit_system_transaction(Bedrock.transaction(), [pid()]) ::
          {:ok, Bedrock.version()} | {:error, :no_commit_proxies | :timeout | :unavailable}
  defp submit_system_transaction(_system_transaction, []), do: {:error, :no_commit_proxies}

  defp submit_system_transaction(system_transaction, proxies) when is_list(proxies) do
    proxies
    |> Enum.random()
    |> CommitProxy.commit(system_transaction)
  end

  # Unlock commit proxies before exercising the transaction system
  @spec unlock_services(RecoveryAttempt.t(), TransactionSystemLayout.t(), Bedrock.lock_token()) ::
          :ok | {:error, {:unlock_failed, :timeout | :unavailable}}
  defp unlock_services(
         recovery_attempt,
         transaction_system_layout,
         lock_token
       )
       when is_binary(lock_token) do
    with :ok <-
           unlock_commit_proxies(recovery_attempt.proxies, transaction_system_layout, lock_token) do
      :ok
    else
      {:error, reason} -> {:error, {:unlock_failed, reason}}
    end
  end

  @spec unlock_commit_proxies([pid()], TransactionSystemLayout.t(), Bedrock.lock_token()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_commit_proxies(proxies, transaction_system_layout, lock_token)
       when is_list(proxies) do
    proxies
    |> Task.async_stream(
      fn proxy ->
        CommitProxy.recover_from(proxy, lock_token, transaction_system_layout)
      end,
      ordered: false
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, {:commit_proxy_unlock_failed, reason}}}
      {:exit, reason}, _ -> {:halt, {:error, {:commit_proxy_unlock_crashed, reason}}}
    end)
  end
end
