defmodule Bedrock.Test.ControlPlane.RecoveryTestSupport do
  @moduledoc """
  Shared test utilities and fixtures for recovery tests.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.DataPlane.Version

  # Helper for best-effort cleanup
  defp cleanup(fun), do: on_exit(fn -> try do: fun.(), catch: (_, _ -> :ok) end)

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false

    def name, do: "test_cluster"
    def otp_name(component), do: :"test_#{component}"
  end

  @doc """
  Sets up logger metadata for recovery tests.
  """
  def setup_recovery_metadata(cluster \\ TestCluster, epoch \\ 1) do
    Logger.metadata(cluster: cluster, epoch: epoch)
    on_exit(fn -> Logger.metadata([]) end)
    :ok
  end

  @doc """
  Creates a basic test context with node capabilities and old transaction system layout.
  """
  def create_test_context(opts \\ []) do
    node_capabilities =
      Keyword.get(opts, :node_capabilities, %{
        log: [Node.self()],
        storage: [Node.self()],
        coordination: [Node.self()],
        resolution: [Node.self()]
      })

    old_transaction_system_layout =
      Keyword.get(opts, :old_transaction_system_layout, %{logs: %{}})

    %{
      node_capabilities: node_capabilities,
      old_transaction_system_layout: old_transaction_system_layout,
      cluster_config: %{
        coordinators: [],
        parameters: %{
          desired_logs: 2,
          desired_replication_factor: 3,
          desired_commit_proxies: 1,
          desired_coordinators: 1,
          desired_read_version_proxies: 1,
          ping_rate_in_hz: 10,
          retransmission_rate_in_hz: 5,
          transaction_window_in_ms: 1000
        },
        policies: %{
          allow_volunteer_nodes_to_join: true
        }
      },
      available_services: %{},
      lock_token: "test_token",
      coordinator: self()
    }
  end

  # ============================================================================
  # RecoveryAttempt Factory Functions
  # ============================================================================

  @doc """
  Creates a base recovery attempt with common defaults.
  """
  def recovery_attempt(overrides \\ %{}) do
    base = %RecoveryAttempt{
      cluster: TestCluster,
      epoch: 1,
      attempt: 1,
      started_at: DateTime.utc_now(),
      required_services: %{},
      locked_service_ids: MapSet.new(),
      log_recovery_info_by_id: %{},
      materializer_recovery_info_by_id: %{},
      old_log_ids_to_copy: [],
      version_vector: {Version.from_integer(0), Version.from_integer(0)},
      durable_version: Version.from_integer(0),
      logs: %{},
      resolvers: [],
      proxies: [],
      sequencer: nil,
      transaction_services: %{},
      service_pids: %{},
      transaction_system_layout: nil,
      metadata_materializer: nil,
      shard_layout: nil
    }

    struct(base, overrides)
  end

  @doc """
  Creates a recovery context with common defaults.
  """
  def recovery_context(overrides \\ %{}), do: Map.merge(create_test_context(), overrides)

  # ============================================================================
  # Preset Recovery Scenarios
  # ============================================================================

  @doc """
  Creates a first-time recovery attempt (new cluster).
  """
  def first_time_recovery(overrides \\ %{}), do: recovery_attempt(overrides)

  @doc """
  Creates an existing cluster recovery attempt with sample old data.
  """
  def existing_cluster_recovery(overrides \\ %{}) do
    base = %{
      epoch: 2,
      log_recovery_info_by_id: %{
        {:log, 1} => %{
          kind: :log,
          oldest_version: Version.zero(),
          last_version: Version.from_integer(100)
        },
        {:log, 2} => %{
          kind: :log,
          oldest_version: Version.zero(),
          last_version: Version.from_integer(100)
        }
      },
      materializer_recovery_info_by_id: %{
        "storage_1" => %{
          kind: :materializer,
          durable_version: Version.from_integer(95),
          oldest_durable_version: Version.zero()
        },
        "storage_2" => %{
          kind: :materializer,
          durable_version: Version.from_integer(95),
          oldest_durable_version: Version.zero()
        }
      }
    }

    base |> recovery_attempt() |> Map.merge(overrides)
  end

  @doc """
  Creates a minimal valid recovery attempt that passes basic validation.
  """
  def minimal_valid_recovery(overrides \\ %{}) do
    base = %{
      sequencer: self(),
      proxies: [self()],
      resolvers: [{"start_key", self()}],
      logs: %{"log_1" => %{}},
      transaction_services: %{
        "log_1" => %{kind: :log, last_seen: {:log_1, :node1}, status: {:up, self()}}
      }
    }

    recovery_attempt(Map.merge(base, overrides))
  end

  # ============================================================================
  # Pipeable Transformation Functions for RecoveryAttempt
  # ============================================================================

  @doc """
  Sets the cluster.
  """
  def with_cluster(recovery_attempt, cluster), do: Map.put(recovery_attempt, :cluster, cluster)

  @doc """
  Sets the epoch.
  """
  def with_epoch(recovery_attempt, epoch), do: Map.put(recovery_attempt, :epoch, epoch)

  @doc """
  Sets logs. Can accept a map of logs or an integer count to generate simple logs.
  """
  def with_logs(recovery_attempt, logs) when is_map(logs), do: Map.put(recovery_attempt, :logs, logs)

  def with_logs(recovery_attempt, count) when is_integer(count) do
    logs = for i <- 1..count, into: %{}, do: {"log_#{i}", %{}}
    Map.put(recovery_attempt, :logs, logs)
  end

  @doc """
  Sets the sequencer PID.
  """
  def with_sequencer(recovery_attempt, sequencer), do: Map.put(recovery_attempt, :sequencer, sequencer)

  @doc """
  Sets commit proxies.
  """
  def with_proxies(recovery_attempt, proxies), do: Map.put(recovery_attempt, :proxies, proxies)

  @doc """
  Sets resolvers.
  """
  def with_resolvers(recovery_attempt, resolvers), do: Map.put(recovery_attempt, :resolvers, resolvers)

  @doc """
  Sets version vector.
  """
  def with_version_vector(recovery_attempt, {first, last}),
    do: Map.put(recovery_attempt, :version_vector, {first, last})

  @doc """
  Sets transaction services.
  """
  def with_transaction_services(recovery_attempt, services),
    do: Map.put(recovery_attempt, :transaction_services, services)

  @doc """
  Sets storage recovery info by ID.
  """
  def with_storage_recovery_info(recovery_attempt, info),
    do: Map.put(recovery_attempt, :materializer_recovery_info_by_id, info)

  @doc """
  Sets log recovery info by ID.
  """
  def with_log_recovery_info(recovery_attempt, info), do: Map.put(recovery_attempt, :log_recovery_info_by_id, info)

  @doc """
  Sets required services.
  """
  def with_required_services(recovery_attempt, services), do: Map.put(recovery_attempt, :required_services, services)

  # ============================================================================
  # Pipeable Transformation Functions for Recovery Context
  # ============================================================================

  @doc """
  Sets up node capabilities with specified number of nodes or custom setup.
  """
  def with_node_tracking(context, opts) do
    node_capabilities =
      case Keyword.get(opts, :nodes) do
        nil ->
          context.node_capabilities

        count when is_integer(count) ->
          nodes = if count > 0, do: for(i <- 1..count, do: :"node#{i}@host"), else: []
          %{log: nodes, storage: nodes}

        nodes when is_list(nodes) ->
          %{log: nodes, storage: nodes}
      end

    Map.put(context, :node_capabilities, node_capabilities)
  end

  @doc """
  Creates a simple mock node capabilities for testing.
  """
  def mock_node_tracking(nodes \\ [:node1@host]) do
    %{log: nodes, storage: nodes}
  end

  @doc """
  Sets old transaction system layout.
  """
  def with_old_layout(context, opts) do
    layout = %{
      logs:
        case Keyword.get(opts, :logs) do
          nil ->
            %{}

          count when is_integer(count) ->
            for i <- 1..count, into: %{}, do: {{:log, i}, ["tag_#{i}"]}

          logs when is_map(logs) ->
            logs
        end
    }

    Map.put(context, :old_transaction_system_layout, layout)
  end

  @doc """
  Sets available services of a specific type.
  """
  def with_available_services(context, service_type, spec) do
    services =
      case {service_type, spec} do
        {:logs, count} when is_integer(count) ->
          for i <- 1..count, into: %{} do
            {"log_#{i}", %{kind: :log, status: {:up, spawn(fn -> :ok end)}}}
          end

        {:materializer, count} when is_integer(count) ->
          for i <- 1..count, into: %{} do
            {"storage_#{i}", %{kind: :materializer, status: {:up, spawn(fn -> :ok end)}}}
          end

        {_, services} when is_map(services) ->
          services
      end

    Map.put(context, :available_services, services)
  end

  @doc """
  Sets available services directly.
  """
  def with_available_services(context, services) when is_map(services),
    do: Map.put(context, :available_services, services)

  @doc """
  Adds additional available services to existing services.
  Use this when you want to add services rather than replace them.
  """
  def add_available_services(context, service_type, spec) do
    new_services =
      case {service_type, spec} do
        {:logs, count} when is_integer(count) ->
          for i <- 1..count, into: %{} do
            {"log_#{i}", %{kind: :log, status: {:up, spawn(fn -> :ok end)}}}
          end

        {:materializer, count} when is_integer(count) ->
          for i <- 1..count, into: %{} do
            {"storage_#{i}", %{kind: :materializer, status: {:up, spawn(fn -> :ok end)}}}
          end

        {_, services} when is_map(services) ->
          services
      end

    current_services = Map.get(context, :available_services, %{})
    Map.put(context, :available_services, Map.merge(current_services, new_services))
  end

  @doc """
  Adds additional available services to existing services (map version).
  """
  def add_available_services(context, services) when is_map(services) do
    current_services = Map.get(context, :available_services, %{})
    Map.put(context, :available_services, Map.merge(current_services, services))
  end

  @doc """
  Sets cluster configuration.
  """
  def with_cluster_config(context, cluster_config) when is_map(cluster_config),
    do: Map.put(context, :cluster_config, cluster_config)

  @doc """
  Merges additional cluster configuration with existing config.
  Use this when you want to add to existing config rather than replace it.
  """
  def merge_cluster_config(context, overrides) when is_map(overrides) do
    current_config = Map.get(context, :cluster_config, %{})
    updated_config = Map.merge(current_config, overrides)
    Map.put(context, :cluster_config, updated_config)
  end

  @doc """
  Sets a lock token.
  """
  def with_lock_token(context, token), do: Map.put(context, :lock_token, token)

  @doc """
  Creates a basic cluster config with sensible defaults.
  """
  def basic_cluster_config do
    %{
      coordinators: [],
      parameters: %{
        desired_logs: 2,
        desired_replication_factor: 3,
        desired_commit_proxies: 1,
        desired_coordinators: 1,
        desired_read_version_proxies: 1,
        ping_rate_in_hz: 10,
        retransmission_rate_in_hz: 5,
        transaction_window_in_ms: 1000
      },
      policies: %{
        allow_volunteer_nodes_to_join: true
      }
    }
  end

  @doc """
  Sets cluster coordinators.
  """
  def with_coordinators(cluster_config, coordinators) when is_list(coordinators),
    do: Map.put(cluster_config, :coordinators, coordinators)

  @doc """
  Sets cluster parameters, replacing any existing parameters.
  """
  def with_parameters(cluster_config, params) when is_map(params), do: Map.put(cluster_config, :parameters, params)

  @doc """
  Merges cluster parameters with existing parameters.
  Use this when you want to add to existing parameters rather than replace them.
  """
  def merge_parameters(cluster_config, params) when is_map(params) do
    current_params = Map.get(cluster_config, :parameters, %{})
    Map.put(cluster_config, :parameters, Map.merge(current_params, params))
  end

  @doc """
  Sets cluster policies, replacing any existing policies.
  """
  def with_policies(cluster_config, policies) when is_map(policies), do: Map.put(cluster_config, :policies, policies)

  @doc """
  Merges cluster policies with existing policies.
  Use this when you want to add to existing policies rather than replace them.
  """
  def merge_policies(cluster_config, policies) when is_map(policies) do
    current_policies = Map.get(cluster_config, :policies, %{})
    Map.put(cluster_config, :policies, Map.merge(current_policies, policies))
  end

  @doc """
  Sets transaction system layout.
  """
  def with_transaction_system_layout(cluster_config, layout),
    do: Map.put(cluster_config, :transaction_system_layout, layout)

  @doc """
  Adds mock functions to the context.
  """
  def with_mocks(context, mock_types) when is_list(mock_types) do
    Enum.reduce(mock_types, context, &add_mock_function/2)
  end

  def with_mocks(context, mock_type) when is_atom(mock_type) do
    add_mock_function(mock_type, context)
  end

  # Private helper for adding individual mock functions
  defp add_mock_function(:service_locking, context) do
    lock_service_fn = fn _service, _epoch ->
      pid = spawn(fn -> :ok end)

      {:ok, pid,
       %{
         kind: :test,
         durable_version: Version.from_integer(95),
         oldest_version: Version.zero(),
         last_version: Version.from_integer(100)
       }}
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp add_mock_function(:worker_creation, context) do
    create_worker_fn = fn _foreman_ref, worker_id, _kind, _opts -> {:ok, "#{worker_id}_ref"} end
    Map.put(context, :create_worker_fn, create_worker_fn)
  end

  defp add_mock_function(:monitoring, context), do: Map.put(context, :monitor_fn, fn _pid -> make_ref() end)

  defp add_mock_function(:commit_proxy, context) do
    commit_transaction_fn =
      Map.get(context, :commit_transaction_fn, fn _proxy, _transaction ->
        {:ok, :rand.uniform(1000)}
      end)

    unlock_commit_proxy_fn = fn _proxy, _lock_token, _layout -> :ok end

    context
    |> Map.put(:commit_transaction_fn, commit_transaction_fn)
    |> Map.put(:unlock_commit_proxy_fn, unlock_commit_proxy_fn)
  end

  defp add_mock_function(:log_recovery, context) do
    copy_log_data_fn = fn _new_log_id, _old_log_id, _first_version, _last_version, _service_pids ->
      {:ok, spawn(fn -> :ok end)}
    end

    Map.put(context, :copy_log_data_fn, copy_log_data_fn)
  end

  defp add_mock_function(:service_discovery, context) do
    discover_services_fn = fn
      :log -> %{"log_1" => %{kind: :log, status: {:up, self()}}}
      :materializer -> %{"storage_1" => %{kind: :materializer, status: {:up, self()}}}
      _ -> %{}
    end

    service_health_fn = fn _service_id -> {:up, self()} end

    context
    |> Map.put(:discover_services_fn, discover_services_fn)
    |> Map.put(:service_health_fn, service_health_fn)
  end

  defp add_mock_function(:supervision, context) do
    start_supervised_fn = Map.get(context, :start_supervised_fn, fn _spec -> {:ok, self()} end)
    supervise_worker_fn = fn _worker_ref -> {:ok, make_ref()} end
    restart_worker_fn = fn _worker_ref -> {:ok, self()} end

    context
    |> Map.put(:start_supervised_fn, start_supervised_fn)
    |> Map.put(:supervise_worker_fn, supervise_worker_fn)
    |> Map.put(:restart_worker_fn, restart_worker_fn)
  end

  defp add_mock_function(_unknown_mock, context), do: context

  # ============================================================================
  # Failure Scenario Helpers
  # ============================================================================

  @doc """
  Adds failure scenarios to the context for testing error paths.
  """
  def with_failure_scenarios(context, failure_types) when is_list(failure_types),
    do: Enum.reduce(failure_types, context, &add_failure_scenario/2)

  def with_failure_scenarios(context, failure_type) when is_atom(failure_type),
    do: add_failure_scenario(failure_type, context)

  # Private helper for adding individual failure scenarios
  defp add_failure_scenario(:worker_creation_failures, context) do
    create_worker_fn = fn _foreman_ref, worker_id, _kind, _opts ->
      if String.contains?(worker_id, "fail"),
        do: {:error, :worker_creation_failed},
        else: {:ok, "#{worker_id}_ref"}
    end

    Map.put(context, :create_worker_fn, create_worker_fn)
  end

  defp add_failure_scenario(:service_locking_failures, context) do
    lock_service_fn = fn service, _epoch ->
      handle_service_locking_with_failures(service)
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp add_failure_scenario(:commit_proxy_failures, context) do
    commit_transaction_fn = fn _proxy, transaction ->
      if is_map(transaction) and Map.get(transaction, :should_fail, false),
        do: {:error, :commit_proxy_timeout},
        else: {:ok, :rand.uniform(1000)}
    end

    Map.put(context, :commit_transaction_fn, commit_transaction_fn)
  end

  defp add_failure_scenario(:log_recovery_failures, context) do
    copy_log_data_fn = fn new_log_id, _old_log_id, _first_version, _last_version, service_pids ->
      new_log_pid = Map.get(service_pids, new_log_id)

      if is_pid(new_log_pid) and Process.alive?(new_log_pid),
        do: {:ok, new_log_pid},
        else: {:error, :log_recovery_timeout}
    end

    Map.put(context, :copy_log_data_fn, copy_log_data_fn)
  end

  defp add_failure_scenario(:supervision_failures, context) do
    start_supervised_fn = fn spec ->
      if is_map(spec) and Map.get(spec, :should_fail, false),
        do: {:error, :supervisor_not_found},
        else: {:ok, self()}
    end

    Map.put(context, :start_supervised_fn, start_supervised_fn)
  end

  defp add_failure_scenario(_unknown_failure, context), do: context

  defp handle_service_locking_with_failures(service) do
    if String.contains?(service, "fail") do
      {:error, :service_lock_timeout}
    else
      create_successful_service_lock()
    end
  end

  defp create_successful_service_lock do
    {:ok, spawn(fn -> :ok end),
     %{
       kind: :test,
       durable_version: Version.from_integer(95),
       oldest_version: Version.zero(),
       last_version: Version.from_integer(100)
     }}
  end

  # ============================================================================
  # Enhanced Mock Management
  # ============================================================================

  @doc """
  Creates realistic mock PIDs that simulate actual process behavior.
  """
  def with_realistic_pids(context, pid_type \\ :long_running) do
    mock_pid_fn = create_mock_pid_function(pid_type)
    Map.put(context, :mock_pid_fn, mock_pid_fn)
  end

  defp create_mock_pid_function(pid_type) do
    case pid_type do
      :long_running -> create_long_running_pid_function()
      :short_lived -> create_short_lived_pid_function()
      :immediate_exit -> create_immediate_exit_pid_function()
    end
  end

  defp create_long_running_pid_function do
    fn ->
      pid = spawn(fn -> receive(do: (:shutdown -> :ok)) end)
      cleanup(fn -> send(pid, :shutdown) end)
      pid
    end
  end

  defp create_short_lived_pid_function do
    fn ->
      pid = spawn(fn -> Process.sleep(50) end)
      cleanup(fn -> Process.exit(pid, :kill) end)
      pid
    end
  end

  defp create_immediate_exit_pid_function do
    fn -> spawn(fn -> :ok end) end
  end

  # Private helper for GenServer mock loop
  defp loop_gen_server_mock do
    receive do
      {:"$gen_call", from, msg} ->
        GenServer.reply(from, {:ok, "mock_response_#{inspect(msg)}"})
        loop_gen_server_mock()

      :shutdown ->
        :ok
    end
  end

  @doc """
  Creates a managed mock process that will be cleaned up on test exit.
  """
  def create_managed_mock_process(behavior \\ :long_running) do
    pid =
      case behavior do
        :long_running -> spawn(fn -> receive(do: (:shutdown -> :ok)) end)
        :short_lived -> spawn(fn -> Process.sleep(50) end)
        :immediate_exit -> spawn(fn -> :ok end)
        :gen_server_mock -> spawn(fn -> loop_gen_server_mock() end)
      end

    if behavior != :immediate_exit do
      cleanup(fn -> terminate_managed_process(pid, behavior) end)
    end

    pid
  end

  defp terminate_managed_process(pid, behavior) do
    case behavior do
      b when b in [:gen_server_mock, :long_running] -> send(pid, :shutdown)
      _ -> Process.exit(pid, :kill)
    end
  end

  @doc """
  Centralizes the TestCluster module definition.
  """
  def default_test_cluster, do: TestCluster
end
