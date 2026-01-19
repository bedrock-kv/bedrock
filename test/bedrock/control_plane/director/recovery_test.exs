defmodule Bedrock.ControlPlane.Director.RecoveryTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.DataPlane.Version

  # Helper to create test state with node capabilities
  defp create_test_state(overrides \\ %{}) do
    node_capabilities = %{
      coordination: [Node.self()],
      log: [Node.self()],
      storage: [Node.self()]
    }

    base_state = %State{
      cluster: __MODULE__.TestCluster,
      epoch: 1,
      node_capabilities: node_capabilities,
      old_transaction_system_layout: %{
        logs: %{},
        storage_teams: []
      },
      config: %{
        coordinators: [],
        parameters: %{
          desired_logs: 2,
          desired_replication_factor: 3,
          desired_commit_proxies: 1
        },
        transaction_system_layout: %{
          logs: %{},
          storage_teams: [],
          services: %{}
        }
      },
      services: %{}
    }

    Map.merge(base_state, overrides)
  end

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"

    def otp_name(component) do
      case component do
        :sequencer -> :test_sequencer
        :foreman -> :test_foreman
        _ -> :"test_#{component}"
      end
    end
  end

  # Mock phases that return completed or stalled states
  defmodule MockStartPhase do
    @moduledoc false
    def execute(_recovery_attempt) do
      # Mock phase does nothing
      nil
    end
  end

  defmodule MockStalledPhase do
    @moduledoc false
    def execute(recovery_attempt) do
      {recovery_attempt, {:stalled, :test_reason}}
    end
  end

  describe "try_to_recover/1" do
    test "handles starting state by setting up initial recovery" do
      state = create_test_state()

      assert %State{
               recovery_attempt: %RecoveryAttempt{
                 cluster: TestCluster,
                 epoch: 1,
                 attempt: 1
               }
             } = Recovery.try_to_recover(state)
    end

    test "handles recovery state by setting up subsequent recovery" do
      existing_recovery_attempt = %RecoveryAttempt{
        cluster: TestCluster,
        epoch: 1,
        attempt: 1,
        started_at: 12_345
      }

      state = %State{
        cluster: TestCluster,
        epoch: 1,
        recovery_attempt: existing_recovery_attempt,
        config: %{
          coordinators: [],
          parameters: %{},
          transaction_system_layout: %{}
        },
        services: %{service1: %{status: :up}}
      }

      # Test just the setup function without full recovery
      assert %State{
               recovery_attempt: %RecoveryAttempt{attempt: 2}
             } = Recovery.setup_for_subsequent_recovery(state)
    end

    test "returns unchanged state for other states" do
      state = %State{state: :running}

      result = Recovery.try_to_recover(state)

      assert result == state
    end
  end

  describe "setup_for_initial_recovery/1" do
    test "resets transaction system layout components" do
      state = %State{
        cluster: TestCluster,
        epoch: 1,
        config: %{
          coordinators: [],
          parameters: %{
            desired_logs: 1,
            desired_replication_factor: 1,
            desired_commit_proxies: 1
          },
          transaction_system_layout: %{
            director: :old_director,
            sequencer: :old_sequencer,
            rate_keeper: :old_rate_keeper,
            proxies: [:old_proxy],
            resolvers: [:old_resolver],
            logs: %{old: :log}
          }
        },
        services: %{}
      }

      zero_version = Version.zero()

      assert %State{
               epoch: 1,
               cluster: TestCluster,
               recovery_attempt: %RecoveryAttempt{
                 attempt: 1,
                 cluster: TestCluster,
                 epoch: 1,
                 started_at: started_at,
                 required_services: %{},
                 locked_service_ids: locked_ids,
                 old_log_ids_to_copy: [],
                 version_vector: {^zero_version, ^zero_version},
                 durable_version: ^zero_version
               }
             } = Recovery.setup_for_initial_recovery(state)

      # Verify empty collection and timestamp
      assert MapSet.size(locked_ids) == 0
      assert %DateTime{} = started_at
    end
  end

  describe "setup_for_subsequent_recovery/1" do
    test "increments attempt counter and resets state" do
      recovery_attempt = %RecoveryAttempt{
        cluster: TestCluster,
        epoch: 1,
        attempt: 3,
        started_at: 12_345
      }

      state = %State{
        recovery_attempt: recovery_attempt,
        config: %{},
        services: %{new: :service, updated: :service}
      }

      assert %State{
               recovery_attempt: %RecoveryAttempt{
                 attempt: 4,
                 cluster: TestCluster,
                 epoch: 1,
                 started_at: 12_345
               }
             } = Recovery.setup_for_subsequent_recovery(state)
    end
  end

  describe "run_recovery_attempt/1" do
    test "processes recovery attempt and stalls with insufficient resources" do
      recovery_attempt = create_test_recovery_attempt()

      # Without sufficient nodes/services, recovery stalls with unable to meet log quorum
      assert {{:stalled, :unable_to_meet_log_quorum}, _} =
               Recovery.run_recovery_attempt(recovery_attempt, create_test_context())
    end

    test "captures warnings during recovery attempt" do
      recovery_attempt = create_test_recovery_attempt()

      capture_log([level: :warning], fn ->
        assert {{:stalled, :unable_to_meet_log_quorum}, _} =
                 Recovery.run_recovery_attempt(recovery_attempt, create_test_context())
      end)
    end
  end

  describe "Full recovery run" do
    test "stalls with insufficient nodes when only one node available" do
      recovery_attempt = create_first_time_recovery_attempt()
      context = create_test_context()

      assert {{:stalled, :unable_to_meet_log_quorum}, _stalled_attempt} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "recovery attempts without state field go through normal flow" do
      recovery_attempt = create_first_time_recovery_attempt()

      context = create_test_context()

      # With no state-based pre-handling, all attempts go through the normal recovery flow
      # This test now verifies that stateless recovery attempts work correctly
      assert {{:stalled, :unable_to_meet_log_quorum}, _returned_attempt} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "existing cluster stalls when sequencer fails to start" do
      recovery_attempt = create_existing_cluster_recovery_attempt()

      context =
        create_test_context(
          old_transaction_system_layout: %{
            logs: %{"existing_log_1" => [0, 100]},
            storage_teams: [%{tag: 0, key_range: {"", <<0xFF, 0xFF>>}, storage_ids: ["existing_storage_1"]}]
          }
        )

      # Recovery now goes directly from LogRecoveryPlanningPhase to LogRecruitmentPhase,
      # skipping VersionDeterminationPhase. Without full mocking, fails at sequencer start.
      assert {{:error, {:failed_to_start, :sequencer, _, _}}, _stalled_attempt} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "with multiple nodes and services but partial mocking stalls at log quorum" do
      recovery_attempt = create_first_time_recovery_attempt()

      context =
        create_test_context()
        |> with_multiple_nodes()
        |> with_available_log_services()
        |> with_available_storage_services()

      # Without service locking or worker creation mocks, fails at log quorum
      assert {{:stalled, :unable_to_meet_log_quorum}, _} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "first-time recovery with full mocking still stalls at log quorum" do
      # This test documents that even with full mocking, recovery stalls at log quorum
      # due to the test setup constraints
      recovery_attempt = create_first_time_recovery_attempt()
      context = create_full_recovery_context()

      # Even with full mocking, still stalls at log quorum in test environment
      assert {{:stalled, :unable_to_meet_log_quorum}, _} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "monitoring phase correctly handles new transaction_services format" do
      alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase

      context = %{
        monitor_fn: fn pid ->
          send(self(), {:monitored, pid})
          make_ref()
        end
      }

      # Should complete without errors
      recovery_attempt =
        recovery_attempt(%{
          transaction_system_layout: %{
            sequencer: spawn(fn -> :ok end),
            proxies: [spawn(fn -> :ok end), spawn(fn -> :ok end)],
            resolvers: [{"start", spawn(fn -> :ok end)}],
            services: %{
              "log_service_1" => %{status: {:up, spawn(fn -> :ok end)}, kind: :log},
              "log_service_2" => %{status: {:up, spawn(fn -> :ok end)}, kind: :log},
              "storage_service_1" => %{status: {:up, spawn(fn -> :ok end)}, kind: :storage},
              "storage_service_2" => %{status: {:up, spawn(fn -> :ok end)}, kind: :storage}
            }
          }
        })

      assert {_result, Bedrock.ControlPlane.Director.Recovery.PersistencePhase} =
               MonitoringPhase.execute(recovery_attempt, context)

      # Should monitor sequencer, proxies, resolvers, and logs (but not storage)
      # Expected: 1 sequencer + 2 proxies + 1 resolver + 2 logs = 6 processes
      monitored_pids = for _ <- 1..6, do: assert_receive({:monitored, pid} when is_pid(pid), 100)

      # Should not receive any more monitoring messages (no storage services)
      refute_receive {:monitored, _}, 50

      assert length(monitored_pids) == 6
    end

    test "coordinator service format works with recovery phases" do
      # Verifies coordinator service format compatibility with recovery phases
      recovery_attempt = first_time_recovery()

      # Coordinator-format services
      coordinator_services = %{
        "log_worker_1" => {:log, {:log_worker_1, :node1}},
        "log_worker_2" => {:log, {:log_worker_2, :node1}},
        "storage_worker_1" => {:storage, {:storage_worker_1, :node1}},
        "storage_worker_2" => {:storage, {:storage_worker_2, :node1}},
        "storage_worker_3" => {:storage, {:storage_worker_3, :node1}}
      }

      context = create_coordinator_format_context(coordinator_services)

      # Should stall with unable to meet log quorum in test environment
      assert {{:stalled, :unable_to_meet_log_quorum}, _} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "recovery with coordinator-format services handles existing cluster (regression test)" do
      # Validates coordinator services work with existing cluster recovery
      old_layout = %{
        logs: %{"existing_log_1" => [0, 100]},
        storage_teams: [%{tag: 0, key_range: {"", <<0xFF, 0xFF>>}, storage_ids: ["storage_1"]}]
      }

      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> with_log_recovery_info(%{})
        |> with_storage_recovery_info(%{})

      coordinator_services = %{
        "existing_log_1" => {:log, {:log_worker_existing_1, :node1}},
        "storage_1" => {:storage, {:storage_worker_1, :node1}}
      }

      context =
        create_coordinator_format_context(coordinator_services,
          old_transaction_system_layout: old_layout
        )

      # Recovery now goes directly from LogRecoveryPlanningPhase to LogRecruitmentPhase,
      # skipping VersionDeterminationPhase. Stalls at TopologyPhase validation.
      assert {{:stalled, {:recovery_system_failed, {:invalid_recovery_state, :no_resolvers}}}, stalled_attempt} =
               Recovery.run_recovery_attempt(recovery_attempt, context)

      # Verify service tracking was populated during recovery
      assert Map.has_key?(stalled_attempt.service_pids, "existing_log_1")
      assert Map.has_key?(stalled_attempt.transaction_services, "existing_log_1")
    end

    test "newer epoch exists returns error instead of stall" do
      # Create recovery attempt for existing cluster (so locking actually happens)
      recovery_attempt = create_existing_cluster_recovery_attempt()

      # Mock lock_service_fn to return newer_epoch_exists
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"existing_log_1" => [0, 100]},
            storage_teams: [%{tag: 0, key_range: {"", <<0xFF, 0xFF>>}, storage_ids: ["existing_storage_1"]}]
          }
        ]
        |> create_test_context()
        |> with_multiple_nodes()
        |> Map.put(:available_services, %{
          "existing_log_1" => {:log, {:log_worker_existing_1, :node1}},
          "existing_storage_1" => {:storage, {:storage_worker_1, :node1}}
        })
        |> Map.put(:lock_service_fn, fn _service, _epoch ->
          {:error, :newer_epoch_exists}
        end)

      # Should return error, not stall
      assert {{:error, :newer_epoch_exists}, _failed_attempt} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end
  end

  # Common recovery attempt creation with default test values
  defp create_test_recovery_attempt(overrides \\ %{}) do
    base = %{
      cluster: TestCluster,
      epoch: 1,
      attempt: 1,
      started_at: 12_345
    }

    recovery_attempt(Map.merge(base, overrides))
  end

  # Helper function to create a first-time recovery attempt
  defp create_first_time_recovery_attempt do
    first_time_recovery()
  end

  # Helper function to create an existing cluster recovery attempt
  defp create_existing_cluster_recovery_attempt do
    existing_cluster_recovery()
    |> with_log_recovery_info(%{
      "existing_log_1" => %{
        kind: :log,
        oldest_version: Version.zero(),
        last_version: Version.from_integer(100)
      },
      "existing_log_2" => %{
        kind: :log,
        oldest_version: Version.zero(),
        last_version: Version.from_integer(100)
      }
    })
    |> with_storage_recovery_info(%{
      "existing_storage_1" => %{
        kind: :storage,
        durable_version: Version.from_integer(95),
        oldest_durable_version: Version.zero()
      },
      "storage_worker_2" => %{
        kind: :storage,
        durable_version: Version.from_integer(95),
        oldest_durable_version: Version.zero()
      },
      "storage_worker_3" => %{
        kind: :storage,
        durable_version: Version.from_integer(95),
        oldest_durable_version: Version.zero()
      }
    })
  end

  # Common full test context setup for recovery tests
  defp create_full_recovery_context(overrides \\ []) do
    overrides
    |> create_test_context()
    |> with_multiple_nodes()
    |> with_available_log_services()
    |> with_available_storage_services()
    |> with_mocked_service_locking()
    |> with_complete_mocking()
  end

  # Helper for coordinator format context setup
  defp create_coordinator_format_context(services, overrides \\ []) do
    base_context =
      overrides
      |> create_test_context()
      |> with_multiple_nodes()
      |> Map.put(:available_services, services)
      |> with_mocked_service_locking_coordinator_format()
      |> with_complete_mocking()

    # Apply any additional overrides that weren't handled by create_test_context
    additional_overrides = Keyword.delete(overrides, :old_transaction_system_layout)

    Enum.reduce(additional_overrides, base_context, fn {key, value}, ctx ->
      Map.put(ctx, key, value)
    end)
  end

  # Extract common mocking patterns into a single helper
  defp with_complete_mocking(context) do
    context
    |> with_mocked_worker_creation()
    |> with_mocked_supervision()
    |> with_mocked_transactions()
    |> with_mocked_log_recovery()
    |> with_mocked_worker_management()
  end

  # Composable context modification functions
  defp with_multiple_nodes(context) do
    node_capabilities = %{
      log: [:node1@host, :node2@host, :node3@host],
      storage: [:node1@host, :node2@host, :node3@host],
      coordination: [:node1@host, :node2@host, :node3@host],
      resolution: [:node1@host, :node2@host, :node3@host]
    }

    context
    |> Map.put(:node_capabilities, node_capabilities)
    |> Map.put(:node_list_fn, fn -> [:node1@host, :node2@host, :node3@host] end)
  end

  defp with_available_log_services(context) do
    log_services = %{
      "log_worker_1" => {:log, {:log_worker_1, :node1}},
      "log_worker_2" => {:log, {:log_worker_2, :node1}}
    }

    Map.update(context, :available_services, log_services, &Map.merge(&1, log_services))
  end

  defp with_available_storage_services(context) do
    storage_services = %{
      "storage_worker_1" => {:storage, {:storage_worker_1, :node1}},
      "storage_worker_2" => {:storage, {:storage_worker_2, :node1}},
      "storage_worker_3" => {:storage, {:storage_worker_3, :node1}},
      "storage_worker_4" => {:storage, {:storage_worker_4, :node1}},
      "storage_worker_5" => {:storage, {:storage_worker_5, :node1}},
      "storage_worker_6" => {:storage, {:storage_worker_6, :node1}}
    }

    Map.update(context, :available_services, storage_services, &Map.merge(&1, storage_services))
  end

  defp with_mocked_service_locking(context) do
    with_mocked_service_locking_coordinator_format(context)
  end

  defp with_mocked_service_locking_coordinator_format(context) do
    # Mock that handles coordinator-format services: {kind, {otp_name, node}}
    lock_service_fn = fn service, _epoch ->
      pid = spawn(fn -> :ok end)

      case service do
        {kind, _location} ->
          {:ok, pid, create_mock_service_info(kind)}

        _ ->
          {:error, :invalid_service_format}
      end
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp create_mock_service_info(kind) do
    %{
      kind: kind,
      durable_version: Version.from_integer(95),
      oldest_version: Version.zero(),
      last_version: Version.from_integer(100)
    }
  end

  defp with_mocked_worker_creation(context) do
    create_worker_fn = fn _foreman_ref, worker_id, _kind, _opts ->
      {:ok, "#{worker_id}_ref"}
    end

    worker_info_fn = fn {worker_ref, _node}, _fields, _opts ->
      worker_id = String.replace(worker_ref, "_ref", "")

      {:ok,
       [
         id: worker_id,
         otp_name: String.to_atom(worker_id),
         kind: :log,
         pid: spawn(fn -> :ok end)
       ]}
    end

    context
    |> Map.put(:create_worker_fn, create_worker_fn)
    |> Map.put(:worker_info_fn, worker_info_fn)
  end

  defp with_mocked_supervision(context) do
    start_supervised_fn = fn _child_spec, _node ->
      {:ok,
       spawn(fn ->
         receive do
           {:"$gen_call", from, {:recover_from, _token, _logs, _first, _last}} ->
             GenServer.reply(from, :ok)

             receive do
               :stop -> :ok
             after
               5000 -> :ok
             end

           _ ->
             :ok
         after
           5000 -> :ok
         end
       end)}
    end

    Map.put(context, :start_supervised_fn, start_supervised_fn)
  end

  defp with_mocked_transactions(context) do
    commit_transaction_fn = fn _proxy, _transaction -> {:ok, 101} end
    unlock_commit_proxy_fn = fn _proxy, _lock_token, _layout -> :ok end
    unlock_storage_fn = fn _storage_pid, _durable_version, _layout -> :ok end

    context
    |> Map.put(:commit_transaction_fn, commit_transaction_fn)
    |> Map.put(:unlock_commit_proxy_fn, unlock_commit_proxy_fn)
    |> Map.put(:unlock_storage_fn, unlock_storage_fn)
  end

  defp with_mocked_log_recovery(context) do
    copy_log_data_fn = fn _new_log_id, _old_log_id, _first_version, _last_version, _service_pids ->
      {:ok, spawn(fn -> :ok end)}
    end

    Map.put(context, :copy_log_data_fn, copy_log_data_fn)
  end

  defp with_mocked_worker_management(context) do
    foreman_all_fn = fn _foreman_ref, _opts -> {:ok, []} end

    remove_workers_fn = fn _foreman_ref, worker_ids, _opts ->
      Map.new(worker_ids, &{&1, :ok})
    end

    monitor_fn = fn pid -> Process.monitor(pid) end

    context
    |> Map.put(:foreman_all_fn, foreman_all_fn)
    |> Map.put(:remove_workers_fn, remove_workers_fn)
    |> Map.put(:monitor_fn, monitor_fn)
  end
end
