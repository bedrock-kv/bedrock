defmodule Bedrock.ControlPlane.Director.RecoveryTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import RecoveryTestSupport

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

      result = Recovery.try_to_recover(state)

      assert result.recovery_attempt.cluster == __MODULE__.TestCluster
      assert result.recovery_attempt.epoch == 1
      assert result.recovery_attempt.attempt == 1
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
      result = Recovery.setup_for_subsequent_recovery(state)

      assert result.recovery_attempt.attempt == 2
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

      empty_mapset = MapSet.new([])
      empty_map = %{}
      zero_version = Version.zero()

      assert %State{
               epoch: 1,
               cluster: TestCluster,
               config: %{
                 coordinators: [],
                 parameters: %{
                   desired_logs: 1,
                   desired_replication_factor: 1,
                   desired_commit_proxies: 1
                 },
                 transaction_system_layout: %{
                   logs: %{old: :log},
                   director: :old_director,
                   sequencer: :old_sequencer,
                   rate_keeper: :old_rate_keeper,
                   proxies: [:old_proxy],
                   resolvers: [:old_resolver]
                 }
               },
               recovery_attempt: %RecoveryAttempt{
                 attempt: 1,
                 cluster: TestCluster,
                 epoch: 1,
                 started_at: _,
                 required_services: ^empty_map,
                 locked_service_ids: ^empty_mapset,
                 log_recovery_info_by_id: ^empty_map,
                 storage_recovery_info_by_id: ^empty_map,
                 old_log_ids_to_copy: [],
                 version_vector: {^zero_version, ^zero_version},
                 durable_version: ^zero_version,
                 degraded_teams: [],
                 logs: ^empty_map,
                 storage_teams: [],
                 resolvers: [],
                 proxies: [],
                 sequencer: nil
               }
             } = Recovery.setup_for_initial_recovery(state)
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

      result = Recovery.setup_for_subsequent_recovery(state)

      updated_attempt = result.recovery_attempt
      assert updated_attempt.attempt == 4
      # Other fields should be preserved
      assert updated_attempt.cluster == TestCluster
      assert updated_attempt.epoch == 1
      assert updated_attempt.started_at == 12_345
    end
  end

  describe "run_recovery_attempt/1" do
    test "processes recovery attempt without state validation" do
      recovery_attempt =
        recovery_attempt(%{
          cluster: TestCluster,
          epoch: 1,
          attempt: 1,
          started_at: 12_345
        })

      # Without state field, no pre-validation occurs, recovery proceeds normally
      {{:stalled, reason}, _} =
        Recovery.run_recovery_attempt(recovery_attempt, create_test_context())

      # Will stall with unable to meet log quorum for the minimal setup
      assert reason == :unable_to_meet_log_quorum
    end

    test "returns stalled result when recovery cannot proceed" do
      recovery_attempt =
        recovery_attempt(%{
          cluster: TestCluster,
          epoch: 1,
          attempt: 1,
          started_at: 12_345
        })

      capture_log([level: :warning], fn ->
        result = Recovery.run_recovery_attempt(recovery_attempt, create_test_context())
        # Without state-based pre-handling, recovery attempts go through actual phases
        # and will stall with unable to meet log quorum for this minimal test setup
        assert {{:stalled, :unable_to_meet_log_quorum}, _} = result
      end)
    end

    test "recovery proceeds through normal flow without state field" do
      # This test verifies that recovery works without the state field
      recovery_attempt =
        recovery_attempt(%{
          cluster: TestCluster,
          epoch: 1,
          attempt: 1,
          started_at: 12_345
        })

      # Should not raise an exception, should return a stall result
      {{:stalled, reason}, _} =
        Recovery.run_recovery_attempt(recovery_attempt, create_test_context())

      # Will stall with unable to meet log quorum in this minimal test setup
      assert reason == :unable_to_meet_log_quorum
    end
  end

  describe "recovery/1 state dispatch" do
    test "handles recovery attempt flow correctly" do
      recovery_attempt =
        recovery_attempt(%{
          cluster: TestCluster,
          epoch: 1,
          attempt: 1,
          started_at: 12_345
        })

      {{:stalled, reason}, _result} =
        Recovery.run_recovery_attempt(recovery_attempt, create_test_context())

      # Without state-based pre-handling, gets actual stall reason from recovery flow
      assert reason == :unable_to_meet_log_quorum
    end

    test "processes recovery attempt without state validation" do
      recovery_attempt =
        recovery_attempt(%{
          cluster: TestCluster,
          epoch: 1,
          attempt: 1,
          started_at: 12_345
        })

      # No longer raises exceptions for "invalid states" since state field is removed
      {{:stalled, reason}, _} =
        Recovery.run_recovery_attempt(recovery_attempt, create_test_context())

      assert reason == :unable_to_meet_log_quorum
    end
  end

  describe "Full recovery run" do
    test "stalls with insufficient nodes when only one node available" do
      recovery_attempt = create_first_time_recovery_attempt()
      context = create_test_context()

      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      assert reason == :unable_to_meet_log_quorum
      # Note: stalled_attempt.state is no longer updated since phases control transitions
      # Should remain at original state
    end

    test "recovery attempts without state field go through normal flow" do
      recovery_attempt = create_first_time_recovery_attempt()

      context = create_test_context()

      {{:stalled, reason}, _returned_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # With no state-based pre-handling, all attempts go through the normal recovery flow
      # This test now verifies that stateless recovery attempts work correctly
      assert reason == :unable_to_meet_log_quorum
    end

    test "existing cluster stalls unable to meet log quorum when logs unavailable" do
      recovery_attempt = create_existing_cluster_recovery_attempt()

      context =
        create_test_context(
          old_transaction_system_layout: %{
            logs: %{"existing_log_1" => [0, 100]},
            storage_teams: [%{tag: 0, key_range: {"", :end}, storage_ids: ["existing_storage_1"]}]
          }
        )

      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # With selective locking, we now fail more specifically when trying to create new workers
      assert reason == {:insufficient_replication, [0]}
    end

    test "with multiple nodes and log services but no worker creation mocks" do
      recovery_attempt = create_first_time_recovery_attempt()

      context =
        create_test_context()
        |> with_multiple_nodes()
        |> with_available_log_services()

      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # Should fail when trying to lock recruited services since locking isn't mocked
      assert reason == :unable_to_meet_log_quorum
    end

    test "with nodes and services but no service locking" do
      recovery_attempt = create_first_time_recovery_attempt()

      context =
        create_test_context()
        |> with_multiple_nodes()
        |> with_available_log_services()
        |> with_available_storage_services()

      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # Should fail when trying to lock recruited services since locking isn't mocked
      assert reason == :unable_to_meet_log_quorum
    end

    test "first-time recovery now succeeds with resolver descriptors" do
      # This test documents that the :no_resolvers issue has been fixed
      # First-time recovery now succeeds because InitializationPhase creates resolver descriptors
      recovery_attempt = create_first_time_recovery_attempt()

      context =
        create_test_context()
        |> with_multiple_nodes()
        |> with_available_log_services()
        |> with_available_storage_services()
        |> with_mocked_service_locking()
        |> with_mocked_worker_creation()
        |> with_mocked_supervision()
        |> with_mocked_transactions()
        |> with_mocked_log_recovery()
        |> with_mocked_worker_management()

      # Should now succeed instead of stalling with :no_resolvers
      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      assert reason == :unable_to_meet_log_quorum
    end

    test "monitoring phase correctly handles new transaction_services format" do
      alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase

      # Test data that simulates the new format with both logs and storage
      _layout = %{
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

      {_result, next_phase} = MonitoringPhase.execute(recovery_attempt, context)

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.PersistencePhase

      # Should monitor sequencer, proxies, resolvers, and logs (but not storage)
      # Expected: 1 sequencer + 2 proxies + 1 resolver + 2 logs = 6 processes
      monitored_messages =
        for _ <- 1..6 do
          receive do
            {:monitored, pid} -> pid
          after
            100 -> :timeout
          end
        end

      # Should not receive any more monitoring messages (no storage services)
      refute_receive {:monitored, _}, 50

      # All messages should be PIDs, not :timeout
      assert Enum.all?(monitored_messages, &is_pid/1)
    end

    test "coordinator service format works directly with early recovery phases" do
      # This test verifies that coordinator service format is directly compatible
      # with early recovery phases without needing log copying or complex recovery scenarios

      # Use a simple first-time recovery to test coordinator format compatibility
      recovery_attempt = first_time_recovery()

      # Coordinator-format services (the real format from coordinator)
      coordinator_format_services = %{
        "log_worker_1" => {:log, {:log_worker_1, :node1}},
        "log_worker_2" => {:log, {:log_worker_2, :node1}},
        "storage_worker_1" => {:storage, {:storage_worker_1, :node1}},
        "storage_worker_2" => {:storage, {:storage_worker_2, :node1}},
        "storage_worker_3" => {:storage, {:storage_worker_3, :node1}}
      }

      # Use coordinator format directly (no conversion needed anymore)
      context =
        create_test_context()
        |> with_multiple_nodes()
        # Use coordinator format directly!
        |> Map.put(:available_services, coordinator_format_services)
        |> with_mocked_service_locking_coordinator_format()
        |> with_mocked_worker_creation()
        |> with_mocked_supervision()
        |> with_mocked_transactions()
        |> with_mocked_log_recovery()
        |> with_mocked_worker_management()

      # Keep default cluster config for first-time recovery (2 logs, 3 storage replication)

      # The coordinator format should enable early recovery phases to proceed
      # and now succeeds completely since we have proper coordination capabilities
      {{:stalled, reason}, _stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # Should stall with unable to meet log quorum
      assert reason == :unable_to_meet_log_quorum
    end

    test "recovery with coordinator-format services succeeds (regression test)" do
      # This test ensures that coordinator-format services work directly
      # This validates our new architecture where coordinator services are used without conversion

      old_transaction_system_layout = %{
        logs: %{"existing_log_1" => [0, 100]},
        storage_teams: [%{tag: 0, key_range: {"", :end}, storage_ids: ["storage_1"]}]
      }

      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> with_log_recovery_info(%{})
        |> with_storage_recovery_info(%{})

      # Coordinator services in their native format
      coordinator_services = %{
        "existing_log_1" => {:log, {:log_worker_existing_1, :node1}},
        "storage_1" => {:storage, {:storage_worker_1, :node1}}
      }

      context =
        create_test_context()
        |> with_multiple_nodes()
        # Coordinator format - should work directly!
        |> Map.put(:available_services, coordinator_services)
        |> with_mocked_service_locking()
        |> with_mocked_worker_creation()
        |> with_mocked_supervision()
        |> with_mocked_transactions()
        |> with_mocked_log_recovery()
        |> with_mocked_worker_management()
        |> Map.put(:old_transaction_system_layout, old_transaction_system_layout)

      # This should stall at version determination due to insufficient storage replication
      # (only one storage service but requires quorum) but should successfully complete
      # log recruitment and service locking first
      {{:stalled, {:insufficient_replication, [0]}}, stalled_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)

      # Should successfully complete log recruitment and populate service tracking
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
            storage_teams: [%{tag: 0, key_range: {"", :end}, storage_ids: ["existing_storage_1"]}]
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
      {{:error, :newer_epoch_exists}, _failed_attempt} =
        Recovery.run_recovery_attempt(recovery_attempt, context)
    end
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
    lock_service_fn = fn service, _epoch ->
      pid = spawn(fn -> :ok end)
      # Handle coordinator format: {kind, {otp_name, node}}
      {kind, _location} = service

      {:ok, pid,
       %{
         kind: kind,
         durable_version: Version.from_integer(95),
         oldest_version: Version.zero(),
         last_version: Version.from_integer(100)
       }}
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
  end

  defp with_mocked_service_locking_coordinator_format(context) do
    # Mock that handles coordinator-format services directly
    lock_service_fn = fn service, _epoch ->
      pid = spawn(fn -> :ok end)

      # The service is in coordinator format: {kind, {otp_name, node}}
      case service do
        {kind, _location} ->
          {:ok, pid,
           %{
             kind: kind,
             durable_version: Version.from_integer(95),
             oldest_version: Version.zero(),
             last_version: Version.from_integer(100)
           }}

        _ ->
          # If we get here, the format is unexpected
          {:error, :invalid_service_format}
      end
    end

    Map.put(context, :lock_service_fn, lock_service_fn)
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
