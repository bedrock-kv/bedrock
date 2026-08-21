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
        logs: %{}
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

  describe "ghost_directory_ids/2" do
    test "selects exactly the directory entries the completed recovery does not reference" do
      live_mat_pid = spawn(fn -> Process.sleep(:infinity) end)

      services = %{
        "live_log" => {:log, {:a, :node1}},
        "live_mat" => {:materializer, {:b, :node1}},
        "ghost" => {:log, {:c, :dead@nowhere}}
      }

      completed = %{
        logs: %{"live_log" => []},
        shard_materializers: %{0 => {"live_mat", node(), live_mat_pid}},
        transaction_services: %{
          "live_log" => %{kind: :log, status: {:up, self()}},
          "live_mat" => %{kind: :materializer, status: {:up, live_mat_pid}}
        }
      }

      assert Recovery.ghost_directory_ids(services, completed) == ["ghost"]
    end

    test "a worker created this attempt (not yet in the directory) is referenced — never pruned" do
      # Attempt-created workers reach transaction_services at creation;
      # advertisement to the coordinator directory is async and may lag.
      # The reference set is computed from the attempt alone, so the lag
      # can never deregister a worker the epoch references.
      services = %{"old_log" => {:log, {:a, :node1}}}

      completed = %{
        logs: %{"old_log" => [], "brand_new_log" => []},
        shard_materializers: %{},
        transaction_services: %{
          "old_log" => %{kind: :log, status: {:up, self()}},
          "brand_new_log" => %{kind: :log, status: {:up, self()}}
        }
      }

      assert Recovery.ghost_directory_ids(services, completed) == []
    end

    test "a locked-but-inactive materializer is not referenced — it is a ghost candidate" do
      active_pid = spawn(fn -> Process.sleep(:infinity) end)
      inactive_pid = spawn(fn -> Process.sleep(:infinity) end)

      services = %{
        "active_mat" => {:materializer, {:a, :node1}},
        "inactive_mat" => {:materializer, {:b, :node1}}
      }

      completed = %{
        logs: %{},
        shard_materializers: %{0 => {"active_mat", node(), active_pid}},
        transaction_services: %{
          "active_mat" => %{kind: :materializer, status: {:up, active_pid}},
          "inactive_mat" => %{kind: :materializer, status: {:up, inactive_pid}}
        }
      }

      assert Recovery.ghost_directory_ids(services, completed) == ["inactive_mat"]
    end
  end

  defmodule StubCoordinator do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call(:fetch_service_directory, _from, s), do: {:reply, {:ok, %{}}, s}

    @impl true
    def handle_cast(_msg, s), do: {:noreply, s}
  end

  defmodule StubMaterializer do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:lock_for_recovery, _epoch}, _from, s) do
      info = %{
        kind: :materializer,
        durable_version: Version.zero(),
        oldest_durable_version: Version.zero()
      }

      {:reply, {:ok, self(), info}, s}
    end
  end

  describe "do_recovery/1 retains the stalled attempt" do
    test "a stall leaves the live state and the persisted config holding the same mutated attempt" do
      {:ok, coordinator} = StubCoordinator.start_link()
      {:ok, materializer} = StubMaterializer.start_link()

      state =
        %{
          coordinator: coordinator,
          lock_token: "test-lock-token",
          # An advertised materializer: the locking phase locks it and
          # records it into the attempt — a genuine phase mutation that
          # must survive the later stall.
          services: %{"mat-1" => {:materializer, materializer}}
        }
        |> create_test_state()
        |> Recovery.setup_for_initial_recovery()

      initial_attempt = state.recovery_attempt

      result = capture_log_and_return(fn -> Recovery.do_recovery(state) end)

      # The pipeline stalled (no log-capable services to recruit from), so
      # the director stays in recovery with the attempt persisted…
      assert result.state == :recovery
      stalled_attempt = result.config.recovery_attempt
      assert %RecoveryAttempt{} = stalled_attempt

      # …and the LIVE attempt must be that same stalled attempt. Anything
      # else discards the phases' accumulated observations (lock-failed
      # ids, recruited services) on the next in-process retry.
      assert result.recovery_attempt == stalled_attempt

      # The phases genuinely mutated the attempt before stalling; adopting
      # the stalled attempt is not a no-op. The locked materializer is the
      # observable mutation.
      refute stalled_attempt == initial_attempt
      assert MapSet.member?(stalled_attempt.locked_service_ids, "mat-1")
    end

    defp capture_log_and_return(fun) do
      holder = self()
      capture_log(fn -> send(holder, {:result, fun.()}) end)

      receive do
        {:result, result} -> result
      end
    end
  end

  describe "setup_for_subsequent_recovery/1" do
    test "the retry increments the retained attempt and preserves cross-attempt memory" do
      remembered = MapSet.new(["ghost-log-1", "ghost-log-2"])

      state = %State{
        recovery_attempt: %RecoveryAttempt{
          cluster: TestCluster,
          epoch: 1,
          attempt: 3,
          started_at: 12_345,
          lock_failed_service_ids: remembered
        },
        config: %{},
        services: %{}
      }

      assert %State{recovery_attempt: retried} = Recovery.setup_for_subsequent_recovery(state)
      assert retried.attempt == 4
      assert retried.lock_failed_service_ids == remembered
    end

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

    test "existing cluster stalls when log capacity is insufficient for recruitment" do
      recovery_attempt = create_existing_cluster_recovery_attempt()

      context =
        create_test_context(
          old_transaction_system_layout: %{
            logs: %{"existing_log_1" => [0, 100]}
          }
        )

      # Logs are generational: an existing-cluster recovery recruits a
      # fresh set (survivors are copy sources only). With no recruitable
      # candidates and one node for two desired logs, recruitment stalls.
      assert {{:stalled, {:insufficient_nodes, _needed, _available}}, _stalled_attempt} =
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
          sequencer: spawn(fn -> :ok end),
          proxies: [spawn(fn -> :ok end), spawn(fn -> :ok end)],
          resolvers: [{"start", spawn(fn -> :ok end)}],
          logs: %{"log_service_1" => [], "log_service_2" => []},
          transaction_services: %{
            "log_service_1" => %{status: {:up, spawn(fn -> :ok end)}, kind: :log},
            "log_service_2" => %{status: {:up, spawn(fn -> :ok end)}, kind: :log},
            "storage_service_1" => %{status: {:up, spawn(fn -> :ok end)}, kind: :materializer},
            "storage_service_2" => %{status: {:up, spawn(fn -> :ok end)}, kind: :materializer}
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
        "storage_worker_1" => {:materializer, {:storage_worker_1, :node1}},
        "storage_worker_2" => {:materializer, {:storage_worker_2, :node1}},
        "storage_worker_3" => {:materializer, {:storage_worker_3, :node1}}
      }

      context = create_coordinator_format_context(coordinator_services)

      # Should stall with unable to meet log quorum in test environment
      assert {{:stalled, :unable_to_meet_log_quorum}, _} =
               Recovery.run_recovery_attempt(recovery_attempt, context)
    end

    test "recovery with coordinator-format services handles existing cluster (regression test)" do
      # Validates coordinator services work with existing cluster recovery
      old_layout = %{
        logs: %{"existing_log_1" => [0, 100]}
      }

      durable_version = Version.from_integer(100)

      recovery_attempt =
        existing_cluster_recovery()
        |> Map.put(:epoch, 2)
        |> Map.put(:durable_version, durable_version)
        |> with_log_recovery_info(%{})
        |> with_storage_recovery_info(%{})

      # Include materializer so MaterializerBootstrapPhase passes
      materializer_pid = spawn(fn -> Process.sleep(5000) end)

      # Two candidate logs so generational recruitment can fill its
      # vacancies without creating workers (survivor is a copy source).
      coordinator_services = %{
        "existing_log_1" => {:log, {:log_worker_existing_1, :node1}},
        "candidate_log_a" => {:log, {:log_worker_candidate_a, :node1}},
        "candidate_log_b" => {:log, {:log_worker_candidate_b, :node1}},
        "storage_1" => {:materializer, {:storage_worker_1, :node1}},
        "metadata_materializer" => {:materializer, {:materializer, :node1}}
      }

      context =
        coordinator_services
        |> create_coordinator_format_context(old_transaction_system_layout: old_layout)
        |> Map.update!(
          :available_services,
          &Map.put(&1, "metadata_materializer", {:materializer, {:materializer, :node1}})
        )
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:current_version] ->
          {:ok, %{current_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      # With materializer reuse, generational log recruitment, and
      # resolver seeding, an existing-cluster recovery now runs to
      # completion.
      log =
        capture_log(fn ->
          assert {:ok, completed_attempt} =
                   Recovery.run_recovery_attempt(recovery_attempt, context)

          # Verify service tracking was populated during recovery
          assert Map.has_key?(completed_attempt.service_pids, "existing_log_1")
          assert Map.has_key?(completed_attempt.transaction_services, "existing_log_1")
        end)

      # Materializer bootstrap phase should log catchup completion
      assert log =~ "Materializer caught up to version"
    end

    test "newer epoch exists returns error instead of stall" do
      # Create recovery attempt for existing cluster (so locking actually happens)
      recovery_attempt = create_existing_cluster_recovery_attempt()

      # Mock lock_service_fn to return newer_epoch_exists
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"existing_log_1" => [0, 100]}
          }
        ]
        |> create_test_context()
        |> with_multiple_nodes()
        |> Map.put(:available_services, %{
          "existing_log_1" => {:log, {:log_worker_existing_1, :node1}},
          "existing_storage_1" => {:materializer, {:storage_worker_1, :node1}}
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
        available_after: Version.zero(),
        oldest_version: Version.zero(),
        last_version: Version.from_integer(100)
      },
      "existing_log_2" => %{
        kind: :log,
        available_after: Version.zero(),
        oldest_version: Version.zero(),
        last_version: Version.from_integer(100)
      }
    })
    |> with_storage_recovery_info(%{
      "existing_storage_1" => %{
        kind: :materializer,
        durable_version: Version.from_integer(95),
        oldest_durable_version: Version.zero()
      },
      "storage_worker_2" => %{
        kind: :materializer,
        durable_version: Version.from_integer(95),
        oldest_durable_version: Version.zero()
      },
      "storage_worker_3" => %{
        kind: :materializer,
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
      materializer: [:node1@host, :node2@host, :node3@host],
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
      "storage_worker_1" => {:materializer, {:storage_worker_1, :node1}},
      "storage_worker_2" => {:materializer, {:storage_worker_2, :node1}},
      "storage_worker_3" => {:materializer, {:storage_worker_3, :node1}},
      "storage_worker_4" => {:materializer, {:storage_worker_4, :node1}},
      "storage_worker_5" => {:materializer, {:storage_worker_5, :node1}},
      "storage_worker_6" => {:materializer, {:storage_worker_6, :node1}}
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
    base = %{
      kind: kind,
      durable_version: Version.from_integer(95),
      available_after: Version.zero(),
      oldest_version: Version.zero(),
      last_version: Version.from_integer(100)
    }

    # Materializers report their shard assignment when locked (the
    # bootstrap phase reuses them by shard).
    if kind == :materializer, do: Map.put(base, :shard_id, 0), else: base
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
    commit_transaction_fn = fn _proxy, _epoch, _transaction -> {:ok, 101, 1} end
    unlock_commit_proxy_fn = fn _proxy, _lock_token, _sequencer, _resolver_layout, _routing_data -> :ok end
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
