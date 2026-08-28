defmodule Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"
    def otp_name(:foreman), do: :test_foreman
  end

  # Helper to create basic test context with common configuration
  defp create_recovery_context(old_logs, available_services \\ %{}, opts \\ []) do
    [
      prior_core_state: %{
        logs: old_logs
      }
    ]
    |> create_test_context()
    |> Map.merge(%{
      cluster_config: %{
        transaction_system_layout: %{logs: old_logs}
      },
      available_services: available_services
    })
    |> Map.merge(Map.new(opts))
  end

  describe "execute/1" do
    test "transitions to stalled state when insufficient nodes available" do
      recovery_attempt = %{
        cluster: TestCluster,
        logs: %{
          {:vacancy, 1} => %{},
          {:vacancy, 2} => %{},
          {:vacancy, 3} => %{}
        }
      }

      context = create_recovery_context(%{{:log, 1} => %{}, {:log, 2} => %{}})

      capture_log(fn ->
        assert {_result, {:stalled, {:insufficient_nodes, 3, _}}} =
                 LogRecruitmentPhase.execute(recovery_attempt, context)
      end)
    end

    test "proceeds to log replay when log vacancies are successfully filled" do
      recovery_attempt = %{
        cluster: TestCluster,
        epoch: 1,
        logs: %{
          {:vacancy, 1} => %{},
          {:vacancy, 2} => %{}
        }
      }

      available_services = %{
        {:log, 2} => {:log, {:log_2, :node1}},
        {:log, 3} => {:log, {:log_3, :node1}}
      }

      lock_service_fn = fn _service, _epoch ->
        pid = spawn(fn -> :ok end)
        {:ok, pid, %{kind: :log, oldest_version: 0, last_version: 1}}
      end

      context = create_recovery_context(%{{:log, 1} => %{}}, available_services, lock_service_fn: lock_service_fn)

      assert {%{logs: logs}, LogReplayPhase} =
               LogRecruitmentPhase.execute(recovery_attempt, context)

      assert %{{:log, 2} => _, {:log, 3} => _} = logs
    end

    test "a candidate that fails to lock (ghost registration) is replaced, not a stall" do
      # The directory can carry registrations from dead nodes — nothing on
      # a dead node can deregister itself. Recruitment must treat a failed
      # lock as 'this candidate is gone' and create a fresh worker instead
      # of wedging every recovery attempt forever.
      good_pid = spawn(fn -> Process.sleep(:infinity) end)
      replacement_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt = %{
        cluster: TestCluster,
        epoch: 3,
        logs: %{{:vacancy, 1} => %{}, {:vacancy, 2} => %{}},
        transaction_services: %{},
        service_pids: %{}
      }

      context =
        create_recovery_context(
          %{{:log, :old} => %{}},
          %{
            "ghost_log" => {:log, {:ghost_worker, :dead@nowhere}},
            "good_log" => {:log, {:good_worker, :node1}}
          },
          node_capabilities: %{log: [:node1@host]},
          create_worker_fn: fn _foreman, _id, :log, _opts -> {:ok, :replacement_ref} end,
          worker_info_fn: fn {_ref, _node}, _facts, _opts ->
            {:ok, %{id: "replacement", otp_name: :replacement_otp, kind: :log, pid: replacement_pid}}
          end,
          lock_service_fn: fn
            {:log, {:ghost_worker, _}}, _epoch ->
              {:error, :unavailable}

            {:log, {:good_worker, _}}, _epoch ->
              {:ok, good_pid, %{kind: :log, oldest_version: 0, last_version: 1}}

            {:log, {:replacement_otp, _}}, _epoch ->
              {:ok, replacement_pid, %{kind: :log, oldest_version: 0, last_version: 0}}
          end
        )

      log =
        capture_log(fn ->
          assert {%{logs: logs, transaction_services: services}, LogReplayPhase} =
                   LogRecruitmentPhase.execute(recovery_attempt, context)

          # The ghost id is gone from the layout; the good candidate and a
          # fresh replacement fill the two vacancies.
          refute Map.has_key?(logs, "ghost_log")
          assert Map.has_key?(logs, "good_log")
          assert map_size(logs) == 2

          refute Map.has_key?(services, "ghost_log")
          assert %{"good_log" => %{status: {:up, ^good_pid}}} = services
        end)

      assert log =~ "replaced log candidates that failed to lock"
    end

    test "lock failures are recorded on the attempt so later attempts exclude them" do
      good_pid = spawn(fn -> Process.sleep(:infinity) end)
      replacement_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt = %{
        cluster: TestCluster,
        epoch: 3,
        logs: %{{:vacancy, 1} => %{}, {:vacancy, 2} => %{}},
        transaction_services: %{},
        service_pids: %{},
        lock_failed_service_ids: MapSet.new(["earlier_ghost"])
      }

      context =
        create_recovery_context(
          %{{:log, :old} => %{}},
          %{
            "ghost_log" => {:log, {:ghost_worker, :dead@nowhere}},
            "good_log" => {:log, {:good_worker, :node1}}
          },
          node_capabilities: %{log: [:node1@host]},
          create_worker_fn: fn _foreman, _id, :log, _opts -> {:ok, :replacement_ref} end,
          worker_info_fn: fn {_ref, _node}, _facts, _opts ->
            {:ok, %{id: "replacement", otp_name: :replacement_otp, kind: :log, pid: replacement_pid}}
          end,
          lock_service_fn: fn
            {:log, {:ghost_worker, _}}, _epoch ->
              {:error, :unavailable}

            {:log, {:good_worker, _}}, _epoch ->
              {:ok, good_pid, %{kind: :log, oldest_version: 0, last_version: 1}}

            {:log, {:replacement_otp, _}}, _epoch ->
              {:ok, replacement_pid, %{kind: :log, oldest_version: 0, last_version: 0}}
          end
        )

      capture_log(fn ->
        assert {%{lock_failed_service_ids: remembered}, LogReplayPhase} =
                 LogRecruitmentPhase.execute(recovery_attempt, context)

        # The new failure joins what earlier attempts already learned
        assert MapSet.equal?(remembered, MapSet.new(["earlier_ghost", "ghost_log"]))
      end)
    end

    test "replacing a lock-failed candidate with no log-capable nodes stalls, never crashes" do
      # The capability view on a booting node can be momentarily empty
      # (registration timing). Replacement must stall honestly with
      # :insufficient_nodes — the next registration retriggers the attempt
      # — not divide by zero in round-robin assignment.
      recovery_attempt = %{
        cluster: TestCluster,
        epoch: 3,
        logs: %{{:vacancy, 1} => %{}},
        transaction_services: %{},
        service_pids: %{}
      }

      context =
        create_recovery_context(
          %{{:log, :old} => %{}},
          %{"ghost_log" => {:log, {:ghost_worker, :dead@nowhere}}},
          node_capabilities: %{log: []},
          lock_service_fn: fn {:log, {:ghost_worker, _}}, _epoch -> {:error, :unavailable} end
        )

      capture_log(fn ->
        assert {_attempt, {:stalled, {:insufficient_nodes, 1, 0}}} =
                 LogRecruitmentPhase.execute(recovery_attempt, context)
      end)
    end

    test "workers created this attempt complete recruitment in the SAME attempt" do
      # A just-created worker cannot be in the coordinator's directory yet
      # (advertisement is async) — recruitment must lock it via the ref it
      # already holds, never stall waiting to rediscover its own creation.
      recovery_attempt = %{
        cluster: TestCluster,
        epoch: 1,
        logs: %{{:vacancy, 1} => %{}},
        transaction_services: %{},
        service_pids: %{}
      }

      worker_pid = spawn(fn -> Process.sleep(:infinity) end)

      context =
        create_recovery_context(
          %{{:log, 1} => %{}},
          # No candidates: the vacancy forces worker creation
          %{},
          node_capabilities: %{log: [:node1@host]},
          create_worker_fn: fn _foreman, _id, :log, _opts -> {:ok, :new_log_ref} end,
          worker_info_fn: fn {_ref, node}, _facts, _opts ->
            {:ok, %{id: "new_log", otp_name: :new_log_otp, kind: :log, pid: worker_pid}}
          end,
          lock_service_fn: fn _service, _epoch ->
            {:ok, worker_pid, %{kind: :log, oldest_version: 0, last_version: 1}}
          end
        )

      assert {%{logs: logs, transaction_services: services}, LogReplayPhase} =
               LogRecruitmentPhase.execute(recovery_attempt, context)

      # The vacancy was filled by the new worker, and it is locked/tracked
      assert map_size(logs) == 1
      [new_id] = Map.keys(logs)
      assert %{^new_id => %{status: {:up, ^worker_pid}}} = services
    end
  end

  describe "fill_log_vacancies/4" do
    test "fills vacancies with existing workers when sufficient candidates available" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:vacancy, 2} => %{role: :log}
      }

      assigned_log_ids = MapSet.new([{:log, 1}])
      all_log_ids = MapSet.new([{:log, 1}, {:log, 2}, {:log, 3}])
      available_nodes = [:node1, :node2]

      assert {:ok, updated_logs, []} =
               LogRecruitmentPhase.fill_log_vacancies(
                 logs,
                 assigned_log_ids,
                 all_log_ids,
                 available_nodes
               )

      assert map_size(updated_logs) == 2
      refute Map.has_key?(updated_logs, {:vacancy, 1})
      refute Map.has_key?(updated_logs, {:vacancy, 2})
    end

    test "creates new workers when insufficient existing candidates" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:vacancy, 2} => %{role: :log},
        {:vacancy, 3} => %{role: :log}
      }

      assigned_log_ids = MapSet.new([{:log, 1}])
      all_log_ids = MapSet.new([{:log, 1}, {:log, 2}])
      available_nodes = [:node1, :node2]

      assert {:ok, updated_logs, new_worker_ids} =
               LogRecruitmentPhase.fill_log_vacancies(
                 logs,
                 assigned_log_ids,
                 all_log_ids,
                 available_nodes
               )

      assert length(new_worker_ids) == 2
      assert map_size(updated_logs) == 3
      # All vacancies should be replaced
      refute Map.has_key?(updated_logs, {:vacancy, 1})
      refute Map.has_key?(updated_logs, {:vacancy, 2})
      refute Map.has_key?(updated_logs, {:vacancy, 3})
    end

    test "returns error when insufficient nodes for new workers" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:vacancy, 2} => %{role: :log},
        {:vacancy, 3} => %{role: :log}
      }

      assigned_log_ids = MapSet.new([{:log, 1}])
      all_log_ids = MapSet.new([{:log, 1}, {:log, 2}])
      # Only 1 node, but need 2 new workers
      available_nodes = [:node1]

      assert {:error, {:insufficient_nodes, 2, 1}} =
               LogRecruitmentPhase.fill_log_vacancies(
                 logs,
                 assigned_log_ids,
                 all_log_ids,
                 available_nodes
               )
    end
  end

  describe "all_vacancies/1" do
    test "extracts all vacancy keys from logs map" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:log, 2} => %{role: :log},
        {:vacancy, 3} => %{role: :log},
        {:vacancy, 4} => %{role: :log}
      }

      assert vacancies = LogRecruitmentPhase.all_vacancies(logs)
      assert MapSet.new([{:vacancy, 1}, {:vacancy, 3}, {:vacancy, 4}]) == vacancies
    end

    test "returns empty set when no vacancies" do
      logs = %{
        {:log, 1} => %{role: :log},
        {:log, 2} => %{role: :log}
      }

      assert MapSet.new() == LogRecruitmentPhase.all_vacancies(logs)
    end
  end

  describe "replace_vacancies_with_log_ids/2" do
    test "replaces vacancy keys with log IDs" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:log, 2} => %{role: :log},
        {:vacancy, 3} => %{role: :log}
      }

      log_id_for_vacancy = %{
        {:vacancy, 1} => {:log, 100},
        {:vacancy, 3} => {:log, 200}
      }

      assert updated_logs =
               LogRecruitmentPhase.replace_vacancies_with_log_ids(logs, log_id_for_vacancy)

      assert %{{:log, 100} => _, {:log, 2} => _, {:log, 200} => _} = updated_logs
      refute Map.has_key?(updated_logs, {:vacancy, 1})
      refute Map.has_key?(updated_logs, {:vacancy, 3})
    end

    test "preserves original keys when no replacement provided" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:log, 2} => %{role: :log}
      }

      log_id_for_vacancy = %{}

      assert updated_logs =
               LogRecruitmentPhase.replace_vacancies_with_log_ids(logs, log_id_for_vacancy)

      assert %{{:vacancy, 1} => _, {:log, 2} => _} = updated_logs
    end
  end
end
