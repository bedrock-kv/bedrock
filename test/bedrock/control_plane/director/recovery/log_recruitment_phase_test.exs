defmodule Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"
    def otp_name(:foreman), do: :test_foreman
  end

  # Helper to create basic test context with common configuration
  defp create_recovery_context(old_logs, available_services \\ %{}, opts \\ []) do
    [
      old_transaction_system_layout: %{
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

      assert {%{logs: logs}, Bedrock.ControlPlane.Director.Recovery.LogReplayPhase} =
               LogRecruitmentPhase.execute(recovery_attempt, context)

      assert %{{:log, 2} => _, {:log, 3} => _} = logs
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
