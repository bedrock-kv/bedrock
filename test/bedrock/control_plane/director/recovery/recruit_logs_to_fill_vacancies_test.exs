defmodule Bedrock.ControlPlane.Director.Recovery.RecruitLogsToFillVacanciesPhaseTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.RecruitLogsToFillVacanciesPhase

  import RecoveryTestSupport

  # Mock cluster module for testing
  defmodule TestCluster do
    def name(), do: "test_cluster"
    def otp_name(:foreman), do: :test_foreman
  end

  describe "execute/1" do
    test "transitions to stalled state when insufficient nodes available" do
      recovery_attempt = %{
        state: :recruit_logs_to_fill_vacancies,
        cluster: TestCluster,
        logs: %{
          {:vacancy, 1} => %{},
          {:vacancy, 2} => %{},
          {:vacancy, 3} => %{}
        }
      }

      context =
        create_test_context()
        |> Map.merge(%{
          cluster_config: %{
            transaction_system_layout: %{logs: %{{:log, 1} => %{}, {:log, 2} => %{}}}
          },
          available_services: %{}
        })

      capture_log(fn ->
        result =
          RecruitLogsToFillVacanciesPhase.execute(
            recovery_attempt,
            context
          )

        assert {:stalled, {:insufficient_nodes, 3, _}} = result.state
      end)
    end

    test "transitions to recruit_storage_to_fill_vacancies when vacancies filled with existing workers" do
      recovery_attempt = %{
        state: :recruit_logs_to_fill_vacancies,
        cluster: TestCluster,
        logs: %{
          {:vacancy, 1} => %{},
          {:vacancy, 2} => %{}
        }
      }

      context =
        create_test_context()
        |> Map.merge(%{
          cluster_config: %{
            transaction_system_layout: %{logs: %{{:log, 1} => %{}}}
          },
          available_services: %{
            {:log, 2} => %{kind: :log},
            {:log, 3} => %{kind: :log}
          }
        })

      result = RecruitLogsToFillVacanciesPhase.execute(recovery_attempt, context)

      assert result.state == :recruit_storage_to_fill_vacancies
      assert Map.has_key?(result.logs, {:log, 2})
      assert Map.has_key?(result.logs, {:log, 3})
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

      {:ok, updated_logs, new_worker_ids} =
        RecruitLogsToFillVacanciesPhase.fill_log_vacancies(
          logs,
          assigned_log_ids,
          all_log_ids,
          available_nodes
        )

      assert new_worker_ids == []
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

      {:ok, updated_logs, new_worker_ids} =
        RecruitLogsToFillVacanciesPhase.fill_log_vacancies(
          logs,
          assigned_log_ids,
          all_log_ids,
          available_nodes
        )

      assert length(new_worker_ids) == 2
      assert map_size(updated_logs) == 3
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

      {:error, {:insufficient_nodes, 2, 1}} =
        RecruitLogsToFillVacanciesPhase.fill_log_vacancies(
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

      vacancies = RecruitLogsToFillVacanciesPhase.all_vacancies(logs)

      assert MapSet.size(vacancies) == 3
      assert MapSet.member?(vacancies, {:vacancy, 1})
      assert MapSet.member?(vacancies, {:vacancy, 3})
      assert MapSet.member?(vacancies, {:vacancy, 4})
      refute MapSet.member?(vacancies, {:log, 2})
    end

    test "returns empty set when no vacancies" do
      logs = %{
        {:log, 1} => %{role: :log},
        {:log, 2} => %{role: :log}
      }

      vacancies = RecruitLogsToFillVacanciesPhase.all_vacancies(logs)

      assert MapSet.size(vacancies) == 0
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

      updated_logs =
        RecruitLogsToFillVacanciesPhase.replace_vacancies_with_log_ids(logs, log_id_for_vacancy)

      assert Map.has_key?(updated_logs, {:log, 100})
      assert Map.has_key?(updated_logs, {:log, 2})
      assert Map.has_key?(updated_logs, {:log, 200})
      refute Map.has_key?(updated_logs, {:vacancy, 1})
      refute Map.has_key?(updated_logs, {:vacancy, 3})
    end

    test "preserves original keys when no replacement provided" do
      logs = %{
        {:vacancy, 1} => %{role: :log},
        {:log, 2} => %{role: :log}
      }

      log_id_for_vacancy = %{}

      updated_logs =
        RecruitLogsToFillVacanciesPhase.replace_vacancies_with_log_ids(logs, log_id_for_vacancy)

      assert Map.has_key?(updated_logs, {:vacancy, 1})
      assert Map.has_key?(updated_logs, {:log, 2})
    end
  end

  describe "integration with worker creation" do
    test "handles insufficient nodes error" do
      recovery_attempt = %{
        state: :recruit_logs_to_fill_vacancies,
        cluster: TestCluster,
        logs: %{
          {:vacancy, 1} => %{},
          {:vacancy, 2} => %{}
        }
      }

      context =
        create_test_context()
        |> Map.merge(%{
          available_services: %{},
          cluster_config: %{
            transaction_system_layout: %{logs: %{{:log, 1} => %{}}}
          }
        })

      result = RecruitLogsToFillVacanciesPhase.execute(recovery_attempt, context)

      # Should transition to stalled state when insufficient nodes available
      assert {:stalled, {:insufficient_nodes, 2, _}} = result.state
    end
  end
end
