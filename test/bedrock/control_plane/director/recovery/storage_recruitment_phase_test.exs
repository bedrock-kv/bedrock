defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhaseTest do
  use ExUnit.Case, async: true
  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase

  # Mock cluster for testing
  defmodule TestCluster do
    def otp_name(_component), do: :test_otp_name
  end

  describe "execute/1" do
    test "successfully fills storage team vacancies and advances state" do
      recovery_attempt =
        recovery_attempt()
        |> with_state(:recruit_storage_to_fill_vacancies)
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          # Available for vacancy
          "storage_3" => %{},
          # Available for vacancy
          "storage_4" => %{}
        })

      result =
        StorageRecruitmentPhase.execute(
          recovery_attempt,
          recovery_context() |> with_node_tracking(nodes: 1)
        )

      assert result.state == :replay_old_logs
      assert is_list(result.storage_teams)
      assert length(result.storage_teams) == 2

      # Verify vacancies have been filled
      assert Enum.all?(result.storage_teams, fn team ->
               Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
             end)
    end

    test "stalls recovery when insufficient storage workers available" do
      recovery_attempt =
        recovery_attempt()
        |> with_state(:recruit_storage_to_fill_vacancies)
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          # Only 1 available, but need 2 vacancies filled
          "storage_3" => %{}
        })

      result =
        StorageRecruitmentPhase.execute(
          recovery_attempt,
          recovery_context() |> with_node_tracking(nodes: 1)
        )

      assert {:stalled, {:all_workers_failed, _failed_workers}} = result.state
    end

    test "handles storage teams with no vacancies" do
      recovery_attempt =
        recovery_attempt()
        |> with_state(:recruit_storage_to_fill_vacancies)
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
          %{tag: "team_2", storage_ids: ["storage_3", "storage_4"]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          "storage_3" => %{},
          "storage_4" => %{}
        })

      result =
        StorageRecruitmentPhase.execute(
          recovery_attempt,
          recovery_context() |> with_node_tracking(nodes: 1)
        )

      assert result.state == :replay_old_logs
      assert result.storage_teams == recovery_attempt.storage_teams
    end

    test "handles empty storage teams" do
      recovery_attempt =
        recovery_attempt()
        |> with_state(:recruit_storage_to_fill_vacancies)
        |> with_cluster(TestCluster)
        |> with_storage_teams([])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{}
        })

      result =
        StorageRecruitmentPhase.execute(
          recovery_attempt,
          recovery_context() |> with_node_tracking(nodes: 1)
        )

      assert result.state == :replay_old_logs
      assert result.storage_teams == []
    end

    test "collects storage server PIDs in service_pids" do
      mock_pid_1 = spawn(fn -> :timer.sleep(1000) end)
      mock_pid_2 = spawn(fn -> :timer.sleep(1000) end)

      recovery_attempt =
        recovery_attempt()
        |> with_state(:recruit_storage_to_fill_vacancies)
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2"]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          # Available for vacancy
          "storage_3" => %{}
        })
        |> with_transaction_services(%{})

      context = %{
        node_tracking: mock_node_tracking(),
        available_services: %{
          "storage_1" => %{status: {:up, mock_pid_1}},
          "storage_2" => %{status: {:up, mock_pid_2}},
          "storage_3" => %{status: {:up, spawn(fn -> :timer.sleep(1000) end)}}
        }
      }

      result = StorageRecruitmentPhase.execute(recovery_attempt, context)

      assert result.state == :replay_old_logs

      # Verify that storage services have been collected
      assert Map.has_key?(result, :transaction_services)
      assert %{status: {:up, ^mock_pid_1}} = Map.get(result.transaction_services, "storage_1")
      assert %{status: {:up, ^mock_pid_2}} = Map.get(result.transaction_services, "storage_2")
      # The one assigned to fill vacancy
      assert Map.has_key?(result.transaction_services, "storage_3")

      # Verify all assigned storage servers have service descriptors
      all_storage_ids =
        result.storage_teams
        |> Enum.flat_map(& &1.storage_ids)
        |> Enum.reject(&match?({:vacancy, _}, &1))

      for storage_id <- all_storage_ids do
        assert Map.has_key?(result.transaction_services, storage_id),
               "Missing service descriptor for storage ID: #{storage_id}"

        assert %{status: {:up, pid}} = result.transaction_services[storage_id]
        assert is_pid(pid)
      end
    end
  end

  describe "fill_storage_team_vacancies/4" do
    test "successfully fills vacancies when enough storage available" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}])
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])
      available_nodes = []

      {:ok, updated_teams, []} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )

      assert length(updated_teams) == 2

      # Verify no vacancies remain
      assert Enum.all?(updated_teams, fn team ->
               Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
             end)

      # Verify assigned storage IDs are from available candidates
      all_assigned =
        updated_teams
        |> Enum.flat_map(& &1.storage_ids)
        |> MapSet.new()

      assert MapSet.subset?(all_assigned, all_storage_ids)
    end

    test "returns error when insufficient storage workers" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}])
      # Only 1 available
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])
      available_nodes = []

      {:error, {:insufficient_nodes, 1, 0}} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )
    end

    test "handles teams with multiple vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}]}
      ]

      assigned_storage_ids = MapSet.new([{:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}])
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])
      available_nodes = []

      {:ok, updated_teams, []} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )

      team = List.first(updated_teams)
      assert length(team.storage_ids) == 3
      assert Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
    end

    test "handles teams with no vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
        %{tag: "team_2", storage_ids: ["storage_3", "storage_4"]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])

      all_storage_ids =
        MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4", "storage_5"])

      available_nodes = []

      {:ok, updated_teams, []} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )

      # No changes needed
      assert updated_teams == storage_teams
    end

    test "handles empty storage teams" do
      assigned_storage_ids = MapSet.new([])
      all_storage_ids = MapSet.new(["storage_1", "storage_2"])
      available_nodes = []

      {:ok, updated_teams, []} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          [],
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )

      assert updated_teams == []
    end

    test "calculates vacancy count correctly" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}, {:vacancy, 1}]},
        %{tag: "team_2", storage_ids: [{:vacancy, 2}, "storage_2"]}
      ]

      assigned_storage_ids =
        MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}])

      # 3 vacancies total, but only 1 candidate available  
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])
      available_nodes = []

      {:error, {:insufficient_nodes, 2, 0}} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )
    end

    test "avoids reassigning already assigned storage" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}])
      # storage_1 and storage_2 are already assigned, so only storage_3 and storage_4 available
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])
      available_nodes = []

      {:ok, updated_teams, []} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(
          storage_teams,
          assigned_storage_ids,
          all_storage_ids,
          available_nodes
        )

      # Find the newly assigned storage IDs
      new_assignments =
        updated_teams
        |> Enum.flat_map(& &1.storage_ids)
        |> Enum.filter(fn id -> id in ["storage_3", "storage_4"] end)

      # Both vacancies filled with new storage
      assert length(new_assignments) == 2
    end
  end

  describe "vacancy?/1" do
    test "returns true for vacancy tuples" do
      assert StorageRecruitmentPhase.vacancy?({:vacancy, 0}) == true
      assert StorageRecruitmentPhase.vacancy?({:vacancy, 42}) == true
    end

    test "returns false for non-vacancy values" do
      assert StorageRecruitmentPhase.vacancy?("storage_1") == false
      assert StorageRecruitmentPhase.vacancy?(:storage) == false
      assert StorageRecruitmentPhase.vacancy?(123) == false
      assert StorageRecruitmentPhase.vacancy?(nil) == false
    end
  end

  describe "replace_vacancies_with_storage_ids/2" do
    test "replaces vacancies with corresponding storage IDs" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: [{:vacancy, 1}, "storage_2"]}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_3",
        {:vacancy, 1} => "storage_4"
      }

      result =
        StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
          storage_teams,
          storage_id_for_vacancy
        )

      expected = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_3"]},
        %{tag: "team_2", storage_ids: ["storage_4", "storage_2"]}
      ]

      assert result == expected
    end

    test "leaves non-vacancy IDs unchanged" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_3"
      }

      result =
        StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
          storage_teams,
          storage_id_for_vacancy
        )

      # No changes
      assert result == storage_teams
    end

    test "handles teams with multiple vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}]}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_1",
        {:vacancy, 1} => "storage_2",
        {:vacancy, 2} => "storage_3"
      }

      result =
        StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
          storage_teams,
          storage_id_for_vacancy
        )

      expected = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2", "storage_3"]}
      ]

      assert result == expected
    end

    test "handles empty storage teams" do
      result = StorageRecruitmentPhase.replace_vacancies_with_storage_ids([], %{})
      assert result == []
    end

    test "handles partial replacement mapping" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}, {:vacancy, 1}]}
      ]

      # Only provide mapping for one vacancy
      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_1"
      }

      result =
        StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
          storage_teams,
          storage_id_for_vacancy
        )

      expected = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 1}]}
      ]

      assert result == expected
    end

    test "preserves team structure and other fields" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}], extra_field: "preserved"}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_1"
      }

      result =
        StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
          storage_teams,
          storage_id_for_vacancy
        )

      expected = [
        %{tag: "team_1", storage_ids: ["storage_1"], extra_field: "preserved"}
      ]

      assert result == expected
    end
  end
end
