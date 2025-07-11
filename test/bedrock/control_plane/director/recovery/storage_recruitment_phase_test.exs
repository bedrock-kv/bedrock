defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase

  describe "execute/1" do
    test "successfully fills storage team vacancies and advances state" do
      recovery_attempt = %{
        state: :recruit_storage_to_fill_vacancies,
        storage_teams: [
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ],
        storage_recovery_info_by_id: %{
          "storage_1" => %{},
          "storage_2" => %{},
          # Available for vacancy
          "storage_3" => %{},
          # Available for vacancy
          "storage_4" => %{}
        }
      }

      result = StorageRecruitmentPhase.execute(recovery_attempt, %{node_tracking: nil})

      assert result.state == :replay_old_logs
      assert is_list(result.storage_teams)
      assert length(result.storage_teams) == 2

      # Verify vacancies have been filled
      assert Enum.all?(result.storage_teams, fn team ->
               Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
             end)
    end

    test "stalls recovery when insufficient storage workers available" do
      recovery_attempt = %{
        state: :recruit_storage_to_fill_vacancies,
        storage_teams: [
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ],
        storage_recovery_info_by_id: %{
          "storage_1" => %{},
          "storage_2" => %{},
          # Only 1 available, but need 2 vacancies filled
          "storage_3" => %{}
        }
      }

      result = StorageRecruitmentPhase.execute(recovery_attempt, %{node_tracking: nil})

      assert result.state == {:stalled, {:need_storage_workers, 1}}
    end

    test "handles storage teams with no vacancies" do
      recovery_attempt = %{
        state: :recruit_storage_to_fill_vacancies,
        storage_teams: [
          %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
          %{tag: "team_2", storage_ids: ["storage_3", "storage_4"]}
        ],
        storage_recovery_info_by_id: %{
          "storage_1" => %{},
          "storage_2" => %{},
          "storage_3" => %{},
          "storage_4" => %{}
        }
      }

      result = StorageRecruitmentPhase.execute(recovery_attempt, %{node_tracking: nil})

      assert result.state == :replay_old_logs
      assert result.storage_teams == recovery_attempt.storage_teams
    end

    test "handles empty storage teams" do
      recovery_attempt = %{
        state: :recruit_storage_to_fill_vacancies,
        storage_teams: [],
        storage_recovery_info_by_id: %{
          "storage_1" => %{},
          "storage_2" => %{}
        }
      }

      result = StorageRecruitmentPhase.execute(recovery_attempt, %{node_tracking: nil})

      assert result.state == :replay_old_logs
      assert result.storage_teams == []
    end
  end

  describe "fill_storage_team_vacancies/2" do
    test "successfully fills vacancies when enough storage available" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])

      {:ok, updated_teams} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)

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

      # Only 1 available
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])

      {:error, {:need_storage_workers, 1}} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)
    end

    test "handles teams with multiple vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}]}
      ]

      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])

      {:ok, updated_teams} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)

      team = List.first(updated_teams)
      assert length(team.storage_ids) == 3
      assert Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
    end

    test "handles teams with no vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
        %{tag: "team_2", storage_ids: ["storage_3", "storage_4"]}
      ]

      all_storage_ids =
        MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4", "storage_5"])

      {:ok, updated_teams} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)

      # No changes needed
      assert updated_teams == storage_teams
    end

    test "handles empty storage teams" do
      all_storage_ids = MapSet.new(["storage_1", "storage_2"])

      {:ok, updated_teams} =
        StorageRecruitmentPhase.fill_storage_team_vacancies([], all_storage_ids)

      assert updated_teams == []
    end

    test "calculates vacancy count correctly" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}, {:vacancy, 1}]},
        %{tag: "team_2", storage_ids: [{:vacancy, 2}, "storage_2"]}
      ]

      # 3 vacancies total, but only 1 candidate available  
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])

      {:error, {:need_storage_workers, 2}} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)
    end

    test "avoids reassigning already assigned storage" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      # storage_1 and storage_2 are already assigned, so only storage_3 and storage_4 available
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])

      {:ok, updated_teams} =
        StorageRecruitmentPhase.fill_storage_team_vacancies(storage_teams, all_storage_ids)

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
