defmodule Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase
  alias Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase

  describe "execute/1" do
    test "successfully creates vacancies for logs and storage teams" do
      recovery_attempt = recovery_attempt()

      context =
        recovery_context()
        |> with_old_layout(
          logs: %{{:log, 1} => ["tag_a"]},
          storage_teams: [%{tag: "storage_tag_1", storage_ids: ["storage_1"]}]
        )
        |> with_cluster_config(%{
          parameters: %{
            desired_logs: 3,
            desired_replication_factor: 2
          }
        })

      assert {%{logs: logs, storage_teams: storage_teams}, VersionDeterminationPhase} =
               VacancyCreationPhase.execute(recovery_attempt, context)

      assert is_map(logs) and map_size(logs) > 0
      assert is_list(storage_teams) and length(storage_teams) > 0
    end

    test "handles empty logs and storage teams" do
      recovery_attempt = recovery_attempt()

      context =
        recovery_context()
        |> with_old_layout(logs: %{}, storage_teams: [])
        |> with_cluster_config(%{
          parameters: %{
            desired_logs: 2,
            desired_replication_factor: 3
          }
        })

      assert {%{logs: %{}, storage_teams: []}, VersionDeterminationPhase} =
               VacancyCreationPhase.execute(recovery_attempt, context)
    end
  end

  describe "create_vacancies_for_logs/2" do
    test "creates correct number of log vacancies" do
      logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"]
      }

      # Creates 2 unique tag groups (tag_a, tag_b)
      # Each group gets desired_logs vacancies = 3
      # Total: 2 * 3 = 6 vacancies created
      assert {:ok, updated_logs, 6} = VacancyCreationPhase.create_vacancies_for_logs(logs, 3)
      assert map_size(updated_logs) == 6
      assert Enum.all?(Map.keys(updated_logs), fn {type, _} -> type == :vacancy end)
    end

    test "handles logs with same tags correctly" do
      logs = %{
        {:log, 1} => ["tag_a"],
        # Same tags as log 1
        {:log, 2} => ["tag_a"]
      }

      # Since both logs have the same tags, they create 1 unique group
      # The group gets desired_logs vacancies = 2
      # Total: 1 * 2 = 2 vacancies
      assert {:ok, updated_logs, 2} = VacancyCreationPhase.create_vacancies_for_logs(logs, 2)
      assert map_size(updated_logs) == 2
    end

    test "handles empty logs" do
      assert {:ok, %{}, 0} = VacancyCreationPhase.create_vacancies_for_logs(%{}, 3)
    end

    test "creates vacancies with correct tag structure" do
      logs = %{
        {:log, 1} => ["tag_a", "tag_b"]
      }

      # Should have 2 vacancies, each with the sorted tags ["tag_a", "tag_b"]
      assert {:ok, updated_logs, _} = VacancyCreationPhase.create_vacancies_for_logs(logs, 2)
      assert map_size(updated_logs) == 2
      assert Enum.all?(Map.values(updated_logs), &(&1 == ["tag_a", "tag_b"]))
    end
  end

  describe "create_vacancies_for_storage_teams/2" do
    test "handles complex storage team vacancy scenarios" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1"]}
      ]

      assert {:ok, updated_teams, n_vacancies} =
               VacancyCreationPhase.create_vacancies_for_storage_teams(storage_teams, 3)

      assert is_list(updated_teams) and is_integer(n_vacancies) and n_vacancies >= 0
    end

    test "returns original teams when no vacancies needed" do
      storage_teams = [
        # Already at desired replication
        %{tag: "tag_1", storage_ids: ["storage_1", "storage_2"]},
        # Already at desired replication
        %{tag: "tag_2", storage_ids: ["storage_3", "storage_4"]}
      ]

      assert {:ok, updated_teams, n_vacancies} =
               VacancyCreationPhase.create_vacancies_for_storage_teams(storage_teams, 2)

      assert is_integer(n_vacancies) and is_list(updated_teams) and length(updated_teams) == 2
    end

    test "handles empty storage teams" do
      assert {:ok, [], 0} = VacancyCreationPhase.create_vacancies_for_storage_teams([], 3)
    end

    test "correctly handles teams with excess members" do
      storage_teams = [
        # Has 3, only need 2
        %{tag: "tag_1", storage_ids: ["s1", "s2", "s3"]}
      ]

      # No vacancies needed - should be unchanged
      assert {:ok, ^storage_teams, 0} =
               VacancyCreationPhase.create_vacancies_for_storage_teams(storage_teams, 2)
    end
  end

  describe "tag_set_rosters_from_storage_teams/1" do
    test "groups storage IDs by their tag sets" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1", "storage_2"]},
        # storage_1 appears in both
        %{tag: "tag_2", storage_ids: ["storage_1", "storage_3"]},
        %{tag: "tag_3", storage_ids: ["storage_4"]}
      ]

      rosters = VacancyCreationPhase.tag_set_rosters_from_storage_teams(storage_teams)

      # storage_1 should be in tags ["tag_1", "tag_2"] -> sorted as ["tag_1", "tag_2"]
      # storage_2 should be in tags ["tag_1"]
      # storage_3 should be in tags ["tag_2"]
      # storage_4 should be in tags ["tag_3"]
      expected_keys = [["tag_1"], ["tag_1", "tag_2"], ["tag_2"], ["tag_3"]]
      assert is_map(rosters)
      assert Enum.all?(expected_keys, &Map.has_key?(rosters, &1))
    end

    test "handles empty storage teams" do
      assert %{} = VacancyCreationPhase.tag_set_rosters_from_storage_teams([])
    end

    test "handles single storage team" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1", "storage_2"]}
      ]

      assert %{["tag_1"] => ["storage_1", "storage_2"]} =
               VacancyCreationPhase.tag_set_rosters_from_storage_teams(storage_teams)
    end
  end

  describe "expand_rosters_and_add_vacancies/2" do
    test "processes rosters correctly" do
      rosters = %{
        ["tag_1"] => ["storage_1"],
        ["tag_2"] => ["storage_2", "storage_3"]
      }

      assert {expanded_rosters, n_vacancies} =
               VacancyCreationPhase.expand_rosters_and_add_vacancies(rosters, 2)

      assert is_map(expanded_rosters) and is_integer(n_vacancies) and n_vacancies >= 0
      assert Map.has_key?(expanded_rosters, ["tag_1"]) and Map.has_key?(expanded_rosters, ["tag_2"])
    end

    test "handles rosters that don't need vacancies" do
      rosters = %{
        # Has 3, only need 2
        ["tag_1"] => ["storage_1", "storage_2", "storage_3"]
      }

      assert {^rosters, 0} =
               VacancyCreationPhase.expand_rosters_and_add_vacancies(rosters, 2)
    end

    test "handles empty rosters" do
      assert {%{}, 0} = VacancyCreationPhase.expand_rosters_and_add_vacancies(%{}, 3)
    end

    test "generates vacancy IDs when needed" do
      rosters = %{
        ["tag_1"] => ["storage_1"],
        ["tag_2"] => ["storage_2"]
      }

      assert {expanded_rosters, 2} =
               VacancyCreationPhase.expand_rosters_and_add_vacancies(rosters, 3)

      # Check that when vacancies are needed, some are created in the rosters
      all_vacancies =
        expanded_rosters
        |> Map.values()
        |> List.flatten()
        |> Enum.filter(&match?({:vacancy, _}, &1))

      # With 2 existing storages and needing 3 total, we expect 4 vacancies total
      assert length(all_vacancies) == 4
    end
  end

  describe "apply_expanded_roster_to_storage_teams/2" do
    test "updates storage teams with expanded rosters" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1"]},
        %{tag: "tag_2", storage_ids: ["storage_2"]}
      ]

      expanded_rosters = %{
        ["tag_1"] => ["storage_1", {:vacancy, 0}],
        ["tag_2"] => ["storage_2", {:vacancy, 1}]
      }

      updated_teams =
        VacancyCreationPhase.apply_expanded_roster_to_storage_teams(
          storage_teams,
          expanded_rosters
        )

      assert [
               %{tag: "tag_1", storage_ids: [{:vacancy, 0}, "storage_1"]},
               %{tag: "tag_2", storage_ids: [{:vacancy, 1}, "storage_2"]}
             ] = Enum.sort_by(updated_teams, & &1.tag)
    end

    test "sorts storage IDs in expanded rosters" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1"]}
      ]

      expanded_rosters = %{
        # Unsorted
        ["tag_1"] => [{:vacancy, 0}, "storage_1"]
      }

      assert [
               %{tag: "tag_1", storage_ids: [{:vacancy, 0}, "storage_1"]}
             ] =
               VacancyCreationPhase.apply_expanded_roster_to_storage_teams(
                 storage_teams,
                 expanded_rosters
               )
    end

    test "handles teams with no roster updates" do
      storage_teams = [
        %{tag: "tag_1", storage_ids: ["storage_1", "storage_2"]}
      ]

      # No roster found in expanded_rosters
      assert [%{tag: "tag_1", storage_ids: nil}] =
               VacancyCreationPhase.apply_expanded_roster_to_storage_teams(
                 storage_teams,
                 %{}
               )
    end
  end
end
