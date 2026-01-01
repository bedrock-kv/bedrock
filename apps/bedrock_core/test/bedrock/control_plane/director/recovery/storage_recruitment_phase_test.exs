defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase
  alias Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase

  # Mock cluster for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(_component), do: :test_otp_name
  end

  # Helper functions for common test setup
  defp default_lock_service_fn do
    fn _service, _epoch ->
      pid = spawn(fn -> :ok end)
      {:ok, pid, %{kind: :storage, durable_version: 0, oldest_durable_version: 0}}
    end
  end

  defp basic_available_services(storage_ids) do
    Map.new(storage_ids, fn id -> {id, {:storage, {String.to_atom(id), :node1}}} end)
  end

  defp basic_recovery_context(nodes: node_count, storage_ids: storage_ids) do
    recovery_context()
    |> with_node_tracking(nodes: node_count)
    |> Map.put(:old_transaction_system_layout, %{logs: %{}, storage_teams: []})
    |> Map.put(:available_services, basic_available_services(storage_ids))
    |> Map.put(:lock_service_fn, default_lock_service_fn())
  end

  describe "execute/1" do
    test "successfully fills storage team vacancies and advances state" do
      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          "storage_3" => %{},
          "storage_4" => %{}
        })

      # Use pattern matching to combine tuple destructuring and assertions
      assert {%{storage_teams: teams}, LogReplayPhase} =
               StorageRecruitmentPhase.execute(
                 recovery_attempt,
                 basic_recovery_context(
                   nodes: 3,
                   storage_ids: ["storage_1", "storage_2", "storage_3", "storage_4"]
                 )
               )

      assert length(teams) == 2
      # Verify vacancies have been filled
      assert Enum.all?(teams, fn team ->
               Enum.all?(team.storage_ids, fn id -> not match?({:vacancy, _}, id) end)
             end)
    end

    test "stalls recovery when insufficient storage workers available" do
      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          "storage_3" => %{}
        })

      # Use pattern matching to capture both result and error directly
      assert {_result, {:stalled, {:insufficient_nodes, 1, 0}}} =
               StorageRecruitmentPhase.execute(
                 recovery_attempt,
                 basic_recovery_context(
                   nodes: 0,
                   storage_ids: ["storage_1", "storage_2", "storage_3"]
                 )
               )
    end

    test "handles storage teams with no vacancies" do
      recovery_attempt =
        recovery_attempt()
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

      # Pattern match to verify teams remain unchanged and next phase is correct
      expected_teams = recovery_attempt.storage_teams

      assert {%{storage_teams: ^expected_teams}, LogReplayPhase} =
               StorageRecruitmentPhase.execute(
                 recovery_attempt,
                 basic_recovery_context(
                   nodes: 3,
                   storage_ids: ["storage_1", "storage_2", "storage_3", "storage_4"]
                 )
               )
    end

    test "handles empty storage teams" do
      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_storage_teams([])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{}
        })

      # Pattern match to verify empty teams remain empty
      assert {%{storage_teams: []}, LogReplayPhase} =
               StorageRecruitmentPhase.execute(
                 recovery_attempt,
                 basic_recovery_context(
                   nodes: 3,
                   storage_ids: ["storage_1", "storage_2", "storage_3", "storage_4"]
                 )
               )
    end

    test "collects storage server PIDs in service_pids" do
      mock_pid_1 = spawn(fn -> :timer.sleep(1000) end)
      mock_pid_2 = spawn(fn -> :timer.sleep(1000) end)

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_storage_teams([
          %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
          %{tag: "team_2", storage_ids: ["storage_2"]}
        ])
        |> with_storage_recovery_info(%{
          "storage_1" => %{},
          "storage_2" => %{},
          "storage_3" => %{}
        })
        |> with_transaction_services(%{})

      custom_lock_fn = fn service, _epoch ->
        {_kind, {otp_name, _node}} = service

        existing_pid =
          case otp_name do
            :storage_1 -> mock_pid_1
            :storage_2 -> mock_pid_2
            _ -> spawn(fn -> :ok end)
          end

        {:ok, existing_pid, %{kind: :storage, durable_version: 0, oldest_durable_version: 0}}
      end

      context =
        [nodes: 3, storage_ids: ["storage_1", "storage_2", "storage_3"]]
        |> basic_recovery_context()
        |> Map.put(:lock_service_fn, custom_lock_fn)

      # Pattern match result structure and verify service collection
      assert {%{transaction_services: services, storage_teams: teams}, LogReplayPhase} =
               StorageRecruitmentPhase.execute(recovery_attempt, context)

      # Pattern match specific service descriptors
      assert %{"storage_1" => %{status: {:up, ^mock_pid_1}}} = services
      assert %{"storage_2" => %{status: {:up, ^mock_pid_2}}} = services
      assert Map.has_key?(services, "storage_3")

      # Verify all assigned storage servers have service descriptors
      all_storage_ids =
        teams
        |> Enum.flat_map(& &1.storage_ids)
        |> Enum.reject(&match?({:vacancy, _}, &1))

      for storage_id <- all_storage_ids do
        assert %{status: {:up, pid}} = Map.get(services, storage_id)
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

      # Pattern match successful result and verify structure
      assert {:ok, updated_teams, []} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
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
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])
      available_nodes = []

      # Pattern match error directly
      assert {:error, {:insufficient_nodes, 1, 0}} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
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

      # Pattern match success and verify team structure
      assert {:ok, [%{storage_ids: storage_ids}], []} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
                 available_nodes
               )

      assert length(storage_ids) == 3
      assert Enum.all?(storage_ids, fn id -> not match?({:vacancy, _}, id) end)
    end

    test "handles teams with no vacancies" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
        %{tag: "team_2", storage_ids: ["storage_3", "storage_4"]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4", "storage_5"])
      available_nodes = []

      # Pattern match to verify teams remain unchanged
      assert {:ok, ^storage_teams, []} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
                 available_nodes
               )
    end

    test "handles empty storage teams" do
      assigned_storage_ids = MapSet.new([])
      all_storage_ids = MapSet.new(["storage_1", "storage_2"])
      available_nodes = []

      # Pattern match to verify empty teams remain empty
      assert {:ok, [], []} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 [],
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
                 available_nodes
               )
    end

    test "calculates vacancy count correctly" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}, {:vacancy, 1}]},
        %{tag: "team_2", storage_ids: [{:vacancy, 2}, "storage_2"]}
      ]

      assigned_storage_ids =
        MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}, {:vacancy, 2}])

      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3"])
      available_storage_ids = MapSet.difference(all_storage_ids, assigned_storage_ids)
      available_nodes = []

      # Pattern match error with correct vacancy calculation (3 vacancies, 1 available = need 2 more)
      assert {:error, {:insufficient_nodes, 2, 0}} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 available_storage_ids,
                 available_nodes
               )
    end

    test "avoids reassigning already assigned storage" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 0}]},
        %{tag: "team_2", storage_ids: ["storage_2", {:vacancy, 1}]}
      ]

      assigned_storage_ids = MapSet.new(["storage_1", "storage_2", {:vacancy, 0}, {:vacancy, 1}])
      all_storage_ids = MapSet.new(["storage_1", "storage_2", "storage_3", "storage_4"])
      available_nodes = []

      assert {:ok, updated_teams, []} =
               StorageRecruitmentPhase.fill_storage_team_vacancies(
                 storage_teams,
                 assigned_storage_ids,
                 MapSet.difference(all_storage_ids, assigned_storage_ids),
                 available_nodes
               )

      # Verify both vacancies filled with new storage (storage_3 and storage_4)
      new_assignments =
        updated_teams
        |> Enum.flat_map(& &1.storage_ids)
        |> Enum.filter(fn id -> id in ["storage_3", "storage_4"] end)

      assert length(new_assignments) == 2
    end
  end

  describe "vacancy?/1" do
    test "returns true for vacancy tuples" do
      assert StorageRecruitmentPhase.vacancy?({:vacancy, 0})
      assert StorageRecruitmentPhase.vacancy?({:vacancy, 42})
    end

    test "returns false for non-vacancy values" do
      refute StorageRecruitmentPhase.vacancy?("storage_1")
      refute StorageRecruitmentPhase.vacancy?(:storage)
      refute StorageRecruitmentPhase.vacancy?(123)
      refute StorageRecruitmentPhase.vacancy?(nil)
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

      # Pattern match expected result structure
      assert [
               %{tag: "team_1", storage_ids: ["storage_1", "storage_3"]},
               %{tag: "team_2", storage_ids: ["storage_4", "storage_2"]}
             ] =
               StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
                 storage_teams,
                 storage_id_for_vacancy
               )
    end

    test "leaves non-vacancy IDs unchanged" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_3"
      }

      # Pattern match to verify no changes
      assert ^storage_teams =
               StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
                 storage_teams,
                 storage_id_for_vacancy
               )
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

      # Pattern match expected structure
      assert [%{tag: "team_1", storage_ids: ["storage_1", "storage_2", "storage_3"]}] =
               StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
                 storage_teams,
                 storage_id_for_vacancy
               )
    end

    test "handles empty storage teams" do
      assert [] = StorageRecruitmentPhase.replace_vacancies_with_storage_ids([], %{})
    end

    test "handles partial replacement mapping" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}, {:vacancy, 1}]}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_1"
      }

      # Pattern match partial replacement result
      assert [%{tag: "team_1", storage_ids: ["storage_1", {:vacancy, 1}]}] =
               StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
                 storage_teams,
                 storage_id_for_vacancy
               )
    end

    test "preserves team structure and other fields" do
      storage_teams = [
        %{tag: "team_1", storage_ids: [{:vacancy, 0}], extra_field: "preserved"}
      ]

      storage_id_for_vacancy = %{
        {:vacancy, 0} => "storage_1"
      }

      # Pattern match to verify structure and field preservation
      assert [%{tag: "team_1", storage_ids: ["storage_1"], extra_field: "preserved"}] =
               StorageRecruitmentPhase.replace_vacancies_with_storage_ids(
                 storage_teams,
                 storage_id_for_vacancy
               )
    end
  end
end
