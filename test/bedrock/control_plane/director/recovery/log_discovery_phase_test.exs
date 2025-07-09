defmodule Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhase

  describe "execute/1" do
    test "successfully determines logs to copy and advances state" do
      recovery_attempt = %{
        state: :determine_old_logs_to_copy,
        last_transaction_system_layout: %{
          logs: %{
            {:log, 1} => ["tag_a"],
            {:log, 2} => ["tag_b"]
          }
        },
        log_recovery_info_by_id: %{
          {:log, 1} => %{oldest_version: 10, last_version: 50},
          {:log, 2} => %{oldest_version: 5, last_version: 45}
        },
        # Quorum = 2
        parameters: %{desired_logs: 3}
      }

      result = LogDiscoveryPhase.execute(recovery_attempt)

      assert result.state == :create_vacancies
      assert is_list(result.old_log_ids_to_copy)
      assert is_tuple(result.version_vector)
      assert length(result.old_log_ids_to_copy) > 0
    end

    test "stalls recovery when unable to meet log quorum" do
      recovery_attempt = %{
        state: :determine_old_logs_to_copy,
        last_transaction_system_layout: %{
          # No logs available
          logs: %{}
        },
        log_recovery_info_by_id: %{},
        # Quorum = 2, but no logs
        parameters: %{desired_logs: 3}
      }

      result = LogDiscoveryPhase.execute(recovery_attempt)

      assert result.state == {:stalled, :unable_to_meet_log_quorum}
    end

    test "handles single log with quorum of 1" do
      recovery_attempt = %{
        state: :determine_old_logs_to_copy,
        last_transaction_system_layout: %{
          logs: %{
            {:log, 1} => ["tag_a"]
          }
        },
        log_recovery_info_by_id: %{
          {:log, 1} => %{oldest_version: 10, last_version: 50}
        },
        # Quorum = 1
        parameters: %{desired_logs: 1}
      }

      result = LogDiscoveryPhase.execute(recovery_attempt)

      assert result.state == :create_vacancies
      assert result.old_log_ids_to_copy == [{:log, 1}]
      assert result.version_vector == {10, 50}
    end
  end

  describe "determine_old_logs_to_copy/3" do
    test "returns error for empty logs list" do
      result = LogDiscoveryPhase.determine_old_logs_to_copy([], %{}, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end

    test "successfully determines logs for quorum of 1" do
      old_logs = %{
        {:log, 1} => ["tag_a"]
      }

      recovery_info = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50}
      }

      {:ok, log_ids, version_vector} =
        LogDiscoveryPhase.determine_old_logs_to_copy(old_logs, recovery_info, 1)

      assert log_ids == [{:log, 1}]
      assert version_vector == {10, 50}
    end

    test "successfully determines logs for quorum of 2" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50},
        {:log, 2} => %{oldest_version: 5, last_version: 45},
        {:log, 3} => %{oldest_version: 15, last_version: 55}
      }

      {:ok, log_ids, version_vector} =
        LogDiscoveryPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      assert length(log_ids) == 2
      assert is_tuple(version_vector)
      # oldest version
      assert elem(version_vector, 0) >= 0
      # newest version
      assert elem(version_vector, 1) >= 0
    end

    test "returns error when recovery info is missing for all logs" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"]
      }

      # No recovery info available
      recovery_info = %{}

      result = LogDiscoveryPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end

    test "handles partial recovery info availability" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50},
        {:log, 2} => %{oldest_version: 5, last_version: 45}
        # {:log, 3} has no recovery info
      }

      {:ok, log_ids, version_vector} =
        LogDiscoveryPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Should work with available logs
      assert length(log_ids) == 2
      assert Enum.all?(log_ids, fn id -> id in [{:log, 1}, {:log, 2}] end)
      assert is_tuple(version_vector)
    end
  end

  describe "recovery_info_for_logs/2" do
    test "filters logs to only those with recovery info" do
      logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info_by_id = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50},
        {:log, 3} => %{oldest_version: 15, last_version: 55}
        # {:log, 2} has no recovery info
      }

      result = LogDiscoveryPhase.recovery_info_for_logs(logs, recovery_info_by_id)

      expected = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50},
        {:log, 3} => %{oldest_version: 15, last_version: 55}
      }

      assert result == expected
    end

    test "returns empty map when no logs have recovery info" do
      logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"]
      }

      recovery_info_by_id = %{}

      result = LogDiscoveryPhase.recovery_info_for_logs(logs, recovery_info_by_id)
      assert result == %{}
    end

    test "handles empty logs map" do
      result = LogDiscoveryPhase.recovery_info_for_logs(%{}, %{some: :info})
      assert result == %{}
    end
  end

  describe "combinations/2" do
    test "generates correct combinations for small lists" do
      result = LogDiscoveryPhase.combinations([1, 2, 3], 2)
      expected = [[1, 2], [1, 3], [2, 3]]
      assert Enum.sort(result) == Enum.sort(expected)
    end

    test "handles combinations of size 1" do
      result = LogDiscoveryPhase.combinations([1, 2, 3], 1)
      expected = [[1], [2], [3]]
      assert Enum.sort(result) == Enum.sort(expected)
    end

    test "handles combinations of size 0" do
      result = LogDiscoveryPhase.combinations([1, 2, 3], 0)
      assert result == [[]]
    end

    test "handles empty list" do
      result = LogDiscoveryPhase.combinations([], 2)
      assert result == []
    end

    test "handles combinations larger than list size" do
      result = LogDiscoveryPhase.combinations([1, 2], 3)
      assert result == []
    end
  end

  describe "version_vectors_by_id/1" do
    test "converts recovery info to version vectors" do
      log_info = %{
        {:log, 1} => %{oldest_version: 10, last_version: 50},
        {:log, 2} => %{oldest_version: 5, last_version: 45}
      }

      result = LogDiscoveryPhase.version_vectors_by_id(log_info)

      expected = [
        {{:log, 1}, {10, 50}},
        {{:log, 2}, {5, 45}}
      ]

      assert Enum.sort(result) == Enum.sort(expected)
    end

    test "handles empty log info" do
      result = LogDiscoveryPhase.version_vectors_by_id(%{})
      assert result == []
    end
  end

  describe "build_log_groups_and_vectors_from_combinations/1" do
    test "builds log groups with correct version vectors" do
      combinations = [
        [{{:log, 1}, {10, 50}}, {{:log, 2}, {5, 45}}],
        [{{:log, 1}, {10, 50}}, {{:log, 3}, {15, 55}}]
      ]

      result = LogDiscoveryPhase.build_log_groups_and_vectors_from_combinations(combinations)

      assert length(result) == 2

      # Check first group: max(10, 5) = 10, min(50, 45) = 45
      {log_ids_1, {oldest_1, newest_1}} = Enum.at(result, 0)
      assert Enum.sort(log_ids_1) == Enum.sort([{:log, 1}, {:log, 2}])
      assert oldest_1 == 10
      assert newest_1 == 45

      # Check second group: max(10, 15) = 15, min(50, 55) = 50  
      {log_ids_2, {oldest_2, newest_2}} = Enum.at(result, 1)
      assert Enum.sort(log_ids_2) == Enum.sort([{:log, 1}, {:log, 3}])
      assert oldest_2 == 15
      assert newest_2 == 50
    end

    test "filters out invalid ranges" do
      combinations = [
        # newest < oldest (invalid)
        [{{:log, 1}, {50, 10}}],
        # valid
        [{{:log, 2}, {10, 50}}]
      ]

      result = LogDiscoveryPhase.build_log_groups_and_vectors_from_combinations(combinations)

      assert length(result) == 1
      {log_ids, {oldest, newest}} = List.first(result)
      assert log_ids == [{:log, 2}]
      assert oldest == 10
      assert newest == 50
    end
  end

  describe "valid_range?/1" do
    test "allows valid ranges where newest >= oldest" do
      assert LogDiscoveryPhase.valid_range?({[], {10, 50}}) == true
      assert LogDiscoveryPhase.valid_range?({[], {10, 10}}) == true
    end

    test "rejects invalid ranges where newest < oldest" do
      assert LogDiscoveryPhase.valid_range?({[], {50, 10}}) == false
    end

    test "handles special case with oldest = 0" do
      assert LogDiscoveryPhase.valid_range?({[], {0, 50}}) == true
      assert LogDiscoveryPhase.valid_range?({[], {0, 0}}) == true
    end

    test "handles special case with newest = 0" do
      assert LogDiscoveryPhase.valid_range?({[], {10, 0}}) == false
      assert LogDiscoveryPhase.valid_range?({[], {0, 0}}) == true
    end
  end

  describe "rank_log_groups/1" do
    test "ranks groups by difference between newest and oldest (descending)" do
      groups = [
        # difference: 20
        {[{:log, 1}], {10, 30}},
        # difference: 10  
        {[{:log, 2}], {5, 15}},
        # difference: 25
        {[{:log, 3}], {20, 45}}
      ]

      result = LogDiscoveryPhase.rank_log_groups(groups)

      # Should be sorted by difference descending: 25, 20, 10
      assert result == [
               # difference: 25
               {[{:log, 3}], {20, 45}},
               # difference: 20
               {[{:log, 1}], {10, 30}},
               # difference: 10
               {[{:log, 2}], {5, 15}}
             ]
    end

    test "handles groups with same difference" do
      groups = [
        # difference: 20
        {[{:log, 1}], {10, 30}},
        # difference: 20
        {[{:log, 2}], {5, 25}}
      ]

      result = LogDiscoveryPhase.rank_log_groups(groups)

      # Both have same difference, order should be stable
      assert length(result) == 2
      assert Enum.all?(result, fn {_, {oldest, newest}} -> newest - oldest == 20 end)
    end

    test "handles empty groups list" do
      result = LogDiscoveryPhase.rank_log_groups([])
      assert result == []
    end
  end
end
