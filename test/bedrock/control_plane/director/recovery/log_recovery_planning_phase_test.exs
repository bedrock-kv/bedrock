defmodule Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase
  alias Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase
  alias Bedrock.DataPlane.Version

  # Helper functions for common test setup
  defp recovery_setup(log_recovery_info, old_logs, desired_logs) do
    {
      with_log_recovery_info(recovery_attempt(), log_recovery_info),
      %{
        node_tracking: nil,
        old_transaction_system_layout: %{logs: old_logs, storage_teams: []},
        cluster_config: %{
          parameters: %{desired_logs: desired_logs},
          transaction_system_layout: %{logs: old_logs}
        }
      }
    }
  end

  defp log_info(oldest, last),
    do: %{oldest_version: Version.from_integer(oldest), last_version: Version.from_integer(last)}

  describe "execute/1" do
    test "successfully determines logs to copy and advances state" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"]}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 3)

      assert {%{old_log_ids_to_copy: old_log_ids, version_vector: version_vector}, VacancyCreationPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert is_list(old_log_ids) and length(old_log_ids) > 0
      assert is_tuple(version_vector)
    end

    test "stalls recovery when unable to meet log quorum" do
      {recovery_attempt, context} = recovery_setup(%{}, %{}, 3)

      {_result, next_phase_or_stall} = LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert {:stalled, :unable_to_meet_log_quorum} = next_phase_or_stall
    end

    test "handles single log with quorum of 1" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50)}
      old_logs = %{{:log, 1} => ["tag_a"]}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 1)

      expected_version = {Version.from_integer(10), Version.from_integer(50)}

      assert {%{old_log_ids_to_copy: [{:log, 1}], version_vector: ^expected_version}, VacancyCreationPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)
    end
  end

  describe "determine_old_logs_to_copy/3" do
    test "returns error for empty logs list" do
      result = LogRecoveryPlanningPhase.determine_old_logs_to_copy([], %{}, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end

    test "successfully determines logs for quorum of 1" do
      old_logs = %{{:log, 1} => ["tag_a"]}
      recovery_info = %{{:log, 1} => log_info(10, 50)}
      expected_version_vector = {Version.from_integer(10), Version.from_integer(50)}

      assert {:ok, [{:log, 1}], ^expected_version_vector} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 1)
    end

    test "successfully determines logs for quorum of 2" do
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"], {:log, 3} => ["tag_c"]}
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45), {:log, 3} => log_info(15, 55)}
      expected_version_vector = {Version.from_integer(15), Version.from_integer(45)}
      expected_logs = [{:log, 1}, {:log, 2}, {:log, 3}]

      assert {:ok, log_ids, ^expected_version_vector} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Shard-aware algorithm includes all available shards
      assert length(log_ids) == 3 and Enum.all?(expected_logs, &(&1 in log_ids))
    end

    test "returns error when recovery info is missing for all logs" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"]
      }

      # No recovery info available
      recovery_info = %{}

      result = LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end

    test "handles partial recovery info availability" do
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"], {:log, 3} => ["tag_c"]}
      # {:log, 3} has no recovery info
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}

      # Shard-aware algorithm requires all shards to participate
      # Since shard "tag_c" has no recovery info, it fails
      assert {:error, :unable_to_meet_log_quorum} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
    end
  end

  describe "recovery_info_for_logs/2" do
    test "filters logs to only those with recovery info" do
      logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"], {:log, 3} => ["tag_c"]}
      # {:log, 2} has no recovery info
      recovery_info_by_id = %{{:log, 1} => log_info(10, 50), {:log, 3} => log_info(15, 55)}

      expected = %{{:log, 1} => log_info(10, 50), {:log, 3} => log_info(15, 55)}

      assert ^expected = LogRecoveryPlanningPhase.recovery_info_for_logs(logs, recovery_info_by_id)
    end

    test "returns empty map when no logs have recovery info" do
      logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"]}

      assert %{} = LogRecoveryPlanningPhase.recovery_info_for_logs(logs, %{})
    end

    test "handles empty logs map" do
      result = LogRecoveryPlanningPhase.recovery_info_for_logs(%{}, %{some: :info})
      assert result == %{}
    end
  end

  describe "combinations/2" do
    test "generates correct combinations for small lists" do
      result = LogRecoveryPlanningPhase.combinations([1, 2, 3], 2)
      expected = [[1, 2], [1, 3], [2, 3]]
      assert MapSet.new(result) == MapSet.new(expected)
    end

    test "handles combinations of size 1" do
      result = LogRecoveryPlanningPhase.combinations([1, 2, 3], 1)
      expected = [[1], [2], [3]]
      assert MapSet.new(result) == MapSet.new(expected)
    end

    test "handles edge cases" do
      edge_cases = [
        # {input_list, size, expected_result, description}
        {[1, 2, 3], 0, [[]], "size 0 combinations"},
        {[], 2, [], "empty list"},
        {[1, 2], 3, [], "combinations larger than list size"}
      ]

      Enum.each(edge_cases, fn {input_list, size, expected, _description} ->
        assert LogRecoveryPlanningPhase.combinations(input_list, size) == expected
      end)
    end
  end

  describe "version_vectors_by_id/1" do
    test "converts recovery info to version vectors" do
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}

      expected = [
        {{:log, 1}, {Version.from_integer(10), Version.from_integer(50)}},
        {{:log, 2}, {Version.from_integer(5), Version.from_integer(45)}}
      ]

      result = LogRecoveryPlanningPhase.version_vectors_by_id(recovery_info)
      assert MapSet.new(result) == MapSet.new(expected)
    end

    test "handles empty log info" do
      result = LogRecoveryPlanningPhase.version_vectors_by_id(%{})
      assert result == []
    end
  end

  describe "build_log_groups_and_vectors_from_combinations/1" do
    test "builds log groups with correct version vectors" do
      combinations = [
        [
          {{:log, 1}, {Version.from_integer(10), Version.from_integer(50)}},
          {{:log, 2}, {Version.from_integer(5), Version.from_integer(45)}}
        ],
        [
          {{:log, 1}, {Version.from_integer(10), Version.from_integer(50)}},
          {{:log, 3}, {Version.from_integer(15), Version.from_integer(55)}}
        ]
      ]

      result = LogRecoveryPlanningPhase.build_log_groups_and_vectors_from_combinations(combinations)
      assert length(result) == 2

      # Check first group: max(10, 5) = 10, min(50, 45) = 45
      expected_version_1 = {Version.from_integer(10), Version.from_integer(45)}
      assert {log_ids_1, ^expected_version_1} = Enum.at(result, 0)
      assert MapSet.new(log_ids_1) == MapSet.new([{:log, 1}, {:log, 2}])

      # Check second group: max(10, 15) = 15, min(50, 55) = 50
      expected_version_2 = {Version.from_integer(15), Version.from_integer(50)}
      assert {log_ids_2, ^expected_version_2} = Enum.at(result, 1)
      assert MapSet.new(log_ids_2) == MapSet.new([{:log, 1}, {:log, 3}])
    end

    test "filters out invalid ranges" do
      combinations = [
        # newest < oldest (invalid)
        [{{:log, 1}, {Version.from_integer(50), Version.from_integer(10)}}],
        # valid
        [{{:log, 2}, {Version.from_integer(10), Version.from_integer(50)}}]
      ]

      result = LogRecoveryPlanningPhase.build_log_groups_and_vectors_from_combinations(combinations)
      assert length(result) == 1
      expected = {[{:log, 2}], {Version.from_integer(10), Version.from_integer(50)}}
      assert ^expected = List.first(result)
    end
  end

  describe "valid_range?/1" do
    test "validates version ranges correctly" do
      valid_ranges = [
        {Version.from_integer(10), Version.from_integer(50)},
        {Version.from_integer(10), Version.from_integer(10)},
        {Version.zero(), Version.from_integer(50)},
        {Version.zero(), Version.zero()}
      ]

      invalid_ranges = [
        {Version.from_integer(50), Version.from_integer(10)},
        {Version.from_integer(10), Version.zero()}
      ]

      Enum.each(valid_ranges, fn range ->
        assert LogRecoveryPlanningPhase.valid_range?({[], range})
      end)

      Enum.each(invalid_ranges, fn range ->
        refute LogRecoveryPlanningPhase.valid_range?({[], range})
      end)
    end
  end

  describe "rank_log_groups/1" do
    test "ranks groups by difference between newest and oldest (descending)" do
      groups = [
        # difference: 20
        {[{:log, 1}], {Version.from_integer(10), Version.from_integer(30)}},
        # difference: 10
        {[{:log, 2}], {Version.from_integer(5), Version.from_integer(15)}},
        # difference: 25
        {[{:log, 3}], {Version.from_integer(20), Version.from_integer(45)}}
      ]

      result = LogRecoveryPlanningPhase.rank_log_groups(groups)

      # Should be sorted by difference descending: 25, 20, 10
      assert result == [
               # difference: 25
               {[{:log, 3}], {Version.from_integer(20), Version.from_integer(45)}},
               # difference: 20
               {[{:log, 1}], {Version.from_integer(10), Version.from_integer(30)}},
               # difference: 10
               {[{:log, 2}], {Version.from_integer(5), Version.from_integer(15)}}
             ]
    end

    test "handles groups with same difference" do
      groups = [
        # difference: 20
        {[{:log, 1}], {Version.from_integer(10), Version.from_integer(30)}},
        # difference: 20
        {[{:log, 2}], {Version.from_integer(5), Version.from_integer(25)}}
      ]

      result = LogRecoveryPlanningPhase.rank_log_groups(groups)

      # Both have same difference, order should be stable
      assert length(result) == 2

      assert Enum.all?(result, fn {_, {oldest, newest}} ->
               Version.distance(newest, oldest) == 20
             end)
    end

    test "handles empty groups list" do
      result = LogRecoveryPlanningPhase.rank_log_groups([])
      assert result == []
    end
  end

  describe "shard-aware algorithm" do
    test "uses shard-aware algorithm when multiple logs per shard exist" do
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_a"], {:log, 3} => ["tag_b"], {:log, 4} => ["tag_b"]}

      recovery_info = %{
        {:log, 1} => log_info(10, 50),
        {:log, 2} => log_info(15, 45),
        {:log, 3} => log_info(5, 60),
        {:log, 4} => log_info(20, 55)
      }

      # Version vector should be cross-shard minimum of maximums
      # Shard A: max(10,15) = 15, min(50,45) = 45 -> {15, 45}
      # Shard B: max(5,20) = 20, min(60,55) = 55 -> {20, 55}
      # Cross-shard: max(15,20) = 20, min(45,55) = 45
      expected_version_vector = {Version.from_integer(20), Version.from_integer(45)}
      expected_logs = [{:log, 1}, {:log, 2}, {:log, 3}, {:log, 4}]

      assert {:ok, log_ids, ^expected_version_vector} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Should include logs from both shards
      assert length(log_ids) == 4 and Enum.all?(expected_logs, &(&1 in log_ids))
    end

    test "uses shard-aware algorithm universally for all scenarios" do
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_b"], {:log, 3} => ["tag_c"]}
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45), {:log, 3} => log_info(15, 55)}
      expected_logs = [{:log, 1}, {:log, 2}, {:log, 3}]

      # Conservative minimum for storage lag tolerance
      expected_version_vector = {Version.from_integer(15), Version.from_integer(45)}

      assert {:ok, log_ids, ^expected_version_vector} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Shard-aware algorithm includes all shards and provides conservative minimum
      assert length(log_ids) == 3 and Enum.all?(expected_logs, &(&1 in log_ids))
    end

    test "handles logs participating in multiple shards" do
      # log 1 participates in both shards
      old_logs = %{{:log, 1} => ["tag_a", "tag_b"], {:log, 2} => ["tag_a"], {:log, 3} => ["tag_b"]}
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45), {:log, 3} => log_info(15, 55)}
      expected_logs = [{:log, 1}, {:log, 2}, {:log, 3}]

      assert {:ok, log_ids, _version_vector} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Should include all logs since both shards have multiple logs
      assert length(log_ids) == 3 and Enum.all?(expected_logs, &(&1 in log_ids))
    end

    test "fails when shard cannot meet quorum" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_a"],
        {:log, 3} => ["tag_b"],
        {:log, 4} => ["tag_b"],
        {:log, 5} => ["tag_b"]
      }

      # logs 4 and 5 have no recovery info - shard B cannot meet quorum (1 < 2)
      recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(15, 45), {:log, 3} => log_info(5, 40)}

      assert {:error, :unable_to_meet_log_quorum} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
    end

    test "returns error when all shards result in invalid version ranges" do
      # Logs with invalid version ranges (newest < oldest)
      old_logs = %{{:log, 1} => ["tag_a"], {:log, 2} => ["tag_a"]}

      # Both logs have inverted ranges (newest < oldest)
      recovery_info = %{
        {:log, 1} => log_info(50, 10),
        {:log, 2} => log_info(45, 15)
      }

      # Should fail because all combinations produce invalid ranges
      assert {:error, :unable_to_meet_log_quorum} =
               LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
    end
  end

  describe "shard grouping functions" do
    test "group_logs_by_shard groups logs correctly" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(15, 45)}
      old_logs = %{{:log, 1} => ["tag_a", "tag_b"], {:log, 2} => ["tag_a"]}

      result = LogRecoveryPlanningPhase.group_logs_by_shard(log_recovery_info, old_logs)

      # Check structure (order may vary due to list insertion)
      assert MapSet.new(Map.keys(result)) == MapSet.new(["tag_a", "tag_b"])
      assert length(result["tag_a"]) == 2 and length(result["tag_b"]) == 1
    end
  end
end
