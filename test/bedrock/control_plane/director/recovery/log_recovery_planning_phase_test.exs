defmodule Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.DataPlane.Version

  # Helper functions for common test setup
  defp recovery_setup(log_recovery_info, old_logs, desired_logs) do
    {
      with_log_recovery_info(recovery_attempt(), log_recovery_info),
      %{
        node_tracking: nil,
        old_transaction_system_layout: %{logs: old_logs},
        cluster_config: %{
          parameters: %{desired_logs: desired_logs},
          transaction_system_layout: %{logs: old_logs}
        }
      }
    }
  end

  defp log_info(oldest, last),
    do: %{
      oldest_version: Version.from_integer(oldest),
      last_version: Version.from_integer(last),
      minimum_durable_version: Version.from_integer(oldest)
    }

  describe "execute/1 - majority quorum" do
    test "successfully determines logs to copy with all logs available" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 2)

      assert {%{old_log_ids_to_copy: old_log_ids, version_vector: version_vector, durable_version: durable_version},
              LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert is_list(old_log_ids) and length(old_log_ids) == 2
      assert is_tuple(version_vector)
      # version_vector is {max(oldest), min(newest)} = {10, 45}
      assert version_vector == {Version.from_integer(10), Version.from_integer(45)}
      # durable_version is min of minimum_durable_versions (5 and 10)
      assert durable_version == Version.from_integer(5)
    end

    test "succeeds with majority quorum (2 of 3 logs available)" do
      # 2 of 3 logs available meets majority quorum (2 > 1.5)
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}, {:log, 3} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 3)

      assert {%{old_log_ids_to_copy: old_log_ids, version_vector: version_vector}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert length(old_log_ids) == 2
      # version_vector is {max(oldest), min(newest)} = {10, 45}
      assert version_vector == {Version.from_integer(10), Version.from_integer(45)}
    end

    test "stalls recovery when unable to meet log quorum (1 of 3)" do
      # 1 of 3 logs does not meet majority quorum (1 is not > 1.5)
      log_recovery_info = %{{:log, 1} => log_info(10, 50)}
      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}, {:log, 3} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 3)

      {_result, next_phase_or_stall} = LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert {:stalled, :unable_to_meet_log_quorum} = next_phase_or_stall
    end

    test "stalls recovery when no logs available" do
      {recovery_attempt, context} = recovery_setup(%{}, %{}, 3)

      {_result, next_phase_or_stall} = LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert {:stalled, :unable_to_meet_log_quorum} = next_phase_or_stall
    end

    test "handles single log with quorum of 1" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50)}
      old_logs = %{{:log, 1} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 1)

      expected_version = {Version.from_integer(10), Version.from_integer(50)}

      assert {%{old_log_ids_to_copy: [{:log, 1}], version_vector: ^expected_version, durable_version: durable_version},
              LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert durable_version == Version.from_integer(10)
    end

    test "stores survivor_log_ids in recovery_attempt" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}, {:log, 3} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 3)

      {result, LogRecruitmentPhase} = LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert Map.has_key?(result, :survivor_log_ids)
      assert length(result.survivor_log_ids) == 2
      assert {:log, 1} in result.survivor_log_ids
      assert {:log, 2} in result.survivor_log_ids
    end

    test "rebuilds consistent-hash recovery layout from fresh log vacancies" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 3} => log_info(5, 45)}

      old_logs = %{
        {:log, 1} => [],
        {:log, 2} => [],
        {:log, 3} => []
      }

      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 3)

      assert {%{logs: logs}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert logs == %{{:vacancy, 1} => [], {:vacancy, 2} => [], {:vacancy, 3} => []}
    end

    test "uses fresh vacancies for all-empty consistent-hash layouts" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => [], {:log, 2} => []}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 2)

      assert {%{logs: logs, old_log_ids_to_copy: old_log_ids}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert length(old_log_ids) == 2
      assert logs == %{{:vacancy, 1} => [], {:vacancy, 2} => []}
    end

    test "preserves survivor descriptors for legacy non-empty layouts" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => ["tag_a", "tag_b"], {:log, 2} => ["tag_a"]}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 2)

      assert {%{logs: logs, old_log_ids_to_copy: old_log_ids}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert length(old_log_ids) == 2
      assert logs == old_logs
    end
  end

  describe "compute_version_vector/1" do
    test "computes version vector as {max(oldest), min(newest)}" do
      log_recovery_info = %{
        {:log, 1} => log_info(10, 50),
        {:log, 2} => log_info(5, 45),
        {:log, 3} => log_info(15, 55)
      }

      # max(oldest) = max(10, 5, 15) = 15
      # min(newest) = min(50, 45, 55) = 45
      expected_version_vector = {Version.from_integer(15), Version.from_integer(45)}

      assert {:ok, ^expected_version_vector} =
               LogRecoveryPlanningPhase.compute_version_vector(log_recovery_info)
    end

    test "returns error for empty log recovery info" do
      assert {:error, :invalid_version_range} =
               LogRecoveryPlanningPhase.compute_version_vector(%{})
    end

    test "handles single log" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50)}
      expected_version_vector = {Version.from_integer(10), Version.from_integer(50)}

      assert {:ok, ^expected_version_vector} =
               LogRecoveryPlanningPhase.compute_version_vector(log_recovery_info)
    end

    test "returns error for invalid version range (newest < oldest after aggregation)" do
      # If all logs have inverted ranges, the aggregation produces invalid range
      log_recovery_info = %{
        {:log, 1} => log_info(50, 10),
        {:log, 2} => log_info(45, 15)
      }

      # max(oldest) = 50, min(newest) = 10 -> invalid because 10 < 50
      assert {:error, :invalid_version_range} =
               LogRecoveryPlanningPhase.compute_version_vector(log_recovery_info)
    end

    test "handles logs starting at version zero" do
      log_recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.zero(),
          last_version: Version.from_integer(50),
          minimum_durable_version: Version.zero()
        },
        {:log, 2} => log_info(5, 45)
      }

      # max(oldest) = max(0, 5) = 5
      # min(newest) = min(50, 45) = 45
      expected_version_vector = {Version.from_integer(5), Version.from_integer(45)}

      assert {:ok, ^expected_version_vector} =
               LogRecoveryPlanningPhase.compute_version_vector(log_recovery_info)
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
        assert LogRecoveryPlanningPhase.valid_range?(range)
      end)

      Enum.each(invalid_ranges, fn range ->
        refute LogRecoveryPlanningPhase.valid_range?(range)
      end)
    end
  end

  describe "calculate_durable_version/1" do
    test "returns minimum of all available minimum_durable_versions" do
      log_recovery_info = %{
        {:log, 1} => %{minimum_durable_version: Version.from_integer(100)},
        {:log, 2} => %{minimum_durable_version: Version.from_integer(50)},
        {:log, 3} => %{minimum_durable_version: Version.from_integer(75)}
      }

      assert Version.from_integer(50) == LogRecoveryPlanningPhase.calculate_durable_version(log_recovery_info)
    end

    test "ignores :unavailable minimum_durable_versions" do
      log_recovery_info = %{
        {:log, 1} => %{minimum_durable_version: Version.from_integer(100)},
        {:log, 2} => %{minimum_durable_version: :unavailable},
        {:log, 3} => %{minimum_durable_version: Version.from_integer(75)}
      }

      assert Version.from_integer(75) == LogRecoveryPlanningPhase.calculate_durable_version(log_recovery_info)
    end

    test "returns Version.zero when all are :unavailable" do
      log_recovery_info = %{
        {:log, 1} => %{minimum_durable_version: :unavailable},
        {:log, 2} => %{minimum_durable_version: :unavailable}
      }

      assert Version.zero() == LogRecoveryPlanningPhase.calculate_durable_version(log_recovery_info)
    end

    test "returns Version.zero for empty log recovery info" do
      assert Version.zero() == LogRecoveryPlanningPhase.calculate_durable_version(%{})
    end
  end

  describe "majority quorum edge cases" do
    test "2 of 4 logs does not meet majority (2 is not > 2)" do
      log_recovery_info = %{{:log, 1} => log_info(10, 50), {:log, 2} => log_info(5, 45)}
      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}, {:log, 3} => %{}, {:log, 4} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 4)

      {_result, next_phase_or_stall} = LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert {:stalled, :unable_to_meet_log_quorum} = next_phase_or_stall
    end

    test "3 of 4 logs meets majority (3 > 2)" do
      log_recovery_info = %{
        {:log, 1} => log_info(10, 50),
        {:log, 2} => log_info(5, 45),
        {:log, 3} => log_info(15, 55)
      }

      old_logs = %{{:log, 1} => %{}, {:log, 2} => %{}, {:log, 3} => %{}, {:log, 4} => %{}}
      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 4)

      assert {%{old_log_ids_to_copy: old_log_ids}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert length(old_log_ids) == 3
    end

    test "3 of 5 logs meets majority (3 > 2.5)" do
      log_recovery_info = %{
        {:log, 1} => log_info(10, 50),
        {:log, 2} => log_info(5, 45),
        {:log, 3} => log_info(15, 55)
      }

      old_logs = %{
        {:log, 1} => %{},
        {:log, 2} => %{},
        {:log, 3} => %{},
        {:log, 4} => %{},
        {:log, 5} => %{}
      }

      {recovery_attempt, context} = recovery_setup(log_recovery_info, old_logs, 5)

      assert {%{old_log_ids_to_copy: old_log_ids}, LogRecruitmentPhase} =
               LogRecoveryPlanningPhase.execute(recovery_attempt, context)

      assert length(old_log_ids) == 3
    end
  end
end
