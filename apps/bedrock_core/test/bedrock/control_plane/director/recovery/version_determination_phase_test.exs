defmodule Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase
  alias Bedrock.DataPlane.Version

  # Helper functions for common test patterns
  defp create_storage_teams(team_configs) do
    Enum.map(team_configs, fn {tag, storage_ids} ->
      %{tag: tag, storage_ids: storage_ids}
    end)
  end

  defp create_storage_info(storages) do
    Map.new(storages, fn {id, durable, oldest} ->
      {id,
       %{
         durable_version: Version.from_integer(durable),
         oldest_durable_version: Version.from_integer(oldest)
       }}
    end)
  end

  defp basic_execution_context(storage_teams, replication_factor \\ 3) do
    %{
      node_tracking: nil,
      old_transaction_system_layout: %{storage_teams: storage_teams},
      cluster_config: %{
        parameters: %{desired_replication_factor: replication_factor}
      }
    }
  end

  describe "execute/1" do
    test "successfully determines durable version with healthy teams" do
      storage_teams =
        create_storage_teams([
          {"team_1", ["storage_1", "storage_2", "storage_3"]},
          {"team_2", ["storage_4", "storage_5", "storage_6"]}
        ])

      storage_recovery_info =
        create_storage_info([
          {"storage_1", 100, 50},
          {"storage_2", 102, 50},
          {"storage_3", 101, 50},
          {"storage_4", 98, 40},
          {"storage_5", 99, 40},
          {"storage_6", 97, 40}
        ])

      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

      capture_log(fn ->
        assert {%{durable_version: durable_version, degraded_teams: degraded_teams}, LogRecruitmentPhase} =
                 VersionDeterminationPhase.execute(recovery_attempt, basic_execution_context(storage_teams))

        assert durable_version == Version.from_integer(98)
        assert length(degraded_teams) == 2
      end)
    end

    test "handles insufficient replication and stalls recovery" do
      # Only 1 storage, need quorum=2
      storage_teams = create_storage_teams([{"team_1", ["storage_1"]}])
      storage_recovery_info = create_storage_info([{"storage_1", 100, 50}])
      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

      assert {_result, {:stalled, {:insufficient_replication, ["team_1"]}}} =
               VersionDeterminationPhase.execute(recovery_attempt, basic_execution_context(storage_teams))
    end

    test "identifies degraded teams correctly" do
      storage_teams =
        create_storage_teams([
          # Only 2 storages
          {"team_1", ["storage_1", "storage_2"]},
          # 3 storages
          {"team_2", ["storage_3", "storage_4", "storage_5"]}
        ])

      storage_recovery_info =
        create_storage_info([
          {"storage_1", 100, 50},
          {"storage_2", 102, 50},
          {"storage_3", 98, 40},
          {"storage_4", 99, 40},
          {"storage_5", 97, 40}
        ])

      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

      assert {%{durable_version: durable_version, degraded_teams: degraded_teams}, LogRecruitmentPhase} =
               VersionDeterminationPhase.execute(recovery_attempt, basic_execution_context(storage_teams))

      # min(100, 98) from team quorums
      assert durable_version == Version.from_integer(98)
      # 2 storages == 2 quorum (healthy)
      refute "team_1" in degraded_teams
      # 3 storages > 2 quorum (degraded)
      assert "team_2" in degraded_teams
    end
  end

  describe "determine_durable_version/3" do
    test "returns minimum version across all healthy teams" do
      teams = create_storage_teams([{"team_1", ["s1", "s2", "s3"]}, {"team_2", ["s4", "s5", "s6"]}])
      # Create simplified storage info (no oldest_durable_version needed for this function)
      info_by_id =
        Map.new(
          [
            {"s1", 100},
            {"s2", 102},
            {"s3", 101},
            {"s4", 98},
            {"s5", 99},
            {"s6", 97}
          ],
          fn {id, version} -> {id, %{durable_version: Version.from_integer(version)}} end
        )

      assert {:ok, durable_version, _healthy_teams, degraded_teams} =
               VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == Version.from_integer(98)
      # Both teams have 3 storages but quorum is 2, so both are degraded
      assert length(degraded_teams) == 2
    end

    test "returns error when team has insufficient replication" do
      teams =
        create_storage_teams([
          # Only 1 storage
          {"team_1", ["s1"]},
          {"team_2", ["s2", "s3", "s4"]}
        ])

      info_by_id =
        Map.new(
          [
            {"s1", 100},
            {"s2", 98},
            {"s3", 99},
            {"s4", 97}
          ],
          fn {id, version} -> {id, %{durable_version: Version.from_integer(version)}} end
        )

      assert {:error, {:insufficient_replication, failed_teams}} =
               VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      assert "team_1" in failed_teams
    end

    test "correctly categorizes healthy vs degraded teams" do
      teams =
        create_storage_teams([
          # 3 storages > quorum 2 = degraded
          {"degraded", ["s1", "s2", "s3"]},
          # 2 storages == quorum 2 = healthy
          {"healthy", ["s4", "s5"]}
        ])

      info_by_id =
        Map.new(
          [
            {"s1", 100},
            {"s2", 102},
            {"s3", 101},
            {"s4", 98},
            {"s5", 99}
          ],
          fn {id, version} -> {id, %{durable_version: Version.from_integer(version)}} end
        )

      assert {:ok, durable_version, healthy_teams, degraded_teams} =
               VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == Version.from_integer(98)
      assert "healthy" in healthy_teams
      assert "degraded" in degraded_teams
    end

    test "handles missing storage info gracefully" do
      teams = create_storage_teams([{"team_1", ["s1", "s2", "missing"]}])
      # "missing" not in info_by_id
      info_by_id =
        Map.new(
          [
            {"s1", 100},
            {"s2", 102}
          ],
          fn {id, version} -> {id, %{durable_version: Version.from_integer(version)}} end
        )

      assert {:ok, durable_version, _healthy_teams, []} =
               VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # From s1 and s2, quorum of 2
      assert durable_version == Version.from_integer(100)
    end
  end

  describe "smallest_version/2" do
    test "handles nil arguments correctly" do
      v100 = Version.from_integer(100)
      assert VersionDeterminationPhase.smallest_version(nil, v100) == v100
      assert VersionDeterminationPhase.smallest_version(v100, nil) == v100
      assert VersionDeterminationPhase.smallest_version(nil, nil) == nil
    end

    test "returns correct minimum of non-nil versions" do
      v50 = Version.from_integer(50)
      v100 = Version.from_integer(100)
      v150 = Version.from_integer(150)
      assert VersionDeterminationPhase.smallest_version(v150, v100) == v100
      assert VersionDeterminationPhase.smallest_version(v50, v100) == v50
      assert VersionDeterminationPhase.smallest_version(v100, v100) == v100
    end

    test "correctly accumulates minimum across multiple versions using reduce" do
      versions = Enum.map([120, 100, 150, 80], &Version.from_integer/1)

      result =
        Enum.reduce(versions, nil, fn version, acc ->
          VersionDeterminationPhase.smallest_version(version, acc)
        end)

      assert result == Version.from_integer(80)
    end

    test "works correctly with various version magnitudes and accumulation" do
      very_small = Version.from_integer(1)
      small = Version.from_integer(50)
      medium = Version.from_integer(100)
      large = Version.from_integer(1000)

      # Accumulation behavior
      assert VersionDeterminationPhase.smallest_version(medium, nil) == medium

      # Various magnitudes
      assert VersionDeterminationPhase.smallest_version(very_small, nil) == very_small
      assert VersionDeterminationPhase.smallest_version(nil, very_small) == very_small
      assert VersionDeterminationPhase.smallest_version(large, very_small) == very_small
      assert VersionDeterminationPhase.smallest_version(medium, small) == small
    end
  end

  describe "determine_durable_version_and_status_for_storage_team/3" do
    test "returns degraded status when team has more than quorum storages" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "s3"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)},
        "s3" => %{durable_version: Version.from_integer(101)}
      }

      assert {:ok, version, :degraded} =
               VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
                 team,
                 info_by_id,
                 2
               )

      # 2nd highest version (102, 101, 100) -> 101
      assert version == Version.from_integer(101)
    end

    test "returns healthy status when team has exactly quorum storages" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)}
      }

      assert {:ok, version, :healthy} =
               VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
                 team,
                 info_by_id,
                 2
               )

      # 2nd highest version (102, 100) -> 100
      assert version == Version.from_integer(100)
    end

    test "returns error when team has insufficient storages" do
      team = %{tag: "team_1", storage_ids: ["s1"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)}
      }

      assert {:error, :insufficient_replication} =
               VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
                 team,
                 info_by_id,
                 2
               )
    end

    test "handles missing storage info" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "missing"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)}
        # "missing" not in info_by_id
      }

      assert {:ok, version, :healthy} =
               VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
                 team,
                 info_by_id,
                 2
               )

      # 2nd highest from available (102, 100) -> 100
      assert version == Version.from_integer(100)
    end

    test "calculates quorum version correctly with different scenarios" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "s3", "s4", "s5"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)},
        "s3" => %{durable_version: Version.from_integer(101)},
        "s4" => %{durable_version: Version.from_integer(103)},
        "s5" => %{durable_version: Version.from_integer(99)}
      }

      # Quorum of 3 means we take the 3rd highest version
      assert {:ok, version, :degraded} =
               VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
                 team,
                 info_by_id,
                 3
               )

      # Sorted: [99, 100, 101, 102, 103] -> 3rd from end is 101
      assert version == Version.from_integer(101)
    end
  end
end
