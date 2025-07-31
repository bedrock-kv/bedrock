defmodule Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhaseTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase

  describe "execute/1" do
    test "successfully determines durable version with healthy teams" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2", "storage_3"]},
        %{tag: "team_2", storage_ids: ["storage_4", "storage_5", "storage_6"]}
      ]

      storage_recovery_info = %{
        "storage_1" => %{durable_version: 100, oldest_durable_version: 50},
        "storage_2" => %{durable_version: 102, oldest_durable_version: 50},
        "storage_3" => %{durable_version: 101, oldest_durable_version: 50},
        "storage_4" => %{durable_version: 98, oldest_durable_version: 40},
        "storage_5" => %{durable_version: 99, oldest_durable_version: 40},
        "storage_6" => %{durable_version: 97, oldest_durable_version: 40}
      }

      recovery_attempt =
        recovery_attempt()
        |> with_storage_recovery_info(storage_recovery_info)

      capture_log(fn ->
        {result, next_phase} =
          VersionDeterminationPhase.execute(recovery_attempt, %{
            node_tracking: nil,
            old_transaction_system_layout: %{storage_teams: storage_teams},
            cluster_config: %{
              parameters: %{desired_replication_factor: 3}
            }
          })

        assert next_phase == Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
        assert result.durable_version == 98
        assert length(result.degraded_teams) == 2
      end)
    end

    test "handles insufficient replication and stalls recovery" do
      storage_teams = [
        # Only 1 storage, need quorum=2
        %{tag: "team_1", storage_ids: ["storage_1"]}
      ]

      storage_recovery_info = %{
        "storage_1" => %{durable_version: 100, oldest_durable_version: 50}
      }

      recovery_attempt =
        recovery_attempt()
        |> with_storage_recovery_info(storage_recovery_info)

      {_result, next_phase_or_stall} =
        VersionDeterminationPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{storage_teams: storage_teams},
          cluster_config: %{
            # Quorum = 2, but only 1 storage
            parameters: %{desired_replication_factor: 3}
          }
        })

      assert {:stalled, {:insufficient_replication, ["team_1"]}} = next_phase_or_stall
    end

    test "identifies degraded teams correctly" do
      storage_teams = [
        # Only 2 storages
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2"]},
        # 3 storages
        %{tag: "team_2", storage_ids: ["storage_3", "storage_4", "storage_5"]}
      ]

      storage_recovery_info = %{
        "storage_1" => %{durable_version: 100, oldest_durable_version: 50},
        "storage_2" => %{durable_version: 102, oldest_durable_version: 50},
        "storage_3" => %{durable_version: 98, oldest_durable_version: 40},
        "storage_4" => %{durable_version: 99, oldest_durable_version: 40},
        "storage_5" => %{durable_version: 97, oldest_durable_version: 40}
      }

      recovery_attempt =
        recovery_attempt()
        |> with_storage_recovery_info(storage_recovery_info)

      {result, next_phase} =
        VersionDeterminationPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{storage_teams: storage_teams},
          cluster_config: %{
            # Quorum = 2
            parameters: %{desired_replication_factor: 3}
          }
        })

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
      # min(100, 98) from team quorums
      assert result.durable_version == 98
      # 2 storages == 2 quorum (healthy)
      refute "team_1" in result.degraded_teams
      # 3 storages > 2 quorum (degraded)
      assert "team_2" in result.degraded_teams
    end
  end

  describe "determine_durable_version/3" do
    test "returns minimum version across all healthy teams" do
      teams = [
        %{tag: "team_1", storage_ids: ["s1", "s2", "s3"]},
        %{tag: "team_2", storage_ids: ["s4", "s5", "s6"]}
      ]

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102},
        "s3" => %{durable_version: 101},
        "s4" => %{durable_version: 98},
        "s5" => %{durable_version: 99},
        "s6" => %{durable_version: 97}
      }

      {:ok, durable_version, _healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == 98
      # Both teams have 3 storages but quorum is 2, so both are degraded
      assert length(degraded_teams) == 2
    end

    test "returns error when team has insufficient replication" do
      teams = [
        # Only 1 storage
        %{tag: "team_1", storage_ids: ["s1"]},
        %{tag: "team_2", storage_ids: ["s2", "s3", "s4"]}
      ]

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 98},
        "s3" => %{durable_version: 99},
        "s4" => %{durable_version: 97}
      }

      {:error, {:insufficient_replication, failed_teams}} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      assert "team_1" in failed_teams
    end

    test "correctly categorizes healthy vs degraded teams" do
      teams = [
        # 3 storages > quorum 2 = degraded
        %{tag: "degraded", storage_ids: ["s1", "s2", "s3"]},
        # 2 storages == quorum 2 = healthy
        %{tag: "healthy", storage_ids: ["s4", "s5"]}
      ]

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102},
        "s3" => %{durable_version: 101},
        "s4" => %{durable_version: 98},
        "s5" => %{durable_version: 99}
      }

      {:ok, durable_version, healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == 98
      assert "healthy" in healthy_teams
      assert "degraded" in degraded_teams
    end

    test "handles missing storage info gracefully" do
      teams = [
        %{tag: "team_1", storage_ids: ["s1", "s2", "missing"]}
      ]

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102}
        # "missing" not in info_by_id
      }

      {:ok, durable_version, _healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # From s1 and s2, quorum of 2
      assert durable_version == 100
      # 2 available == quorum 2 (healthy)
      assert degraded_teams == []
    end
  end

  describe "smallest_version/2" do
    test "returns second argument when first is nil" do
      assert VersionDeterminationPhase.smallest_version(nil, 100) == 100
    end

    test "returns minimum of two versions" do
      assert VersionDeterminationPhase.smallest_version(150, 100) == 100
      assert VersionDeterminationPhase.smallest_version(50, 100) == 50
    end

    test "handles equal versions" do
      assert VersionDeterminationPhase.smallest_version(100, 100) == 100
    end
  end

  describe "determine_durable_version_and_status_for_storage_team/3" do
    test "returns degraded status when team has more than quorum storages" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "s3"]}

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102},
        "s3" => %{durable_version: 101}
      }

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest version (102, 101, 100) -> 101
      assert version == 101
      # 3 storages > 2 quorum
      assert status == :degraded
    end

    test "returns healthy status when team has exactly quorum storages" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2"]}

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102}
      }

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest version (102, 100) -> 100
      assert version == 100
      # 2 storages == 2 quorum
      assert status == :healthy
    end

    test "returns error when team has insufficient storages" do
      team = %{tag: "team_1", storage_ids: ["s1"]}

      info_by_id = %{
        "s1" => %{durable_version: 100}
      }

      {:error, :insufficient_replication} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )
    end

    test "handles missing storage info" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "missing"]}

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102}
        # "missing" not in info_by_id
      }

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest from available (102, 100) -> 100
      assert version == 100
      # 2 available == 2 quorum
      assert status == :healthy
    end

    test "calculates quorum version correctly with different scenarios" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2", "s3", "s4", "s5"]}

      info_by_id = %{
        "s1" => %{durable_version: 100},
        "s2" => %{durable_version: 102},
        "s3" => %{durable_version: 101},
        "s4" => %{durable_version: 103},
        "s5" => %{durable_version: 99}
      }

      # Quorum of 3 means we take the 3rd highest version
      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          3
        )

      # Sorted: [99, 100, 101, 102, 103] -> 3rd from end is 101
      assert version == 101
      # 5 storages > 3 quorum
      assert status == :degraded
    end
  end
end
