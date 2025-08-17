defmodule Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhaseTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase
  alias Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase
  alias Bedrock.DataPlane.Version

  describe "execute/1" do
    test "successfully determines durable version with healthy teams" do
      storage_teams = [
        %{tag: "team_1", storage_ids: ["storage_1", "storage_2", "storage_3"]},
        %{tag: "team_2", storage_ids: ["storage_4", "storage_5", "storage_6"]}
      ]

      storage_recovery_info = %{
        "storage_1" => %{
          durable_version: Version.from_integer(100),
          oldest_durable_version: Version.from_integer(50)
        },
        "storage_2" => %{
          durable_version: Version.from_integer(102),
          oldest_durable_version: Version.from_integer(50)
        },
        "storage_3" => %{
          durable_version: Version.from_integer(101),
          oldest_durable_version: Version.from_integer(50)
        },
        "storage_4" => %{
          durable_version: Version.from_integer(98),
          oldest_durable_version: Version.from_integer(40)
        },
        "storage_5" => %{
          durable_version: Version.from_integer(99),
          oldest_durable_version: Version.from_integer(40)
        },
        "storage_6" => %{
          durable_version: Version.from_integer(97),
          oldest_durable_version: Version.from_integer(40)
        }
      }

      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

      capture_log(fn ->
        {result, next_phase} =
          VersionDeterminationPhase.execute(recovery_attempt, %{
            node_tracking: nil,
            old_transaction_system_layout: %{storage_teams: storage_teams},
            cluster_config: %{
              parameters: %{desired_replication_factor: 3}
            }
          })

        assert next_phase == LogRecruitmentPhase
        assert result.durable_version == Version.from_integer(98)
        assert length(result.degraded_teams) == 2
      end)
    end

    test "handles insufficient replication and stalls recovery" do
      storage_teams = [
        # Only 1 storage, need quorum=2
        %{tag: "team_1", storage_ids: ["storage_1"]}
      ]

      storage_recovery_info = %{
        "storage_1" => %{
          durable_version: Version.from_integer(100),
          oldest_durable_version: Version.from_integer(50)
        }
      }

      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

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
        "storage_1" => %{
          durable_version: Version.from_integer(100),
          oldest_durable_version: Version.from_integer(50)
        },
        "storage_2" => %{
          durable_version: Version.from_integer(102),
          oldest_durable_version: Version.from_integer(50)
        },
        "storage_3" => %{
          durable_version: Version.from_integer(98),
          oldest_durable_version: Version.from_integer(40)
        },
        "storage_4" => %{
          durable_version: Version.from_integer(99),
          oldest_durable_version: Version.from_integer(40)
        },
        "storage_5" => %{
          durable_version: Version.from_integer(97),
          oldest_durable_version: Version.from_integer(40)
        }
      }

      recovery_attempt = with_storage_recovery_info(recovery_attempt(), storage_recovery_info)

      {result, next_phase} =
        VersionDeterminationPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{storage_teams: storage_teams},
          cluster_config: %{
            # Quorum = 2
            parameters: %{desired_replication_factor: 3}
          }
        })

      assert next_phase == LogRecruitmentPhase
      # min(100, 98) from team quorums
      assert result.durable_version == Version.from_integer(98)
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
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)},
        "s3" => %{durable_version: Version.from_integer(101)},
        "s4" => %{durable_version: Version.from_integer(98)},
        "s5" => %{durable_version: Version.from_integer(99)},
        "s6" => %{durable_version: Version.from_integer(97)}
      }

      {:ok, durable_version, _healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == Version.from_integer(98)
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
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(98)},
        "s3" => %{durable_version: Version.from_integer(99)},
        "s4" => %{durable_version: Version.from_integer(97)}
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
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)},
        "s3" => %{durable_version: Version.from_integer(101)},
        "s4" => %{durable_version: Version.from_integer(98)},
        "s5" => %{durable_version: Version.from_integer(99)}
      }

      {:ok, durable_version, healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # min(101, 98)
      assert durable_version == Version.from_integer(98)
      assert "healthy" in healthy_teams
      assert "degraded" in degraded_teams
    end

    test "handles missing storage info gracefully" do
      teams = [
        %{tag: "team_1", storage_ids: ["s1", "s2", "missing"]}
      ]

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)}
        # "missing" not in info_by_id
      }

      {:ok, durable_version, _healthy_teams, degraded_teams} =
        VersionDeterminationPhase.determine_durable_version(teams, info_by_id, 2)

      # From s1 and s2, quorum of 2
      assert durable_version == Version.from_integer(100)
      # 2 available == quorum 2 (healthy)
      assert degraded_teams == []
    end
  end

  describe "smallest_version/2" do
    test "returns non-nil value when first argument is nil" do
      v100 = Version.from_integer(100)
      assert VersionDeterminationPhase.smallest_version(nil, v100) == v100
    end

    test "returns non-nil value when second argument is nil" do
      v100 = Version.from_integer(100)
      assert VersionDeterminationPhase.smallest_version(v100, nil) == v100
    end

    test "returns nil when both arguments are nil" do
      assert VersionDeterminationPhase.smallest_version(nil, nil) == nil
    end

    test "returns minimum of two non-nil versions" do
      v50 = Version.from_integer(50)
      v100 = Version.from_integer(100)
      v150 = Version.from_integer(150)
      assert VersionDeterminationPhase.smallest_version(v150, v100) == v100
      assert VersionDeterminationPhase.smallest_version(v50, v100) == v50
    end

    test "returns same version when both arguments are equal" do
      v100 = Version.from_integer(100)
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

    test "preserves first version when starting accumulation from nil" do
      first_version = Version.from_integer(100)
      accumulator = nil

      result = VersionDeterminationPhase.smallest_version(first_version, accumulator)

      assert result == first_version
      assert result
    end

    test "maintains correctness across various version magnitudes" do
      very_small = Version.from_integer(1)
      small = Version.from_integer(50)
      medium = Version.from_integer(100)
      large = Version.from_integer(1000)

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

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest version (102, 101, 100) -> 101
      assert version == Version.from_integer(101)
      # 3 storages > 2 quorum
      assert status == :degraded
    end

    test "returns healthy status when team has exactly quorum storages" do
      team = %{tag: "team_1", storage_ids: ["s1", "s2"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)}
      }

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest version (102, 100) -> 100
      assert version == Version.from_integer(100)
      # 2 storages == 2 quorum
      assert status == :healthy
    end

    test "returns error when team has insufficient storages" do
      team = %{tag: "team_1", storage_ids: ["s1"]}

      info_by_id = %{
        "s1" => %{durable_version: Version.from_integer(100)}
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
        "s1" => %{durable_version: Version.from_integer(100)},
        "s2" => %{durable_version: Version.from_integer(102)}
        # "missing" not in info_by_id
      }

      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          2
        )

      # 2nd highest from available (102, 100) -> 100
      assert version == Version.from_integer(100)
      # 2 available == 2 quorum
      assert status == :healthy
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
      {:ok, version, status} =
        VersionDeterminationPhase.determine_durable_version_and_status_for_storage_team(
          team,
          info_by_id,
          3
        )

      # Sorted: [99, 100, 101, 102, 103] -> 3rd from end is 101
      assert version == Version.from_integer(101)
      # 5 storages > 3 quorum
      assert status == :degraded
    end
  end
end
