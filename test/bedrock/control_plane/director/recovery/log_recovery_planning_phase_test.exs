defmodule Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhaseTest do
  use ExUnit.Case, async: true
  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase
  alias Bedrock.DataPlane.Version

  describe "execute/1" do
    test "successfully determines logs to copy and advances state" do
      recovery_attempt =
        recovery_attempt()
        |> with_log_recovery_info(%{
          {:log, 1} => %{
            oldest_version: Version.from_integer(10),
            last_version: Version.from_integer(50)
          },
          {:log, 2} => %{
            oldest_version: Version.from_integer(5),
            last_version: Version.from_integer(45)
          }
        })

      # Quorum = 2
      {result, next_phase} =
        LogRecoveryPlanningPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{
            logs: %{
              {:log, 1} => ["tag_a"],
              {:log, 2} => ["tag_b"]
            },
            storage_teams: []
          },
          cluster_config: %{
            # Quorum = 2, with two logs
            parameters: %{desired_logs: 3},
            transaction_system_layout: %{
              logs: %{
                {:log, 1} => ["tag_a"],
                {:log, 2} => ["tag_b"]
              }
            }
          }
        })

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase
      assert is_list(result.old_log_ids_to_copy)
      assert is_tuple(result.version_vector)
      assert length(result.old_log_ids_to_copy) > 0
    end

    test "stalls recovery when unable to meet log quorum" do
      recovery_attempt = %RecoveryAttempt{
        log_recovery_info_by_id: %{}
      }

      {_result, next_phase_or_stall} =
        LogRecoveryPlanningPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{logs: %{}, storage_teams: []},
          cluster_config: %{
            # Quorum = 2, but no logs
            parameters: %{desired_logs: 3},
            transaction_system_layout: %{
              # No logs available
              logs: %{}
            }
          }
        })

      assert {:stalled, :unable_to_meet_log_quorum} = next_phase_or_stall
    end

    test "handles single log with quorum of 1" do
      recovery_attempt = %RecoveryAttempt{
        log_recovery_info_by_id: %{
          {:log, 1} => %{
            oldest_version: Version.from_integer(10),
            last_version: Version.from_integer(50)
          }
        }
      }

      {result, next_phase} =
        LogRecoveryPlanningPhase.execute(recovery_attempt, %{
          node_tracking: nil,
          old_transaction_system_layout: %{
            logs: %{
              {:log, 1} => ["tag_a"]
            },
            storage_teams: []
          },
          cluster_config: %{
            # Quorum = 1
            parameters: %{desired_logs: 1},
            transaction_system_layout: %{
              logs: %{
                {:log, 1} => ["tag_a"]
              }
            }
          }
        })

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase
      assert result.old_log_ids_to_copy == [{:log, 1}]
      assert result.version_vector == {Version.from_integer(10), Version.from_integer(50)}
    end
  end

  describe "determine_old_logs_to_copy/3" do
    test "returns error for empty logs list" do
      result = LogRecoveryPlanningPhase.determine_old_logs_to_copy([], %{}, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end

    test "successfully determines logs for quorum of 1" do
      old_logs = %{
        {:log, 1} => ["tag_a"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        }
      }

      {:ok, log_ids, version_vector} =
        LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 1)

      assert log_ids == [{:log, 1}]
      assert version_vector == {Version.from_integer(10), Version.from_integer(50)}
    end

    test "successfully determines logs for quorum of 2" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(45)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(55)
        }
      }

      {:ok, log_ids, version_vector} =
        LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Shard-aware algorithm includes all available shards
      assert length(log_ids) == 3
      assert Enum.all?([{:log, 1}, {:log, 2}, {:log, 3}], fn id -> id in log_ids end)
      # Cross-shard minimum: max(10,5,15)=15, min(50,45,55)=45
      assert version_vector == {Version.from_integer(15), Version.from_integer(45)}
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
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(45)
        }
        # {:log, 3} has no recovery info
      }

      # Shard-aware algorithm requires all shards to participate
      # Since shard "tag_c" has no recovery info, it fails
      result = LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
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
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(55)
        }
        # {:log, 2} has no recovery info
      }

      result = LogRecoveryPlanningPhase.recovery_info_for_logs(logs, recovery_info_by_id)

      expected = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(55)
        }
      }

      assert result == expected
    end

    test "returns empty map when no logs have recovery info" do
      logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"]
      }

      recovery_info_by_id = %{}

      result = LogRecoveryPlanningPhase.recovery_info_for_logs(logs, recovery_info_by_id)
      assert result == %{}
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
      assert Enum.sort(result) == Enum.sort(expected)
    end

    test "handles combinations of size 1" do
      result = LogRecoveryPlanningPhase.combinations([1, 2, 3], 1)
      expected = [[1], [2], [3]]
      assert Enum.sort(result) == Enum.sort(expected)
    end

    test "handles combinations of size 0" do
      result = LogRecoveryPlanningPhase.combinations([1, 2, 3], 0)
      assert result == [[]]
    end

    test "handles empty list" do
      result = LogRecoveryPlanningPhase.combinations([], 2)
      assert result == []
    end

    test "handles combinations larger than list size" do
      result = LogRecoveryPlanningPhase.combinations([1, 2], 3)
      assert result == []
    end
  end

  describe "version_vectors_by_id/1" do
    test "converts recovery info to version vectors" do
      log_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(45)
        }
      }

      result = LogRecoveryPlanningPhase.version_vectors_by_id(log_info)

      expected = [
        {{:log, 1}, {Version.from_integer(10), Version.from_integer(50)}},
        {{:log, 2}, {Version.from_integer(5), Version.from_integer(45)}}
      ]

      assert Enum.sort(result) == Enum.sort(expected)
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

      result =
        LogRecoveryPlanningPhase.build_log_groups_and_vectors_from_combinations(combinations)

      assert length(result) == 2

      # Check first group: max(10, 5) = 10, min(50, 45) = 45
      {log_ids_1, {oldest_1, newest_1}} = Enum.at(result, 0)
      assert Enum.sort(log_ids_1) == Enum.sort([{:log, 1}, {:log, 2}])
      assert oldest_1 == Version.from_integer(10)
      assert newest_1 == Version.from_integer(45)

      # Check second group: max(10, 15) = 15, min(50, 55) = 50
      {log_ids_2, {oldest_2, newest_2}} = Enum.at(result, 1)
      assert Enum.sort(log_ids_2) == Enum.sort([{:log, 1}, {:log, 3}])
      assert oldest_2 == Version.from_integer(15)
      assert newest_2 == Version.from_integer(50)
    end

    test "filters out invalid ranges" do
      combinations = [
        # newest < oldest (invalid)
        [{{:log, 1}, {Version.from_integer(50), Version.from_integer(10)}}],
        # valid
        [{{:log, 2}, {Version.from_integer(10), Version.from_integer(50)}}]
      ]

      result =
        LogRecoveryPlanningPhase.build_log_groups_and_vectors_from_combinations(combinations)

      assert length(result) == 1
      {log_ids, {oldest, newest}} = List.first(result)
      assert log_ids == [{:log, 2}]
      assert oldest == Version.from_integer(10)
      assert newest == Version.from_integer(50)
    end
  end

  describe "valid_range?/1" do
    test "allows valid ranges where newest >= oldest" do
      assert LogRecoveryPlanningPhase.valid_range?(
               {[], {Version.from_integer(10), Version.from_integer(50)}}
             ) == true

      assert LogRecoveryPlanningPhase.valid_range?(
               {[], {Version.from_integer(10), Version.from_integer(10)}}
             ) == true
    end

    test "rejects invalid ranges where newest < oldest" do
      assert LogRecoveryPlanningPhase.valid_range?(
               {[], {Version.from_integer(50), Version.from_integer(10)}}
             ) == false
    end

    test "handles special case with oldest = 0" do
      assert LogRecoveryPlanningPhase.valid_range?(
               {[], {Version.zero(), Version.from_integer(50)}}
             ) == true

      assert LogRecoveryPlanningPhase.valid_range?({[], {Version.zero(), Version.zero()}}) == true
    end

    test "handles special case with newest = 0" do
      assert LogRecoveryPlanningPhase.valid_range?(
               {[], {Version.from_integer(10), Version.zero()}}
             ) == false

      assert LogRecoveryPlanningPhase.valid_range?({[], {Version.zero(), Version.zero()}}) == true
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
      old_logs = %{
        {:log, 1} => ["tag_a"],
        # Same shard as log 1
        {:log, 2} => ["tag_a"],
        {:log, 3} => ["tag_b"],
        # Same shard as log 3
        {:log, 4} => ["tag_b"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(45)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(60)
        },
        {:log, 4} => %{
          oldest_version: Version.from_integer(20),
          last_version: Version.from_integer(55)
        }
      }

      {:ok, log_ids, version_vector} =
        LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Should include logs from both shards
      assert length(log_ids) == 4
      assert Enum.all?([{:log, 1}, {:log, 2}, {:log, 3}, {:log, 4}], fn id -> id in log_ids end)

      # Version vector should be cross-shard minimum of maximums
      # Shard A: max(10,15) = 15, min(50,45) = 45 -> {15, 45}
      # Shard B: max(5,20) = 20, min(60,55) = 55 -> {Version.from_integer(20), Version.from_integer(55)}
      # Cross-shard: max(15,20) = 20, min(45,55) = 45
      assert version_vector == {Version.from_integer(20), Version.from_integer(45)}
    end

    test "uses shard-aware algorithm universally for all scenarios" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_b"],
        {:log, 3} => ["tag_c"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(45)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(55)
        }
      }

      {:ok, log_ids, version_vector} =
        LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Shard-aware algorithm includes all shards and provides conservative minimum
      assert length(log_ids) == 3
      assert Enum.all?([{:log, 1}, {:log, 2}, {:log, 3}], fn id -> id in log_ids end)
      # Conservative minimum for storage lag tolerance
      assert version_vector == {Version.from_integer(15), Version.from_integer(45)}
    end

    test "handles logs participating in multiple shards" do
      old_logs = %{
        # Participates in both shards
        {:log, 1} => ["tag_a", "tag_b"],
        {:log, 2} => ["tag_a"],
        {:log, 3} => ["tag_b"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(45)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(55)
        }
      }

      {:ok, log_ids, _version_vector} =
        LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)

      # Should include all logs since both shards have multiple logs
      assert length(log_ids) == 3
      assert Enum.all?([{:log, 1}, {:log, 2}, {:log, 3}], fn id -> id in log_ids end)
    end

    test "fails when shard cannot meet quorum" do
      old_logs = %{
        {:log, 1} => ["tag_a"],
        {:log, 2} => ["tag_a"],
        {:log, 3} => ["tag_b"],
        {:log, 4} => ["tag_b"],
        # Shard B has 3 logs, needs 2 for quorum
        {:log, 5} => ["tag_b"]
      }

      recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(45)
        },
        {:log, 3} => %{
          oldest_version: Version.from_integer(5),
          last_version: Version.from_integer(40)
        }
        # logs 4 and 5 have no recovery info - shard B cannot meet quorum (1 < 2)
      }

      result = LogRecoveryPlanningPhase.determine_old_logs_to_copy(old_logs, recovery_info, 2)
      assert result == {:error, :unable_to_meet_log_quorum}
    end
  end

  describe "shard grouping functions" do
    test "group_logs_by_shard groups logs correctly" do
      log_recovery_info = %{
        {:log, 1} => %{
          oldest_version: Version.from_integer(10),
          last_version: Version.from_integer(50)
        },
        {:log, 2} => %{
          oldest_version: Version.from_integer(15),
          last_version: Version.from_integer(45)
        }
      }

      old_logs = %{
        {:log, 1} => ["tag_a", "tag_b"],
        {:log, 2} => ["tag_a"]
      }

      result = LogRecoveryPlanningPhase.group_logs_by_shard(log_recovery_info, old_logs)

      expected = %{
        "tag_a" => [
          {{:log, 2},
           %{oldest_version: Version.from_integer(15), last_version: Version.from_integer(45)}},
          {{:log, 1},
           %{oldest_version: Version.from_integer(10), last_version: Version.from_integer(50)}}
        ],
        "tag_b" => [
          {{:log, 1},
           %{oldest_version: Version.from_integer(10), last_version: Version.from_integer(50)}}
        ]
      }

      # Check structure (order may vary due to list insertion)
      assert Map.keys(result) == Map.keys(expected)
      assert length(result["tag_a"]) == 2
      assert length(result["tag_b"]) == 1
    end
  end
end
