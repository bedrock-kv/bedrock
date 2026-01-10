defmodule Bedrock.DataPlane.ShardRouterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.ShardRouter

  describe "get_log_indices/3 - golden ratio log selection" do
    test "returns empty list when replication factor is 0" do
      assert ShardRouter.get_log_indices(0, 5, 0) == []
      assert ShardRouter.get_log_indices(42, 10, 0) == []
    end

    test "returns single index when replication factor is 1" do
      indices = ShardRouter.get_log_indices(0, 5, 1)
      assert length(indices) == 1
      assert hd(indices) in 0..4
    end

    test "returns correct number of indices for replication factor" do
      for m <- 1..5 do
        indices = ShardRouter.get_log_indices(0, 10, m)
        assert length(indices) == m, "Expected #{m} indices, got #{length(indices)}"
      end
    end

    test "all indices are unique (no duplicates)" do
      for tag <- 0..20, n <- 3..10, m <- 1..min(n, 5) do
        indices = ShardRouter.get_log_indices(tag, n, m)

        assert length(Enum.uniq(indices)) == length(indices),
               "Duplicate indices for tag=#{tag}, n=#{n}, m=#{m}: #{inspect(indices)}"
      end
    end

    test "all indices are within valid range [0, n)" do
      for tag <- 0..20, n <- 3..10, m <- 1..min(n, 5) do
        indices = ShardRouter.get_log_indices(tag, n, m)

        for idx <- indices do
          assert idx >= 0 and idx < n,
                 "Index #{idx} out of range [0, #{n}) for tag=#{tag}"
        end
      end
    end

    test "different tags produce different distributions" do
      n = 10
      m = 3

      distributions =
        for tag <- 0..9 do
          tag |> ShardRouter.get_log_indices(n, m) |> Enum.sort()
        end

      # Not all distributions should be identical
      unique_distributions = Enum.uniq(distributions)

      assert length(unique_distributions) > 1,
             "All tags produced identical distributions"
    end

    test "deterministic - same inputs produce same outputs" do
      for _ <- 1..10 do
        assert ShardRouter.get_log_indices(42, 10, 3) ==
                 ShardRouter.get_log_indices(42, 10, 3)
      end
    end

    test "good distribution across logs" do
      # With many tags, each log should be selected roughly equally
      n = 5
      m = 2
      num_tags = 100

      log_counts =
        for tag <- 0..(num_tags - 1), reduce: %{} do
          acc ->
            indices = ShardRouter.get_log_indices(tag, n, m)

            Enum.reduce(indices, acc, fn idx, a ->
              Map.update(a, idx, 1, &(&1 + 1))
            end)
        end

      # Each log should be selected at least some times
      for log_idx <- 0..(n - 1) do
        count = Map.get(log_counts, log_idx, 0)
        # With 100 tags * 2 replicas = 200 selections across 5 logs,
        # each log should get roughly 40. Allow wide variance.
        assert count > 10,
               "Log #{log_idx} only selected #{count} times out of #{num_tags * m}"
      end
    end
  end

  describe "lookup_shard/2 - ETS ceiling search" do
    setup do
      # Create ETS table with shard_keys
      table = :ets.new(:test_shard_keys, [:ordered_set, :public])

      # Insert shard boundaries: tag 0 covers "" to "m", tag 1 covers "m" to \xff
      :ets.insert(table, {"m", 0})
      :ets.insert(table, {"\xff", 1})

      on_exit(fn ->
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end)

      {:ok, table: table}
    end

    test "finds correct shard for key before first boundary", %{table: table} do
      assert ShardRouter.lookup_shard(table, "a") == 0
      assert ShardRouter.lookup_shard(table, "hello") == 0
      assert ShardRouter.lookup_shard(table, "") == 0
      assert ShardRouter.lookup_shard(table, "lzzz") == 0
    end

    test "finds correct shard for key at boundary", %{table: table} do
      # "m" is the end_key for tag 0, so "m" belongs to tag 0
      assert ShardRouter.lookup_shard(table, "m") == 0
    end

    test "finds correct shard for key after first boundary", %{table: table} do
      assert ShardRouter.lookup_shard(table, "n") == 1
      assert ShardRouter.lookup_shard(table, "zebra") == 1
      assert ShardRouter.lookup_shard(table, "\xfe") == 1
    end

    test "handles system keys (metadata shard)", %{table: table} do
      # System keys start with \xff, which is >= "m", so they go to tag 1
      # But in real setup, there would be a shard_key entry for system keys
      assert ShardRouter.lookup_shard(table, "\xff/system/foo") == 1
    end
  end

  describe "lookup_shard/2 - edge cases" do
    test "single shard covering entire keyspace" do
      table = :ets.new(:single_shard, [:ordered_set, :public])

      try do
        :ets.insert(table, {"\xff", 0})

        assert ShardRouter.lookup_shard(table, "") == 0
        assert ShardRouter.lookup_shard(table, "any_key") == 0
        assert ShardRouter.lookup_shard(table, "\xfe") == 0
      after
        :ets.delete(table)
      end
    end

    test "many shards" do
      table = :ets.new(:many_shards, [:ordered_set, :public])

      try do
        # 5 shards with boundaries at "b", "d", "f", "h", "\xff"
        :ets.insert(table, {"b", 0})
        :ets.insert(table, {"d", 1})
        :ets.insert(table, {"f", 2})
        :ets.insert(table, {"h", 3})
        :ets.insert(table, {"\xff", 4})

        assert ShardRouter.lookup_shard(table, "a") == 0
        assert ShardRouter.lookup_shard(table, "b") == 0
        assert ShardRouter.lookup_shard(table, "c") == 1
        assert ShardRouter.lookup_shard(table, "e") == 2
        assert ShardRouter.lookup_shard(table, "g") == 3
        assert ShardRouter.lookup_shard(table, "z") == 4
      after
        :ets.delete(table)
      end
    end
  end

  describe "get_logs_for_key/4 - full routing" do
    setup do
      table = :ets.new(:routing_test, [:ordered_set, :public])
      :ets.insert(table, {"m", 0})
      :ets.insert(table, {"\xff", 1})

      # Log index to log_id mapping
      log_map = %{0 => "log-a", 1 => "log-b", 2 => "log-c"}

      on_exit(fn ->
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end)

      {:ok, table: table, log_map: log_map}
    end

    test "routes key to correct logs", %{table: table, log_map: log_map} do
      # Key "apple" -> tag 0 -> some logs
      logs = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)

      assert length(logs) == 2
      assert Enum.all?(logs, &is_binary/1)
      assert Enum.all?(logs, &(&1 in Map.values(log_map)))
    end

    test "different shards may route to different logs", %{table: table, log_map: log_map} do
      logs_shard_0 = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)
      logs_shard_1 = ShardRouter.get_logs_for_key(table, "zebra", log_map, 2)

      # They might be the same or different depending on golden ratio
      # Just verify they're valid
      assert length(logs_shard_0) == 2
      assert length(logs_shard_1) == 2
    end
  end
end
