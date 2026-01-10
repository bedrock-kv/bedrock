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

      # Shard ranges are [min, max) - start inclusive, end exclusive
      # Tag 0 covers ["", "m"), tag 1 covers ["m", \xff)
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
      # With [min, max) semantics: "m" is the START of tag 1's range, not end of tag 0
      # Tag 0 covers ["", "m"), tag 1 covers ["m", \xff)
      assert ShardRouter.lookup_shard(table, "m") == 1
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
        # Single shard covers ["", \xff)
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
        # 5 shards with [min, max) ranges:
        # Tag 0: ["", "b"), Tag 1: ["b", "d"), Tag 2: ["d", "f"),
        # Tag 3: ["f", "h"), Tag 4: ["h", \xff)
        :ets.insert(table, {"b", 0})
        :ets.insert(table, {"d", 1})
        :ets.insert(table, {"f", 2})
        :ets.insert(table, {"h", 3})
        :ets.insert(table, {"\xff", 4})

        assert ShardRouter.lookup_shard(table, "a") == 0
        # "b" is START of tag 1's range
        assert ShardRouter.lookup_shard(table, "b") == 1
        assert ShardRouter.lookup_shard(table, "c") == 1
        # "d" is START of tag 2's range
        assert ShardRouter.lookup_shard(table, "d") == 2
        assert ShardRouter.lookup_shard(table, "e") == 2
        assert ShardRouter.lookup_shard(table, "g") == 3
        assert ShardRouter.lookup_shard(table, "z") == 4
      after
        :ets.delete(table)
      end
    end
  end

  describe "lookup_shards_for_range/3 - range to tags" do
    setup do
      table = :ets.new(:range_test, [:ordered_set, :public])
      # 4 shards with [min, max) ranges:
      # Tag 0: ["", "d"), Tag 1: ["d", "h"), Tag 2: ["h", "m"), Tag 3: ["m", \xff)
      :ets.insert(table, {"d", 0})
      :ets.insert(table, {"h", 1})
      :ets.insert(table, {"m", 2})
      :ets.insert(table, {"\xff", 3})

      on_exit(fn ->
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end)

      {:ok, table: table}
    end

    test "range within single shard returns one tag", %{table: table} do
      # ["a", "c") is entirely within tag 0's range ["", "d")
      assert ShardRouter.lookup_shards_for_range(table, "a", "c") == [0]
    end

    test "range spanning two shards returns both tags", %{table: table} do
      # ["c", "f") spans tag 0 ["", "d") and tag 1 ["d", "h")
      tags = ShardRouter.lookup_shards_for_range(table, "c", "f")
      assert Enum.sort(tags) == [0, 1]
    end

    test "range spanning all shards returns all tags", %{table: table} do
      # ["", \xff) spans all shards
      tags = ShardRouter.lookup_shards_for_range(table, "", "\xff")
      assert Enum.sort(tags) == [0, 1, 2, 3]
    end

    test "range at exact boundary", %{table: table} do
      # ["d", "h") exactly matches tag 1's range
      # With [min, max): "d" is the START of tag 1, "h" is the END (exclusive)
      tags = ShardRouter.lookup_shards_for_range(table, "d", "h")
      assert tags == [1]
    end

    test "empty range returns single shard", %{table: table} do
      # ["e", "e") is empty but we still return the shard containing "e"
      tags = ShardRouter.lookup_shards_for_range(table, "e", "e")
      # "e" is in tag 1's range ["d", "h")
      assert tags == [1]
    end

    test "range in last shard", %{table: table} do
      # ["z", \xff) is entirely in tag 3's range ["m", \xff)
      tags = ShardRouter.lookup_shards_for_range(table, "z", "\xff")
      assert tags == [3]
    end

    test "range crossing multiple boundaries", %{table: table} do
      # ["c", "i") spans tags 0, 1, and 2
      # Tag 0: ["", "d"), Tag 1: ["d", "h"), Tag 2: ["h", "m")
      tags = ShardRouter.lookup_shards_for_range(table, "c", "i")
      assert Enum.sort(tags) == [0, 1, 2]
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

  describe "lazy log_indices caching" do
    setup do
      table = :ets.new(:caching_test, [:ordered_set, :public])
      :ets.insert(table, {"m", 0})
      :ets.insert(table, {"\xff", 1})

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

    test "get_logs_for_key caches log_indices in ETS", %{table: table, log_map: log_map} do
      # Initial entry is just {end_key, tag}
      assert :ets.lookup(table, "m") == [{"m", 0}]

      # First call computes and caches
      _logs = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)

      # Entry should now be {end_key, {tag, log_indices}}
      [{_end_key, cached_value}] = :ets.lookup(table, "m")
      assert is_tuple(cached_value)
      {tag, log_indices} = cached_value
      assert tag == 0
      assert is_list(log_indices)
      assert length(log_indices) == 2
    end

    test "subsequent calls use cached log_indices", %{table: table, log_map: log_map} do
      # First call - computes
      logs1 = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)

      # Verify it's cached
      [{_, {0, cached_indices}}] = :ets.lookup(table, "m")

      # Second call - uses cache
      logs2 = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)

      # Results should be identical
      assert logs1 == logs2

      # Cache should be unchanged
      [{_, {0, ^cached_indices}}] = :ets.lookup(table, "m")
    end

    test "lookup_shard handles cached format", %{table: table, log_map: log_map} do
      # Trigger caching
      _logs = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)

      # lookup_shard should still return just the tag
      tag = ShardRouter.lookup_shard(table, "apple")
      assert tag == 0
    end

    test "lookup_shards_for_range handles cached format", %{table: table, log_map: log_map} do
      # Trigger caching for both shards
      _logs1 = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)
      _logs2 = ShardRouter.get_logs_for_key(table, "zebra", log_map, 2)

      # Range lookup should still work
      tags = ShardRouter.lookup_shards_for_range(table, "a", "z")
      assert Enum.sort(tags) == [0, 1]
    end

    test "works with pre-cached entries", %{table: table, log_map: log_map} do
      # Manually insert cached format
      :ets.insert(table, {"m", {0, [1, 2]}})

      # Should use cached indices
      logs = ShardRouter.get_logs_for_key(table, "apple", log_map, 2)
      assert logs == ["log-b", "log-c"]
    end
  end
end
