defmodule Bedrock.DataPlane.ShardRouterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Version

  @v0 Version.from_integer(0)

  describe "tombstone-aware lookups" do
    # Cleared boundaries stay in the table as {end_key, :deleted, version}
    # tombstones so a late write of an older version cannot resurrect them;
    # every navigation step must treat them as absent (bedrock-q67.24).
    setup do
      table = :ets.new(:tombstone_shards, [:ordered_set, :public])
      v1 = Version.from_integer(1)
      :ets.insert(table, {"d", 0, v1})
      :ets.insert(table, {"h", :deleted, v1})
      :ets.insert(table, {"m", 2, v1})
      :ets.insert(table, {<<0xFF>>, 3, v1})
      {:ok, table: table}
    end

    test "lookup_shard skips a tombstoned boundary", %{table: table} do
      # "h" is gone: ["d", "m") is one shard now, owned by tag 2.
      assert ShardRouter.lookup_shard(table, "e") == 2
      assert ShardRouter.lookup_shard(table, "a") == 0
    end

    test "the last-entry fallback skips trailing tombstones", %{table: table} do
      :ets.insert(table, {<<0xFF>>, :deleted, Version.from_integer(2)})

      assert ShardRouter.lookup_shard(table, <<0xFF, 0x01>>) == 2
    end

    test "lookup_shards_with_ranges treats tombstoned boundaries as absent", %{table: table} do
      assert ShardRouter.lookup_shards_with_ranges(table, "a", "z") ==
               [{0, "", "d"}, {2, "d", "m"}, {3, "m", <<0xFF>>}]
    end

    test "a range lying beyond all live boundaries intersects no shard", %{table: table} do
      :ets.insert(table, {<<0xFF>>, :deleted, Version.from_integer(2)})

      assert ShardRouter.lookup_shards_with_ranges(table, "n", "z") == []
    end
  end

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
      :ets.insert(table, {"m", 0, @v0})
      :ets.insert(table, {"\xff", 1, @v0})

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
        :ets.insert(table, {"\xff", 0, @v0})

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
        :ets.insert(table, {"b", 0, @v0})
        :ets.insert(table, {"d", 1, @v0})
        :ets.insert(table, {"f", 2, @v0})
        :ets.insert(table, {"h", 3, @v0})
        :ets.insert(table, {"\xff", 4, @v0})

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

  describe "lookup_shards_with_ranges/3 - range to tags with boundaries" do
    setup do
      table = :ets.new(:ranges_with_bounds, [:ordered_set, :public])
      # 4 shards with [min, max) ranges:
      # Tag 0: ["", "d"), Tag 1: ["d", "h"), Tag 2: ["h", "m"), Tag 3: ["m", \xff)
      :ets.insert(table, {"d", 0, @v0})
      :ets.insert(table, {"h", 1, @v0})
      :ets.insert(table, {"m", 2, @v0})
      :ets.insert(table, {"\xff", 3, @v0})

      on_exit(fn ->
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end)

      {:ok, table: table}
    end

    test "returns shard boundaries for single shard range", %{table: table} do
      # ["a", "c") is entirely within tag 0's range ["", "d")
      result = ShardRouter.lookup_shards_with_ranges(table, "a", "c")
      assert result == [{0, "", "d"}]
    end

    test "returns boundaries for range spanning two shards", %{table: table} do
      # ["c", "f") spans tag 0 ["", "d") and tag 1 ["d", "h")
      result = ShardRouter.lookup_shards_with_ranges(table, "c", "f")
      assert result == [{0, "", "d"}, {1, "d", "h"}]
    end

    test "returns boundaries for range spanning all shards", %{table: table} do
      # ["", \xff) spans all shards
      result = ShardRouter.lookup_shards_with_ranges(table, "", "\xff")
      assert result == [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}, {3, "m", "\xff"}]
    end

    test "returns boundaries at exact shard boundary", %{table: table} do
      # ["d", "h") exactly matches tag 1's range
      result = ShardRouter.lookup_shards_with_ranges(table, "d", "h")
      assert result == [{1, "d", "h"}]
    end

    test "returns boundaries for range in last shard", %{table: table} do
      # ["z", \xff) is entirely in tag 3's range ["m", \xff)
      result = ShardRouter.lookup_shards_with_ranges(table, "z", "\xff")
      assert result == [{3, "m", "\xff"}]
    end

    test "returns boundaries for range crossing multiple boundaries", %{table: table} do
      # ["c", "i") spans tags 0, 1, and 2
      result = ShardRouter.lookup_shards_with_ranges(table, "c", "i")
      assert result == [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}]
    end

    test "returns [] for range entirely beyond shard coverage", %{table: table} do
      # Last shard end_key is "\xff" (exclusive); ranges starting at or beyond
      # it intersect no shard. Regression: this used to raise ArgumentError via
      # :ets.prev(table, :"$end_of_table").
      assert ShardRouter.lookup_shards_with_ranges(table, "\xff", "\xff\x00") == []
      assert ShardRouter.lookup_shards_with_ranges(table, <<0xFF, 0xFF>>, <<0xFF, 0xFF, 0>>) == []
    end

    test "enables correct clamping of clear_range", %{table: table} do
      # Test the use case: clamping clear_range "a" to "z" to shard boundaries
      shards = ShardRouter.lookup_shards_with_ranges(table, "a", "z")

      # Use boundaries to clamp the original range
      clamped =
        Enum.map(shards, fn {tag, shard_start, shard_end} ->
          clamped_start = max("a", shard_start)
          clamped_end = min("z", shard_end)
          {tag, clamped_start, clamped_end}
        end)

      # Verify clamped ranges
      assert clamped == [
               {0, "a", "d"},
               {1, "d", "h"},
               {2, "h", "m"},
               {3, "m", "z"}
             ]
    end
  end
end
