defmodule Bedrock.DataPlane.ShardRouterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.RoutingData
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

  # Shards: %{end_key => {tag, start_key}}
  defp shards(shard_layout) do
    RoutingData.from_snapshot(%{
      shard_layout: shard_layout,
      log_map: %{},
      log_services: %{},
      replication_factor: 1
    }).shards
  end

  describe "lookup_shard/2 - ceiling search" do
    setup do
      # Shard ranges are [min, max) - start inclusive, end exclusive
      # Tag 0 covers ["", "m"), tag 1 covers ["m", \xff)
      {:ok, shards: shards(%{"m" => {0, ""}, "\xff" => {1, "m"}})}
    end

    test "finds correct shard for key before first boundary", %{shards: shards} do
      assert ShardRouter.lookup_shard(shards, "a") == 0
      assert ShardRouter.lookup_shard(shards, "hello") == 0
      assert ShardRouter.lookup_shard(shards, "") == 0
      assert ShardRouter.lookup_shard(shards, "lzzz") == 0
    end

    test "finds correct shard for key at boundary", %{shards: shards} do
      # With [min, max) semantics: "m" is the START of tag 1's range, not end of tag 0
      assert ShardRouter.lookup_shard(shards, "m") == 1
    end

    test "finds correct shard for key after first boundary", %{shards: shards} do
      assert ShardRouter.lookup_shard(shards, "n") == 1
      assert ShardRouter.lookup_shard(shards, "zebra") == 1
      assert ShardRouter.lookup_shard(shards, "\xfe") == 1
    end

    test "raises for a key beyond every boundary instead of misrouting", %{shards: shards} do
      # Ingress bounds keys below the last boundary; a key past it means the
      # map and keyspace diverged. The historical last-shard fallback was the
      # silent-misroute mechanism bedrock-rag closed - never bring it back.
      assert_raise RuntimeError, ~r/beyond all shard boundaries/, fn ->
        ShardRouter.lookup_shard(shards, "\xff/system/foo")
      end
    end

    test "raises on an empty shard map" do
      assert_raise RuntimeError, "Empty shard map", fn ->
        ShardRouter.lookup_shard(shards(%{}), "a")
      end
    end
  end

  describe "lookup_shard/2 - edge cases" do
    test "single shard covering entire keyspace" do
      single = shards(%{"\xff" => {0, ""}})

      assert ShardRouter.lookup_shard(single, "") == 0
      assert ShardRouter.lookup_shard(single, "any_key") == 0
      assert ShardRouter.lookup_shard(single, "\xfe") == 0
    end

    test "many shards" do
      # Tag 0: ["", "b"), Tag 1: ["b", "d"), Tag 2: ["d", "f"),
      # Tag 3: ["f", "h"), Tag 4: ["h", \xff)
      many =
        shards(%{
          "b" => {0, ""},
          "d" => {1, "b"},
          "f" => {2, "d"},
          "h" => {3, "f"},
          "\xff" => {4, "h"}
        })

      assert ShardRouter.lookup_shard(many, "a") == 0
      # "b" is START of tag 1's range
      assert ShardRouter.lookup_shard(many, "b") == 1
      assert ShardRouter.lookup_shard(many, "c") == 1
      # "d" is START of tag 2's range
      assert ShardRouter.lookup_shard(many, "d") == 2
      assert ShardRouter.lookup_shard(many, "e") == 2
      assert ShardRouter.lookup_shard(many, "g") == 3
      assert ShardRouter.lookup_shard(many, "z") == 4
    end
  end

  describe "lookup_shards_with_ranges/3 - range to tags with boundaries" do
    setup do
      # Tag 0: ["", "d"), Tag 1: ["d", "h"), Tag 2: ["h", "m"), Tag 3: ["m", \xff)
      {:ok,
       shards:
         shards(%{
           "d" => {0, ""},
           "h" => {1, "d"},
           "m" => {2, "h"},
           "\xff" => {3, "m"}
         })}
    end

    test "returns shard boundaries for single shard range", %{shards: shards} do
      # ["a", "c") is entirely within tag 0's range ["", "d")
      assert ShardRouter.lookup_shards_with_ranges(shards, "a", "c") == [{0, "", "d"}]
    end

    test "returns boundaries for range spanning two shards", %{shards: shards} do
      # ["c", "f") spans tag 0 ["", "d") and tag 1 ["d", "h")
      assert ShardRouter.lookup_shards_with_ranges(shards, "c", "f") == [{0, "", "d"}, {1, "d", "h"}]
    end

    test "returns boundaries for range spanning all shards", %{shards: shards} do
      assert ShardRouter.lookup_shards_with_ranges(shards, "", "\xff") ==
               [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}, {3, "m", "\xff"}]
    end

    test "returns boundaries at exact shard boundary", %{shards: shards} do
      # ["d", "h") exactly matches tag 1's range; a shard ending AT the range
      # start (exclusive end) does not overlap.
      assert ShardRouter.lookup_shards_with_ranges(shards, "d", "h") == [{1, "d", "h"}]
    end

    test "returns boundaries for range in last shard", %{shards: shards} do
      assert ShardRouter.lookup_shards_with_ranges(shards, "z", "\xff") == [{3, "m", "\xff"}]
    end

    test "returns boundaries for range crossing multiple boundaries", %{shards: shards} do
      assert ShardRouter.lookup_shards_with_ranges(shards, "c", "i") ==
               [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}]
    end

    test "an empty range returns the shard containing its point" do
      # Intent pin: split_mutation_by_shards treats an empty tagged list as a
      # coverage error, so an empty clear_range must still resolve to its
      # containing shard (the clamped result is a harmless no-op).
      shards = shards(%{"h" => {1, "d"}, "\xff" => {2, "h"}})

      assert ShardRouter.lookup_shards_with_ranges(shards, "e", "e") == [{1, "d", "h"}]
    end

    test "returns [] for range entirely beyond shard coverage", %{shards: shards} do
      # Last shard end_key is "\xff" (exclusive); ranges starting at or beyond
      # it intersect no shard.
      assert ShardRouter.lookup_shards_with_ranges(shards, "\xff", "\xff\x00") == []
      assert ShardRouter.lookup_shards_with_ranges(shards, <<0xFF, 0xFF>>, <<0xFF, 0xFF, 0>>) == []
    end

    test "enables correct clamping of clear_range", %{shards: shards} do
      # Test the use case: clamping clear_range "a" to "z" to shard boundaries
      clamped =
        shards
        |> ShardRouter.lookup_shards_with_ranges("a", "z")
        |> Enum.map(fn {tag, shard_start, shard_end} ->
          {tag, max("a", shard_start), min("z", shard_end)}
        end)

      assert clamped == [
               {0, "a", "d"},
               {1, "d", "h"},
               {2, "h", "m"},
               {3, "m", "z"}
             ]
    end
  end
end
