defmodule Bedrock.Cluster.Link.RoutingCacheTest do
  @moduledoc """
  The node-wide routing cache, readable WITHOUT a message.

  It was a `LayoutIndex` held in the Link's state, so every cached lookup
  cost a `GenServer.call` into one process — 0.79µs at rest, and a
  serialization point that every transaction on the node funnels through.
  An ETS read of the same entry is 0.06µs, and gets FASTER under
  concurrency (0.04µs at 16-way) where the call flattens.

  This is FDB's shape: `DatabaseContext locationCache` is a plain
  in-memory structure the calling context reads directly, not a server it
  asks.

  Semantics must not drift from `LayoutIndex`, so the tests below check
  the two against each other rather than restating the rules — including
  the one that is easy to lose: an end key of `<<0xFF, 0xFF>>` is
  INCLUSIVE, because it is the end-of-keyspace sentinel rather than a
  real boundary.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Link.RoutingCache
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  setup do
    table = :"routing_cache_test_#{:erlang.unique_integer([:positive])}"
    RoutingCache.new(table)
    on_exit(fn -> :ets.info(table) != :undefined && :ets.delete(table) end)
    {:ok, table: table}
  end

  # The same ranges loaded into both structures.
  defp load(table, ranges) do
    Enum.reduce(ranges, LayoutIndex.new(), fn {s, e, ref}, index ->
      RoutingCache.insert(table, s, e, ref)
      LayoutIndex.insert(index, s, e, ref)
    end)
  end

  defp agree?(table, index, key) do
    RoutingCache.lookup(table, key) == LayoutIndex.lookup_key(index, key)
  end

  describe "agreement with LayoutIndex" do
    test "adjacent ranges, keys inside, on the boundaries, and outside", %{table: table} do
      ranges = [{"a", "d", :r1}, {"d", "m", :r2}, {"m", "z", :r3}]
      index = load(table, ranges)

      for key <- ~w[a b c d e l m n y z A "" zz] do
        assert agree?(table, index, key), "disagreed on #{inspect(key)}"
      end
    end

    test "the end-of-keyspace sentinel is INCLUSIVE, in both", %{table: table} do
      # <<0xFF,0xFF>> is the sentinel, not a boundary: a key equal to it
      # still lands in the final range. Losing this makes system-keyspace
      # reads uncacheable.
      ranges = [{<<0xFF>>, <<0xFF, 0xFF>>, :system}]
      index = load(table, ranges)

      assert {:ok, {{_, _}, :system}} = RoutingCache.lookup(table, <<0xFF, 0xFF>>)
      assert agree?(table, index, <<0xFF, 0xFF>>)
      assert agree?(table, index, <<0xFF, 0x00>>)
      assert agree?(table, index, <<0xFE>>)
    end

    test "a gap between ranges is a miss in both", %{table: table} do
      ranges = [{"a", "d", :r1}, {"m", "z", :r3}]
      index = load(table, ranges)

      for key <- ~w[d e f l] do
        assert RoutingCache.lookup(table, key) == :not_cached
        assert agree?(table, index, key)
      end
    end

    test "an empty cache misses everything", %{table: table} do
      index = LayoutIndex.new()
      for key <- ~w[a m z], do: assert(agree?(table, index, key))
    end

    test "re-inserting the same range replaces its ref, in both", %{table: table} do
      index = load(table, [{"a", "z", :old}])
      index = load(table, [{"a", "z", :new}])

      assert {:ok, {_, :new}} = RoutingCache.lookup(table, "m")
      assert agree?(table, index, "m")
    end

    test "randomized ranges and probes agree", %{table: table} do
      # Differential fuzzing: the two must be indistinguishable.
      ranges =
        for i <- 0..19 do
          s = <<i>>
          {s, <<i + 1>>, {:shard, i}}
        end

      index = load(table, Enum.shuffle(ranges))

      for _ <- 1..500 do
        key = <<:rand.uniform(24) - 1>>
        assert agree?(table, index, key), "disagreed on #{inspect(key)}"
      end
    end
  end

  describe "clear/1" do
    test "drops every entry", %{table: table} do
      load(table, [{"a", "z", :r1}])
      assert {:ok, _} = RoutingCache.lookup(table, "m")

      RoutingCache.clear(table)
      assert RoutingCache.lookup(table, "m") == :not_cached
    end
  end

  describe "readable without a message" do
    test "a lookup from another process needs no call into the owner", %{table: table} do
      # The whole point: the reader is not the owner and does not message it.
      load(table, [{"a", "z", :r1}])

      task = Task.async(fn -> RoutingCache.lookup(table, "m") end)
      assert {:ok, {_, :r1}} = Task.await(task)
    end
  end

  describe "a missing table" do
    test "is a MISS, not a crash" do
      # If the Link has not started (or restarted), the table is gone. A
      # dead cache must be no worse than an empty one: the caller falls
      # back to asking a proxy and the node warms up again.
      assert RoutingCache.lookup(:no_such_routing_table, "k") == :not_cached
      assert RoutingCache.lookup(:no_such_routing_table, <<0xFF, 0xFF>>) == :not_cached
    end
  end
end
