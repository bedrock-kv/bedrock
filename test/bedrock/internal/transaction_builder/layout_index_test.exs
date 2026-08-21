defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  test "an empty index misses" do
    assert LayoutIndex.lookup_key(LayoutIndex.new(), "a") == :not_cached
  end

  test "a covering entry answers keys in [start, end) and only those" do
    index = LayoutIndex.insert(LayoutIndex.new(), "m", "z", [:ref])

    assert LayoutIndex.lookup_key(index, "m") == {:ok, {{"m", "z"}, [:ref]}}
    assert LayoutIndex.lookup_key(index, "pear") == {:ok, {{"m", "z"}, [:ref]}}
    assert LayoutIndex.lookup_key(index, "z") == :not_cached
    assert LayoutIndex.lookup_key(index, "apple") == :not_cached
  end

  test "a key in a gap between fetched entries is an honest miss — the index is partial" do
    # Non-adjacent fetches: [a, c) and [m, z). Keys between them must not
    # be answered by either neighbor.
    index =
      LayoutIndex.new()
      |> LayoutIndex.insert("m", "z", [:right])
      |> LayoutIndex.insert("a", "c", [:left])

    assert LayoutIndex.lookup_key(index, "b") == {:ok, {{"a", "c"}, [:left]}}
    assert LayoutIndex.lookup_key(index, "g") == :not_cached
    assert LayoutIndex.lookup_key(index, "pear") == {:ok, {{"m", "z"}, [:right]}}
  end

  test "re-inserting a shard is idempotent and refreshes the value" do
    index =
      LayoutIndex.new()
      |> LayoutIndex.insert("m", "z", [:stale])
      |> LayoutIndex.insert("m", "z", [:fresh])

    assert LayoutIndex.lookup_key(index, "pear") == {:ok, {{"m", "z"}, [:fresh]}}
  end

  test "the end-of-keyspace sentinel is inclusive at its own bound" do
    index = LayoutIndex.insert(LayoutIndex.new(), "m", <<0xFF, 0xFF>>, [:ref])

    assert LayoutIndex.lookup_key(index, <<0xFF, 0xFF>>) == {:ok, {{"m", <<0xFF, 0xFF>>}, [:ref]}}
  end
end
