defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.InitializationPhase
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  for order <- [:forward, :reverse] do
    @order order
    test "the fresh cluster's touching shards stay distinct in #{@order} fetch order (gh-141)" do
      attempt = RecoveryAttempt.new(Bedrock.Cluster, 1, DateTime.utc_now())
      {initialized, _next_phase} = InitializationPhase.execute(attempt, %{cluster_config: Config.new([node()])})

      # Use recovery's actual default layout so a copied fixture cannot drift.
      # Different refs make routing to the wrong side of the boundary visible.
      refs = %{0 => [{:metadata_materializer, node()}], 1 => [{:data_materializer, node()}]}
      entries = Enum.sort(initialized.shard_layout)
      entries = if @order == :reverse, do: Enum.reverse(entries), else: entries

      index =
        Enum.reduce(entries, LayoutIndex.new(), fn {end_key, {tag, start_key}}, index ->
          LayoutIndex.insert(index, start_key, end_key, Map.fetch!(refs, tag))
        end)

      # The old bulk builder emitted a third, zero-width entry at 0xFF.
      # OTP 28 accepted its duplicate tree key; OTP 29 rejected it.
      assert :gb_trees.to_list(index.tree) == [
               {<<0xFF>>, {<<>>, refs[1]}},
               {Bedrock.end_of_keyspace(), {<<0xFF>>, refs[0]}}
             ]

      for key <- [<<>>, "banana", <<0xFE, 0xFF>>] do
        assert LayoutIndex.lookup_key(index, key) == {:ok, {{<<>>, <<0xFF>>}, refs[1]}}
      end

      for key <- [<<0xFF>>, <<0xFF, 0>>, Bedrock.end_of_keyspace()] do
        assert LayoutIndex.lookup_key(index, key) ==
                 {:ok, {{<<0xFF>>, Bedrock.end_of_keyspace()}, refs[0]}}
      end
    end
  end

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
