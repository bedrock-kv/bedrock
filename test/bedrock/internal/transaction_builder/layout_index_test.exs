defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  # Dummy materializer processes; only pid identity matters to the index.
  defp dummy_pid, do: spawn(fn -> Process.sleep(:infinity) end)

  # A realistic layout: metadata shard (tag 0) at the top of the keyspace,
  # plus contiguous data shards. shard_layout partitions the keyspace — a
  # shard's end key is always the next shard's start key.
  defp realistic_layout(metadata_pid, apples_pid, mangoes_pid) do
    %{
      shard_layout: %{
        # data shard: "a" ..< "m", tag 1
        "m" => {1, "a"},
        # data shard: "m" ..< <<0xFF>>, tag 2
        <<0xFF>> => {2, "m"},
        # metadata shard: <<0xFF>> ..< <<0xFF, 0xFF>>, tag 0
        <<0xFF, 0xFF>> => {0, <<0xFF>>}
      },
      metadata_materializer: metadata_pid,
      shard_materializers: %{1 => apples_pid, 2 => mangoes_pid}
    }
  end

  # The overlapping layout from the moduledoc: a-f -> p1, d-m -> p2, h-p -> p3.
  defp overlapping_layout(p1, p2, p3) do
    %{
      shard_layout: %{"f" => {1, "a"}, "m" => {2, "d"}, "p" => {3, "h"}},
      metadata_materializer: nil,
      shard_materializers: %{1 => p1, 2 => p2, 3 => p3}
    }
  end

  # Three contiguous shards where the middle shard's tag has no materializer.
  # Under total coverage the middle shard is still a segment — with [] pids.
  defp uncovered_middle_layout(p1, p2) do
    %{
      shard_layout: %{"d" => {1, "a"}, "h" => {3, "d"}, "m" => {2, "h"}},
      metadata_materializer: nil,
      shard_materializers: %{1 => p1, 2 => p2}
    }
  end

  describe "lookup_key!/2" do
    setup do
      [metadata, apples, mangoes] = pids = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(realistic_layout(metadata, apples, mangoes))
      on_exit(fn -> Enum.each(pids, &Process.exit(&1, :kill)) end)
      %{index: index, metadata: metadata, apples: apples, mangoes: mangoes}
    end

    test "returns the containing segment and its materializer pid", ctx do
      assert {{"a", "m"}, [ctx.apples]} == LayoutIndex.lookup_key!(ctx.index, "banana")
      assert {{"m", <<0xFF>>}, [ctx.mangoes]} == LayoutIndex.lookup_key!(ctx.index, "orange")
    end

    test "a key equal to a segment's start key belongs to that segment", ctx do
      assert {{"a", "m"}, [ctx.apples]} == LayoutIndex.lookup_key!(ctx.index, "a")
      assert {{"m", <<0xFF>>}, [ctx.mangoes]} == LayoutIndex.lookup_key!(ctx.index, "m")
    end

    test "a key equal to a segment's end key belongs to the next segment", ctx do
      assert {{<<0xFF>>, <<0xFF, 0xFF>>}, [ctx.metadata]} ==
               LayoutIndex.lookup_key!(ctx.index, <<0xFF>>)
    end

    test "tag 0 keys route to the metadata materializer", ctx do
      assert {{<<0xFF>>, <<0xFF, 0xFF>>}, [ctx.metadata]} ==
               LayoutIndex.lookup_key!(ctx.index, <<0xFF, 0x01, "system_key">>)
    end

    test "the keyspace sentinel end key is included in the final segment", ctx do
      assert {{<<0xFF>>, <<0xFF, 0xFF>>}, [ctx.metadata]} ==
               LayoutIndex.lookup_key!(ctx.index, <<0xFF, 0xFF>>)
    end

    test "raises for a key before the first segment", ctx do
      assert_raise RuntimeError, ~r/No segment found containing key: "A"/, fn ->
        LayoutIndex.lookup_key!(ctx.index, "A")
      end
    end

    test "a key in a shard with no materializer returns the segment with [] pids" do
      p1 = dummy_pid()
      p2 = dummy_pid()
      index = LayoutIndex.build_index(uncovered_middle_layout(p1, p2))

      # The uncovered shard is never invisible: callers must see {range, []}
      # and fail loudly (layout_lookup_failed) rather than silently skip keys.
      assert {{"d", "h"}, []} == LayoutIndex.lookup_key!(index, "e")
    end

    test "raises for any key when the layout has no shards" do
      index = LayoutIndex.build_index(%{})

      assert_raise RuntimeError, ~r/No segment found/, fn ->
        LayoutIndex.lookup_key!(index, "anything")
      end
    end
  end

  describe "lookup_key!/2 with overlapping shards" do
    test "a segment covered by two shards lists both pids in shard order" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      # Segments per the moduledoc: {a,d}->[p1], {d,f}->[p1,p2], {f,h}->[p2],
      # {h,m}->[p2,p3], {m,p}->[p3]
      assert {{"a", "d"}, [^p1]} = LayoutIndex.lookup_key!(index, "b")
      assert {{"d", "f"}, [^p1, ^p2]} = LayoutIndex.lookup_key!(index, "e")
      assert {{"f", "h"}, [^p2]} = LayoutIndex.lookup_key!(index, "g")
      assert {{"h", "m"}, [^p2, ^p3]} = LayoutIndex.lookup_key!(index, "k")
      assert {{"m", "p"}, [^p3]} = LayoutIndex.lookup_key!(index, "o")
    end
  end

  describe "lookup_range/3" do
    setup do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))
      %{index: index, p1: p1, p2: p2, p3: p3}
    end

    test "returns every segment overlapping the range, in key order", ctx do
      assert [
               {{"d", "f"}, [ctx.p1, ctx.p2]},
               {{"f", "h"}, [ctx.p2]},
               {{"h", "m"}, [ctx.p2, ctx.p3]}
             ] == LayoutIndex.lookup_range(ctx.index, "e", "i")
    end

    test "a range within a single segment returns just that segment", ctx do
      assert [{{"a", "d"}, [ctx.p1]}] == LayoutIndex.lookup_range(ctx.index, "b", "c")
    end

    test "a range spanning the whole keyspace returns all segments", ctx do
      assert [
               {{"a", "d"}, [ctx.p1]},
               {{"d", "f"}, [ctx.p1, ctx.p2]},
               {{"f", "h"}, [ctx.p2]},
               {{"h", "m"}, [ctx.p2, ctx.p3]},
               {{"m", "p"}, [ctx.p3]}
             ] == LayoutIndex.lookup_range(ctx.index, <<>>, <<0xFF, 0xFF>>)
    end

    test "a range entirely after all segments returns an empty list", ctx do
      assert [] == LayoutIndex.lookup_range(ctx.index, "q", "z")
    end

    test "a range entirely before all segments returns an empty list", ctx do
      assert [] == LayoutIndex.lookup_range(ctx.index, "A", "Z")
    end

    test "a range ending exactly at a segment start excludes that segment", ctx do
      # end key is exclusive: range [b, d) does not overlap segment {d, f}
      assert [{{"a", "d"}, [ctx.p1]}] == LayoutIndex.lookup_range(ctx.index, "b", "d")
    end

    test "a range covering only an uncovered shard returns that segment with [] pids" do
      p1 = dummy_pid()
      p2 = dummy_pid()
      index = LayoutIndex.build_index(uncovered_middle_layout(p1, p2))

      assert [{{"d", "h"}, []}] == LayoutIndex.lookup_range(index, "e", "g")
    end

    test "an empty layout yields no overlapping segments" do
      index = LayoutIndex.build_index(%{})

      assert [] == LayoutIndex.lookup_range(index, <<>>, <<0xFF, 0xFF>>)
    end
  end

  describe "get_next_segment/2" do
    test "returns the adjacent segment when segments are contiguous" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert {:ok, {{"d", "f"}, [^p1, ^p2]}} = LayoutIndex.get_next_segment(index, "b")
      assert {:ok, {{"m", "p"}, [^p3]}} = LayoutIndex.get_next_segment(index, "i")
    end

    test "returns :end_of_keyspace from the last segment" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert :end_of_keyspace == LayoutIndex.get_next_segment(index, "o")
    end

    test "returns :end_of_keyspace when the key is not inside any segment" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert :end_of_keyspace == LayoutIndex.get_next_segment(index, "z")
    end

    test "returns the adjacent uncovered segment instead of skipping it" do
      p1 = dummy_pid()
      p2 = dummy_pid()
      index = LayoutIndex.build_index(uncovered_middle_layout(p1, p2))

      # Exact-boundary semantics (bedrock-q67.1, revising bedrock-dwu): the
      # segment after {a, d} is {d, h} even though it has no materializer.
      # Skipping it would silently drop live keys during cross-shard
      # KeySelector resolution.
      assert {:ok, {{"d", "h"}, []}} = LayoutIndex.get_next_segment(index, "b")
    end
  end

  describe "uncovered-shard adjacency (bedrock-q67.1, revising bedrock-dwu)" do
    setup do
      p1 = dummy_pid()
      p2 = dummy_pid()
      %{index: LayoutIndex.build_index(uncovered_middle_layout(p1, p2)), p1: p1, p2: p2}
    end

    test "next and previous both return the adjacent uncovered segment, never skip it", ctx do
      # forward: from inside {a, d} to the uncovered {d, h}
      assert {:ok, {{"d", "h"}, []}} == LayoutIndex.get_next_segment(ctx.index, "b")
      # backward: from inside {h, m} to the uncovered {d, h}
      assert {:ok, {{"d", "h"}, []}} == LayoutIndex.get_previous_segment(ctx.index, "i")
    end

    test "navigation out of an uncovered segment reaches its covered neighbors", ctx do
      assert {:ok, {{"h", "m"}, [ctx.p2]}} == LayoutIndex.get_next_segment(ctx.index, "e")
      assert {:ok, {{"a", "d"}, [ctx.p1]}} == LayoutIndex.get_previous_segment(ctx.index, "e")
    end

    test "navigation still terminates at the ends of the keyspace", ctx do
      assert :end_of_keyspace == LayoutIndex.get_next_segment(ctx.index, "i")
      assert :start_of_keyspace == LayoutIndex.get_previous_segment(ctx.index, "b")
    end
  end

  describe "get_previous_segment/2" do
    test "returns the adjacent segment when segments are contiguous" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert {:ok, {{"a", "d"}, [^p1]}} = LayoutIndex.get_previous_segment(index, "e")
      assert {:ok, {{"h", "m"}, [^p2, ^p3]}} = LayoutIndex.get_previous_segment(index, "o")
    end

    test "returns :start_of_keyspace from the first segment" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert :start_of_keyspace == LayoutIndex.get_previous_segment(index, "b")
    end

    test "returns :start_of_keyspace when the key is not inside any segment" do
      [p1, p2, p3] = Enum.map(1..3, fn _ -> dummy_pid() end)
      index = LayoutIndex.build_index(overlapping_layout(p1, p2, p3))

      assert :start_of_keyspace == LayoutIndex.get_previous_segment(index, "z")
    end

    test "returns the adjacent uncovered segment instead of skipping it" do
      p1 = dummy_pid()
      p2 = dummy_pid()
      index = LayoutIndex.build_index(uncovered_middle_layout(p1, p2))

      assert {:ok, {{"d", "h"}, []}} = LayoutIndex.get_previous_segment(index, "i")
    end
  end

  describe "materializer resolution during build_index/1" do
    test "shards whose tag has no materializer become segments with [] pids" do
      pid = dummy_pid()

      layout = %{
        shard_layout: %{"m" => {1, "a"}, "z" => {7, "m"}},
        metadata_materializer: nil,
        shard_materializers: %{1 => pid}
      }

      index = LayoutIndex.build_index(layout)

      assert {{"a", "m"}, [^pid]} = LayoutIndex.lookup_key!(index, "b")
      # tag 7 has no materializer: the shard is still indexed, with no pids
      assert {{"m", "z"}, []} == LayoutIndex.lookup_key!(index, "q")
    end

    test "the metadata shard becomes a [] segment when metadata_materializer is not a pid" do
      pid = dummy_pid()

      layout = %{
        shard_layout: %{<<0xFF>> => {1, <<>>}, <<0xFF, 0xFF>> => {0, <<0xFF>>}},
        metadata_materializer: nil,
        shard_materializers: %{1 => pid}
      }

      index = LayoutIndex.build_index(layout)

      assert {{<<0xFF>>, <<0xFF, 0xFF>>}, []} == LayoutIndex.lookup_key!(index, <<0xFF, 0x01>>)
    end

    test "the production default contiguous layout builds exactly two segments (bedrock-tn5 regression)" do
      metadata_pid = dummy_pid()
      data_pid = dummy_pid()

      # The default layout from initialization_phase.ex: a single data shard
      # covering <<>> ..< <<0xFF>> and the metadata shard <<0xFF>> ..< <<0xFF, 0xFF>>.
      # The shared boundary <<0xFF>> must not create a zero-width artifact segment.
      layout = %{
        shard_layout: %{<<0xFF>> => {1, <<>>}, <<0xFF, 0xFF>> => {0, <<0xFF>>}},
        metadata_materializer: metadata_pid,
        shard_materializers: %{1 => data_pid}
      }

      index = LayoutIndex.build_index(layout)
      segments = LayoutIndex.lookup_range(index, <<>>, <<0xFF, 0xFF>>)

      assert [
               {{<<>>, <<0xFF>>}, [data_pid]},
               {{<<0xFF>>, <<0xFF, 0xFF>>}, [metadata_pid]}
             ] == segments

      refute Enum.any?(segments, fn {{s, e}, _pids} -> s == e end)
    end

    test "the production default layout with a nil metadata pid still builds two segments" do
      data_pid = dummy_pid()

      layout = %{
        shard_layout: %{<<0xFF>> => {1, <<>>}, <<0xFF, 0xFF>> => {0, <<0xFF>>}},
        metadata_materializer: nil,
        shard_materializers: %{1 => data_pid}
      }

      index = LayoutIndex.build_index(layout)

      assert [
               {{<<>>, <<0xFF>>}, [data_pid]},
               {{<<0xFF>>, <<0xFF, 0xFF>>}, []}
             ] == LayoutIndex.lookup_range(index, <<>>, <<0xFF, 0xFF>>)
    end

    test "non-pid entries in shard_materializers are treated as missing" do
      layout = %{
        shard_layout: %{"m" => {1, "a"}},
        metadata_materializer: nil,
        shard_materializers: %{1 => :not_a_pid}
      }

      index = LayoutIndex.build_index(layout)

      assert [{{"a", "m"}, []}] == LayoutIndex.lookup_range(index, <<>>, <<0xFF, 0xFF>>)
    end
  end
end
