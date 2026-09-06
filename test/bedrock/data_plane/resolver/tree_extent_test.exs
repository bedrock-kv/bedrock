defmodule Bedrock.DataPlane.Resolver.TreeExtentTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Resolver.Tree

  test "same-start extents retain their values in either insertion order" do
    for ranges <- [[{{"a", "b"}, 1}, {{"a", "c"}, 2}], [{{"a", "c"}, 2}, {{"a", "b"}, 1}]],
        padding <- [[], [{{"m", "n"}, 3}, {{"x", "y"}, 4}]] do
      tree = Tree.insert_bulk(nil, ranges ++ padding)
      assert Tree.overlap?(tree, "b")
      assert {"a", "b", 1} in Tree.to_list(tree)
      assert {"a", "c", 2} in Tree.to_list(tree)
      refute tree |> Tree.filter_by_value(&(&1 == 1)) |> Tree.overlap?("b")
      assert tree |> Tree.filter_by_value(&(&1 == 2)) |> Tree.overlap?("b")
    end
  end

  test "search finds an interval spanning the query from the left subtree" do
    tree = Tree.insert_bulk(nil, [{{"m", "n"}, 1}, {{"a", "z"}, 2}, {{"x", "y"}, 3}])
    assert Tree.overlap?(tree, "p")
    assert Tree.overlap?(tree, {"p", "q"})
    refute Tree.overlap?(tree, "z")
  end

  test "empty interval queries never overlap and exact duplicate intervals replace values" do
    tree = nil |> Tree.insert({"a", "z"}, 1) |> Tree.insert({"a", "z"}, 2)
    assert Tree.to_list(tree) == [{"a", "z", 2}]
    refute Tree.overlap?(tree, {"m", "m"})
    refute Tree.overlap?(tree, {"z", "a"})
  end

  property "insertions and filtering match a plain interval list including subtree metadata" do
    keys = ~w(a b c d e f g h)
    ranges = for first <- keys, last <- keys, first < last, do: {first, last}

    check all(
            entries <- list_of(tuple({member_of(ranges), integer(0..20)}), min_length: 1, max_length: 30),
            threshold <- integer(0..20),
            max_runs: 60
          ) do
      expected = Map.new(entries)
      regular = Enum.reduce(entries, nil, fn {range, value}, tree -> Tree.insert(tree, range, value) end)
      bulk = Tree.insert_bulk(nil, entries)

      for tree <- [regular, bulk] do
        assert_tree(tree, expected, keys, ranges)
        filtered = Tree.filter_by_value(tree, &(&1 > threshold))
        assert_tree(filtered, Map.reject(expected, fn {_, value} -> value <= threshold end), keys, ranges)
      end
    end
  end

  defp assert_tree(tree, expected, keys, queries) do
    expected_entries = expected |> Enum.map(fn {{first, last}, value} -> {first, last, value} end) |> Enum.sort()
    assert Tree.to_list(tree) == expected_entries
    assert_metadata(tree)

    for key <- keys do
      assert Tree.overlap?(tree, key) == Enum.any?(expected, fn {{first, last}, _} -> first <= key and key < last end)
    end

    for {first, last} = query <- queries do
      assert Tree.overlap?(tree, query) ==
               Enum.any?(expected, fn {{start, stop}, _} -> first < stop and start < last end)
    end
  end

  defp assert_metadata(nil), do: {0, <<>>}

  defp assert_metadata(tree) do
    {left_height, left_max} = assert_metadata(tree.left)
    {right_height, right_max} = assert_metadata(tree.right)
    height = 1 + max(left_height, right_height)
    max_end = max(tree.end, max(left_max, right_max))
    assert tree.height == height
    assert tree.max_end == max_end
    {height, max_end}
  end
end
