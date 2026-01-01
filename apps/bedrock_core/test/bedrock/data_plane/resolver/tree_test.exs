defmodule Bedrock.DataPlane.Resolver.TreeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Bedrock.DataPlane.Resolver.Tree

  defp key_or_range_generator do
    :alphanumeric
    |> string()
    |> StreamData.bind(fn v1 ->
      :alphanumeric
      |> string()
      |> StreamData.map(fn
        v2 when v1 > v2 -> {v2, v1}
        v2 when v1 == v2 -> v1
        v2 -> {v1, v2}
      end)
    end)
  end

  defp build_tree_from_intervals(intervals, value_mapper \\ fn _interval, index -> index end) do
    intervals
    |> Enum.with_index()
    |> Enum.reduce(nil, fn {interval, index}, acc ->
      value = value_mapper.(interval, index)
      Tree.insert(acc, interval, value)
    end)
  end

  property "can find all inserted intervals" do
    check all(intervals <- list_of(key_or_range_generator(), min_length: 1)) do
      tree = build_tree_from_intervals(intervals, fn _interval, _index -> 0 end)

      assert Enum.all?(intervals, fn interval -> Tree.overlap?(tree, interval) end)
    end
  end

  property "overlap? identifies overlapping intervals correctly" do
    check all(intervals <- list_of(key_or_range_generator(), min_length: 2)) do
      tree = build_tree_from_intervals(intervals, fn _interval, _index -> 0 end)
      first_interval = List.first(intervals)

      # Test overlap with known existing interval
      assert Tree.overlap?(tree, first_interval)

      # Test non-overlapping interval
      refute Tree.overlap?(tree, {<<0xFF>>, <<0xFF00>>})
    end
  end

  property "filter_by_value/2 correctly filters nodes with various predicates" do
    check all(
            intervals <- list_of(key_or_range_generator(), min_length: 2),
            filter_fun <-
              one_of([
                constant(fn _ -> true end),
                constant(fn _ -> false end),
                constant(&(&1 > 5)),
                constant(&(&1 < 10))
              ])
          ) do
      tree = build_tree_from_intervals(intervals)
      filtered_tree = Tree.filter_by_value(tree, filter_fun)
      filtered_list = Tree.to_list(filtered_tree)

      # All filtered results should satisfy the predicate
      assert Enum.all?(filtered_list, fn {_start, _end, value} -> filter_fun.(value) end)
    end
  end

  property "filter_by_value/2 produces complete and correct results" do
    check all(
            intervals <- list_of(key_or_range_generator(), min_length: 0, max_length: 50),
            filter_threshold <- integer(0..100)
          ) do
      tree = build_tree_from_intervals(intervals)
      filter_fun = &(&1 > filter_threshold)

      filtered_tree = Tree.filter_by_value(tree, filter_fun)
      original_list = Tree.to_list(tree)
      expected_list = Enum.filter(original_list, fn {_, _, value} -> filter_fun.(value) end)
      actual_list = Tree.to_list(filtered_tree)

      # All filtered results satisfy the predicate
      assert Enum.all?(actual_list, fn {_start, _end, value} -> filter_fun.(value) end)

      # No matching values are missing from the result
      assert Enum.sort(expected_list) == Enum.sort(actual_list)
    end
  end
end
