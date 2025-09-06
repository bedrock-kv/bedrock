defmodule Bedrock.DataPlane.Resolver.Interval.TreeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Bedrock.DataPlane.Resolver.Tree

  def key_or_range_generator do
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

  # Test that inserting an interval into the tree increases its size by 1
  property "can find intervals" do
    check all(intervals <- list_of(key_or_range_generator(), min_length: 1)) do
      tree =
        Enum.reduce(intervals, nil, fn interval, acc -> Tree.insert(acc, interval, 0) end)

      assert Enum.all?(intervals, fn interval -> Tree.overlap?(tree, interval) end)
    end
  end

  # Test that `any?` correctly identifies overlapping intervals
  property "any? identifies overlap" do
    check all(intervals <- list_of(key_or_range_generator(), min_length: 2)) do
      tree =
        Enum.reduce(intervals, nil, fn interval, acc -> Tree.insert(acc, interval, 0) end)

      # Test overlap with known existing interval
      first_interval = List.first(intervals)
      assert Tree.overlap?(tree, first_interval)

      # Test non-overlapping interval
      refute Tree.overlap?(tree, {<<0xFF>>, <<0xFF00>>})
    end
  end

  property "filter_by_value/2 correctly filters nodes" do
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
      # Build the tree from the generated intervals
      tree =
        intervals
        |> Enum.with_index()
        |> Enum.reduce(nil, fn {interval, value}, acc ->
          Tree.insert(acc, interval, value)
        end)

      # Filter the tree by the generated predicate
      filtered_tree = Tree.filter_by_value(tree, filter_fun)

      # Convert the filtered tree back to a list to verify
      filtered_list = Tree.to_list(filtered_tree)

      # Check property: all values in the filtered list should satisfy `filter_fun`
      assert Enum.all?(filtered_list, fn {_start, _end, value} -> filter_fun.(value) end)

      # Additional structural checks could be added here (e.g., assert balance)
    end
  end

  property "filter_by_value produces correct results" do
    check all(
            intervals <- list_of(key_or_range_generator(), min_length: 0, max_length: 50),
            filter_threshold <- integer(0..100)
          ) do
      # Build tree from intervals with sequential values
      tree =
        intervals
        |> Enum.with_index()
        |> Enum.reduce(nil, fn {interval, value}, acc ->
          Tree.insert(acc, interval, value)
        end)

      filter_fun = &(&1 > filter_threshold)

      # Test the implementation
      result = Tree.filter_by_value(tree, filter_fun)

      # Convert to list and verify
      filtered_list = Tree.to_list(result)

      # Verify all results satisfy the predicate
      assert Enum.all?(filtered_list, fn {_start, _end, value} -> filter_fun.(value) end)

      # Verify no values that should be included are missing
      original_list = Tree.to_list(tree)
      expected_values = Enum.filter(original_list, fn {_, _, value} -> filter_fun.(value) end)
      actual_values = Enum.sort(filtered_list)

      assert Enum.sort(expected_values) == actual_values
    end
  end
end
