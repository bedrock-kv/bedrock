defmodule Bedrock.DataPlane.Resolver.Tree do
  @moduledoc """
  Provides functionality for an interval tree, allowing for efficient insertion,
  querying, overlap detection, and filtering of intervals.

  This module defines a self-balancing binary search tree (AVL tree) that stores
  intervals, making it possible to efficiently perform operations such as checking
  for overlaps with a given interval or point, inserting new intervals with
  associated values, and filtering the tree's intervals based on the values
  associated with them.

  Intervals are represented as tuples of a start and end key paired with a value.
  Operations are provided for both inserting new intervals into the tree and querying
  for existing intervals that overlap a given range. The tree is kept balanced
  automatically to ensure operations perform optimally.

  Various utility functions are also provided, including converting the tree into
  a list of its intervals and filtering by a predicate.

  ## Types

    - `t`: The type representing an interval tree node.

  ## Functions

    - `new/3`: Creates a new interval tree node.
    - `overlap?/3`: Checks if a given range or key overlaps with any interval in the tree.
    - `insert/3`: Inserts a new interval into the tree, balancing it if necessary, and
       returns the updated tree.
    - `height/1`: Gets the height of the tree.
    - `filter_by_value/2`: Filters the tree by evaluating a predicate on each node's value.
    - `to_list/1`: Converts the interval tree into a list of tuples.
  """
  alias Bedrock.DataPlane.Resolver.Tree

  @type t :: %Tree{
          start: Bedrock.key(),
          end: Bedrock.key(),
          value: Bedrock.version(),
          left: t() | nil,
          right: t() | nil,
          height: non_neg_integer()
        }
  defstruct [
    :start,
    :end,
    :value,
    :left,
    :right,
    :height
  ]

  @doc """
  Checks if a given range or key overlaps with any interval in the tree.

  ## Parameters

    - tree: The interval tree to check for overlaps, or nil if empty.
    - range: A tuple representing the start and end of the range or a single
      key to be checked for overlap with range in the tree.
    - predicate: An optional function to check if the value associated
      with an overlapping range satisfies a condition. Defaults to always
      returning true.

  ## Returns

    - `true` if there is an overlap with any range in the tree that satisfies
      the predicate function, otherwise `false`.
  """
  @spec overlap?(t(), Bedrock.key() | Bedrock.key_range()) :: boolean()
  def overlap?(tree, range, predicate \\ &default_predicate/1)

  def overlap?(nil, _, _), do: false

  def overlap?(%Tree{start: tree_start, end: tree_end} = tree, {start, end_} = range, predicate) do
    if start < tree_end and tree_start < end_ and predicate.(tree.value) do
      true
    else
      if start < tree.start do
        overlap?(tree.left, range, predicate)
      else
        overlap?(tree.right, range, predicate)
      end
    end
  end

  def overlap?(tree, key, predicate) when is_binary(key), do: overlap?(tree, {key, key <> <<0>>}, predicate)

  defp default_predicate(_), do: true

  @doc """
  Inserts a new interval into the tree, balancing it if necessary, and returns
  the updated tree.

  ## Parameters

    - tree: The current interval tree or `nil` if empty.
    - range: A tuple representing the start and end of the interval to insert,
      or a single key to be inserted as an interval with its respective value.
    - value: The value associated with the interval.

  ## Returns

    - The updated interval tree containing the new interval.
  """
  @spec insert(nil | t(), Bedrock.key() | Bedrock.key_range(), Bedrock.version()) :: t()
  def insert(nil, {start, end_}, value), do: %Tree{start: start, end: end_, value: value, height: 1}

  def insert(%Tree{} = tree, {start, _end} = range, value) do
    cond_result =
      cond do
        # If the range comes before the current tree
        start < tree.start ->
          %{tree | left: insert(tree.left, range, value)}

        # If the range comes after the current tree
        start > tree.start ->
          %{tree | right: insert(tree.right, range, value)}

        # If the range overlaps or is the same, we can choose to overwrite or handle differently
        true ->
          %{tree | value: value}
      end

    balance(cond_result)
  end

  def insert(tree, key, value) when is_binary(key), do: insert(tree, {key, key <> <<0>>}, value)

  @doc """
  Inserts multiple intervals into the tree efficiently with delayed rebalancing.

  This is more efficient than calling insert/3 multiple times as it only rebalances
  once at the end instead of after each insertion.

  ## Parameters

    - tree: The current interval tree or `nil` if empty.
    - ranges: A list of ranges to insert, each as {range, value} tuple.
    - version: The version to associate with all ranges.

  ## Returns

    - The updated interval tree containing all new intervals, properly balanced.
  """
  @spec insert_bulk(nil | t(), [{Bedrock.key_range(), Bedrock.version()}]) :: t() | nil
  def insert_bulk(tree, []), do: tree

  def insert_bulk(tree, range_value_pairs) do
    case range_value_pairs do
      [] ->
        tree

      # For small batches, just use regular insert (more efficient)
      pairs when length(pairs) <= 3 ->
        Enum.reduce(pairs, tree, fn {range, value}, acc_tree ->
          insert(acc_tree, range, value)
        end)

      # For larger batches, use bulk strategy with smarter rebalancing
      _large_batch ->
        # Insert all ranges without rebalancing
        updated_tree =
          Enum.reduce(range_value_pairs, tree, fn {range, value}, acc_tree ->
            insert_no_balance(acc_tree, range, value)
          end)

        rebalance_after_bulk_insert(updated_tree)
    end
  end

  @spec insert_no_balance(nil | t(), Bedrock.key() | Bedrock.key_range(), Bedrock.version()) :: t()
  defp insert_no_balance(nil, {start, end_}, value),
    do: %Tree{start: start, end: end_, value: value, left: nil, right: nil, height: 1}

  defp insert_no_balance(%Tree{} = tree, {start, _end} = range, value) do
    cond do
      start < tree.start ->
        %{tree | left: insert_no_balance(tree.left, range, value)}

      start > tree.start ->
        %{tree | right: insert_no_balance(tree.right, range, value)}

      true ->
        %{tree | value: value}
    end
  end

  defp insert_no_balance(tree, key, value) when is_binary(key), do: insert_no_balance(tree, {key, key <> <<0>>}, value)

  @spec height(t() | nil) :: non_neg_integer()
  def height(nil), do: 0
  def height(%Tree{height: h}), do: h

  @spec update_height(t()) :: t()
  defp update_height(%Tree{left: left, right: right} = tree) do
    left_height = if left, do: left.height, else: 0
    right_height = if right, do: right.height, else: 0
    %{tree | height: 1 + max(left_height, right_height)}
  end

  @spec balance_factor(t()) :: integer()
  defp balance_factor(%Tree{left: left, right: right}) do
    left_height = if left, do: left.height, else: 0
    right_height = if right, do: right.height, else: 0
    left_height - right_height
  end

  @spec rotate_right(t()) :: t()
  defp rotate_right(%Tree{left: %Tree{left: t1, right: t2} = x, right: t3} = y),
    do: update_height(%{x | left: t1, right: update_height(%{y | right: t3, left: t2})})

  @spec rotate_left(t()) :: t()
  defp rotate_left(%Tree{right: %Tree{left: t2, right: t3} = y, left: t1} = x),
    do: update_height(%{y | right: t3, left: update_height(%{x | right: t2, left: t1})})

  @spec balance(t()) :: t()
  defp balance(%Tree{} = tree) do
    balance_factor = balance_factor(tree)

    cond do
      balance_factor > 1 ->
        if balance_factor(tree.left) >= 0 do
          rotate_right(tree)
        else
          rotate_right(%{tree | left: rotate_left(tree.left)})
        end

      balance_factor < -1 ->
        if balance_factor(tree.right) <= 0 do
          rotate_left(tree)
        else
          rotate_left(%{tree | right: rotate_right(tree.right)})
        end

      true ->
        update_height(tree)
    end
  end

  @spec rebalance_after_bulk_insert(t() | nil) :: t() | nil
  defp rebalance_after_bulk_insert(nil), do: nil

  defp rebalance_after_bulk_insert(%Tree{} = tree) do
    left_fixed = fix_heights_only(tree.left)
    right_fixed = fix_heights_only(tree.right)

    updated_tree = update_height(%{tree | left: left_fixed, right: right_fixed})
    balance_factor = abs(balance_factor(updated_tree))

    if balance_factor > 1 do
      balance(updated_tree)
    else
      updated_tree
    end
  end

  @spec fix_heights_only(t() | nil) :: t() | nil
  defp fix_heights_only(nil), do: nil

  defp fix_heights_only(%Tree{} = tree) do
    left_fixed = fix_heights_only(tree.left)
    right_fixed = fix_heights_only(tree.right)

    update_height(%{tree | left: left_fixed, right: right_fixed})
  end

  @doc """
  Filters the tree by evaluating a predicate on each node's value.

  ## Parameters

    - tree: The tree to filter, or nil if empty.
    - predicate: A function that returns true for nodes that should be kept in the tree.

  ## Returns

    - A new tree containing only the nodes for which the predicate returned true.
  """

  @spec filter_by_value(t() | nil, (Bedrock.version() -> boolean())) :: t() | nil
  def filter_by_value(tree, predicate),
    do: tree |> filter_by_value_no_balance(predicate) |> rebalance_after_bulk_filter()

  @spec filter_by_value_no_balance(t() | nil, (Bedrock.version() -> boolean())) :: t() | nil
  defp filter_by_value_no_balance(nil, _predicate), do: nil

  defp filter_by_value_no_balance(%Tree{} = tree, predicate) do
    new_left = filter_by_value_no_balance(tree.left, predicate)
    new_right = filter_by_value_no_balance(tree.right, predicate)

    if predicate.(tree.value) do
      %{tree | left: new_left, right: new_right}
    else
      combine_filtered_subtrees(new_left, new_right)
    end
  end

  @spec rebalance_after_bulk_filter(t() | nil) :: t() | nil
  defp rebalance_after_bulk_filter(nil), do: nil
  defp rebalance_after_bulk_filter(%Tree{} = tree), do: tree |> fix_heights_only() |> balance()

  @spec combine_filtered_subtrees(t() | nil, t() | nil) :: t() | nil
  defp combine_filtered_subtrees(nil, nil), do: nil
  defp combine_filtered_subtrees(nil, right), do: right
  defp combine_filtered_subtrees(left, nil), do: left
  defp combine_filtered_subtrees(left, right), do: find_and_attach_rightmost(left, right)

  # Helper to properly combine subtrees by finding rightmost node in left tree
  defp find_and_attach_rightmost(%Tree{right: nil} = node, right_tree), do: %{node | right: right_tree}

  defp find_and_attach_rightmost(%Tree{right: right} = node, right_tree),
    do: %{node | right: find_and_attach_rightmost(right, right_tree)}

  @doc """
  Converts the interval tree into a list of tuples, where each tuple
  represents an interval with its associated value. The list is ordered
  by the start of the range.

  ## Parameters

    - tree: The interval tree to convert, or nil if empty.

  ## Returns

    - A list of tuples in the form `{start, end, value}`, representing
      the range and their associated values in the tree.
  """
  @spec to_list(t() | nil) :: [{Bedrock.key(), Bedrock.key(), Bedrock.version()}]
  def to_list(nil), do: []
  def to_list(tree), do: to_list([], tree)

  defp to_list(list, nil), do: list

  defp to_list(list, %Tree{left: left, right: right} = tree),
    do: to_list([{tree.start, tree.end, tree.value} | to_list(list, right)], left)
end
