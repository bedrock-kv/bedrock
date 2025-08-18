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

  # Get the height of the tree
  @spec height(t() | nil) :: non_neg_integer()
  def height(nil), do: 0
  def height(%Tree{height: h}), do: h

  # Update the height of the tree based on its children's heights
  @spec update_height(t()) :: t()
  defp update_height(%Tree{left: left, right: right} = tree), do: %{tree | height: 1 + max(height(left), height(right))}

  # Calculate the balance factor of the tree
  @spec balance_factor(t()) :: integer()
  defp balance_factor(%Tree{left: left, right: right}), do: height(left) - height(right)

  # Perform a right rotation
  @spec rotate_right(t()) :: t()
  defp rotate_right(%Tree{left: %Tree{left: t1, right: t2} = x, right: t3} = y),
    do: update_height(%{x | left: t1, right: update_height(%{y | right: t3, left: t2})})

  # Perform a left rotation
  @spec rotate_left(t()) :: t()
  defp rotate_left(%Tree{right: %Tree{left: t2, right: t3} = y, left: t1} = x),
    do: update_height(%{y | right: t3, left: update_height(%{x | right: t2, left: t1})})

  # Balance the tree if unbalanced
  @spec balance(t()) :: t()
  defp balance(%Tree{} = tree) do
    balance_factor = balance_factor(tree)

    cond do
      # Left heavy
      balance_factor > 1 ->
        if balance_factor(tree.left) >= 0 do
          rotate_right(tree)
        else
          rotate_right(%{tree | left: rotate_left(tree.left)})
        end

      # Right heavy
      balance_factor < -1 ->
        if balance_factor(tree.right) <= 0 do
          rotate_left(tree)
        else
          rotate_left(%{tree | right: rotate_right(tree.right)})
        end

      # Already balanced
      true ->
        update_height(tree)
    end
  end

  defp balance(nil), do: nil

  @doc """
  Filters the tree by evaluating a predicate on each node's value.

  ## Parameters

    - tree: The tree to filter, or nil if empty.
    - predicate: A function that returns true for nodes that should be kept in the tree.

  ## Returns

    - A new tree containing only the nodes for which the predicate returned true.
  """

  @spec filter_by_value(t() | nil, (Bedrock.version() -> boolean())) :: t() | nil
  def filter_by_value(%Tree{} = tree, predicate) do
    new_left = filter_by_value(tree.left, predicate)
    new_right = filter_by_value(tree.right, predicate)

    if_result =
      if predicate.(tree.value) do
        %{tree | left: new_left, right: new_right}
      else
        combine_filtered_subtrees(new_left, new_right)
      end

    balance(if_result)
  end

  def filter_by_value(nil, _predicate), do: nil

  # Combine the filtered subtrees when a tree doesn't pass the filter
  @spec combine_filtered_subtrees(t() | nil, t() | nil) :: t() | nil
  defp combine_filtered_subtrees(nil, nil), do: nil
  defp combine_filtered_subtrees(nil, right), do: right
  defp combine_filtered_subtrees(left, nil), do: left
  defp combine_filtered_subtrees(left, right), do: %{left | right: right}

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
