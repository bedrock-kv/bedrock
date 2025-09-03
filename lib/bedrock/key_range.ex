defmodule Bedrock.KeyRange do
  @moduledoc """
  Utilities for working with key ranges.

  A key range is represented as a tuple `{start_key, end_key}` where:
  - `start_key` is inclusive
  - `end_key` is exclusive, or the atom `:end` for unbounded ranges
  """

  alias Bedrock.Key

  @type t :: {Key.t(), Key.t() | :end}

  @doc """
  Check if two key ranges overlap.

  Returns true if the ranges have any keys in common.

  ## Examples

      iex> KeyRange.overlaps?({"a", "c"}, {"b", "d"})
      true

      iex> KeyRange.overlaps?({"a", "b"}, {"c", "d"})
      false

      iex> KeyRange.overlaps?({"a", "c"}, {"b", :end})
      true
  """
  @spec overlap?(t(), t()) :: boolean()
  def overlap?({_start1, end1}, {start2, :end}), do: start2 <= end1
  def overlap?({start1, end1}, {start2, end2}), do: start1 < end2 and start2 < end1

  @doc """
  Check if a key range contains a specific key.

  Returns true if the key is within the range.

  ## Examples

      iex> KeyRange.contains?({"a", "c"}, "b")
      true

      iex> KeyRange.contains?({"a", "c"}, "c")
      false

      iex> KeyRange.contains?({"a", :end}, "z")
      true
  """
  @spec contains?(t(), Key.t() | :end) :: boolean()
  def contains?({min_key, :end}, key), do: key >= min_key
  def contains?({min_key, max_key_ex}, key), do: key >= min_key and key < max_key_ex
end
