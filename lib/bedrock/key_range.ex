defmodule Bedrock.KeyRange do
  @moduledoc """
  Utilities for working with key ranges.

  A key range is represented as a tuple `{start_key, end_key}` where:
  - `start_key` is inclusive
  - `end_key` is exclusive, or the atom `:end` for unbounded ranges
  """

  @type t :: {binary(), binary() | :end}

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
  @spec overlaps?(t(), t()) :: boolean()
  def overlaps?({_start1, :end}, {_start2, :end}), do: true
  def overlaps?({_start1, end1}, {start2, :end}), do: start2 <= end1
  def overlaps?({start1, :end}, {_start2, end2}), do: start1 < end2
  def overlaps?({start1, end1}, {start2, end2}), do: start1 < end2 and start2 < end1
end
