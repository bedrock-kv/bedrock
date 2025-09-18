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
  Returns a range tuple {start, end} for all keys that start with the given prefix.

  The end key is computed using `strinc/1` to create a tight upper bound,
  ensuring the range includes only keys with the exact prefix.

  ## Examples

      iex> Bedrock.Key.from_prefix("user")
      {"user", "uses"}

      iex> Bedrock.Key.from_prefix("prefix/")
      {"prefix/", "prefix0"}

  ## Errors

  Raises an `ArgumentError` if the prefix contains only 0xFF bytes.

      iex> Bedrock.Key.from_prefix(<<0xFF>>)
      ** (ArgumentError) Key must contain at least one byte not equal to 0xFF

  """
  @spec from_prefix(binary()) :: {binary(), binary()}
  def from_prefix(prefix) when is_binary(prefix), do: {prefix, Key.strinc(prefix)}

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
