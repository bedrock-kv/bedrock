defmodule Bedrock.Key do
  @moduledoc """
  Utilities for working with binary keys in Bedrock.

  Binary keys in Bedrock are ordered lexicographically, and this module provides
  utilities for key manipulation, ordering operations, and tuple packing/unpacking.

  ## Tuple Packing

  This module provides tuple packing and unpacking functionality that allows
  complex data structures (tuples, integers, floats, binaries, nil) to be
  encoded as binary keys that maintain lexicographic ordering.
  """

  @type t :: binary()

  @doc """
  Returns the next possible key after the given key in lexicographic order.

  This is useful for creating exclusive upper bounds in range operations.
  For example, if you want all keys that start with "prefix", you would
  use the range from "prefix" to `key_after("prefix")`.

  ## Examples

      iex> Bedrock.Key.key_after("abc")
      "abc\\0"

      iex> Bedrock.Key.key_after("")
      "\\0"

  """
  @spec key_after(Bedrock.key()) :: binary()
  def key_after(key) when is_binary(key), do: key <> <<0>>

  def to_range(key) when is_binary(key), do: {key, key_after(key)}

  @doc """
  Returns the lexicographically next key after the given key by incrementing
  the last non-0xFF byte.

  This is useful for creating tight upper bounds in prefix ranges. Unlike
  `key_after/1` which appends a null byte, `strinc/1` creates the minimal
  lexicographically next key.

  ## Examples

      iex> Bedrock.Key.strinc("abc")
      "abd"

      iex> Bedrock.Key.strinc(<<0, 1, 2>>)
      <<0, 1, 3>>
      iex> Bedrock.Key.strinc("hello")
      "hellp"

  ## Errors

  Raises an `ArgumentError` if the key contains only 0xFF bytes.

      iex> Bedrock.Key.strinc(<<0xFF, 0xFF>>)
      ** (ArgumentError) Key must contain at least one byte not equal to 0xFF

  """
  @spec strinc(binary()) :: binary()
  def strinc(key) when is_binary(key) do
    prefix = rstrip_ff(key)
    prefix_len = byte_size(prefix)
    head = binary_part(prefix, 0, prefix_len - 1)
    tail = :binary.at(prefix, prefix_len - 1)
    head <> <<tail + 1>>
  end

  defp rstrip_ff(<<>>), do: raise(ArgumentError, "Key must contain at least one byte not equal to 0xFF")

  defp rstrip_ff(key) do
    key_len = byte_size(key)

    case :binary.at(key, key_len - 1) do
      0xFF -> key |> binary_part(0, key_len - 1) |> rstrip_ff()
      _ -> key
    end
  end
end
