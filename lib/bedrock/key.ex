defmodule Bedrock.Key do
  @moduledoc """
  Utilities for working with binary keys in Bedrock.

  Binary keys in Bedrock are ordered lexicographically, and this module provides
  utilities for key manipulation and ordering operations.
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
end
