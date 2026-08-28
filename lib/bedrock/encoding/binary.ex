defmodule Bedrock.Encoding.None do
  @moduledoc """
  The identity encoding: binaries in, the same binaries out.

  Use it when your keys or values are already the bytes you want stored, and
  you want that stated explicitly rather than left to the default. A keyspace
  with no encoding configured behaves identically.

      iex> Bedrock.Encoding.None.pack("already bytes")
      "already bytes"

  `pack/1` accepts binaries only and raises `FunctionClauseError` on anything
  else — the encoding will not silently stringify a term for you. Being the
  identity, it is trivially order-preserving and therefore safe for keys.
  """

  @behaviour Bedrock.Encoding

  @spec pack(value :: binary()) :: binary()
  def pack(value) when is_binary(value), do: value

  @spec unpack(packed :: binary()) :: binary()
  def unpack(packed), do: packed
end
