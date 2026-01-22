defmodule Bedrock.Internal.Id do
  @moduledoc """
  Centralized ID generation utilities.

  Generates cryptographically random IDs encoded as base32 lowercase strings.
  Used for worker IDs, cluster IDs, and other unique identifiers.

  ## Examples

      iex> id = Bedrock.Internal.Id.random()
      iex> String.length(id)
      8

      iex> id = Bedrock.Internal.Id.random(4)
      iex> String.length(id)
      7
  """

  @default_bytes 5

  @doc """
  Generates a random ID with default entropy (5 bytes = 8 characters).
  """
  @spec random() :: String.t()
  def random, do: random(@default_bytes)

  @doc """
  Generates a random ID with the specified number of entropy bytes.

  The resulting string length is `ceil(bytes * 8 / 5)` characters.
  """
  @spec random(bytes :: pos_integer()) :: String.t()
  def random(bytes) when is_integer(bytes) and bytes > 0 do
    bytes
    |> :crypto.strong_rand_bytes()
    |> Base.encode32(case: :lower, padding: false)
  end
end
