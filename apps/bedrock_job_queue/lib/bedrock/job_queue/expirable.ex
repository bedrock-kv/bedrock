defmodule Bedrock.JobQueue.Expirable do
  @moduledoc """
  Shared expiration helpers for lease-like structs.

  Any struct with an `expires_at` field can use these functions.
  """

  @doc """
  Returns true if the struct has expired.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec expired?(%{expires_at: non_neg_integer()}, keyword()) :: boolean()
  def expired?(struct, opts \\ [])

  def expired?(%{expires_at: exp}, opts), do: Keyword.get(opts, :now, System.system_time(:millisecond)) >= exp

  @doc """
  Returns the remaining time in milliseconds.
  Returns 0 if expired.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec remaining_ms(%{expires_at: non_neg_integer()}, keyword()) :: non_neg_integer()
  def remaining_ms(struct, opts \\ [])

  def remaining_ms(%{expires_at: exp}, opts),
    do: max(0, exp - Keyword.get(opts, :now, System.system_time(:millisecond)))
end
