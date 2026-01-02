defmodule Bedrock.JobQueue.Expirable do
  @moduledoc """
  Shared expiration helpers for lease-like structs.

  Any struct with an `expires_at` field can use these functions.
  """

  @doc """
  Returns true if the struct has expired.
  """
  @spec expired?(%{expires_at: non_neg_integer()}) :: boolean()
  def expired?(%{expires_at: exp}) do
    System.system_time(:millisecond) >= exp
  end

  @doc """
  Returns the remaining time in milliseconds.
  Returns 0 if expired.
  """
  @spec remaining_ms(%{expires_at: non_neg_integer()}) :: non_neg_integer()
  def remaining_ms(%{expires_at: exp}) do
    max(0, exp - System.system_time(:millisecond))
  end
end
