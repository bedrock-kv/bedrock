defmodule Bedrock.Internal.Time do
  @moduledoc """
  Utility functions for retrieving the current system time in various formats.
  """

  @doc "Return the current system time (UTC)."
  @spec now() :: DateTime.t()
  def now, do: DateTime.utc_now()

  @doc "Return the current system time in milliseconds."
  @spec now_in_ms() :: Bedrock.timestamp_in_ms()
  def now_in_ms, do: :os.system_time(:millisecond)

  @doc "Return the current system monotic time in milliseconds."
  @spec monotonic_now_in_ms() :: Bedrock.timestamp_in_ms()
  def monotonic_now_in_ms, do: :erlang.monotonic_time(:millisecond)

  @doc "Return the elapsed time in milliseconds since the given (monotonic) start time."
  @spec elapsed_monotonic_in_ms(Bedrock.timestamp_in_ms()) :: Bedrock.timestamp_in_ms()
  def elapsed_monotonic_in_ms(start_time), do: monotonic_now_in_ms() - start_time
end
