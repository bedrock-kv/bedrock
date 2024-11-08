defmodule Bedrock.Internal.Time do
  @doc """
  """
  @spec now() :: DateTime.t()
  def now, do: DateTime.utc_now()

  @spec now_in_ms() :: Bedrock.timestamp_in_ms()
  def now_in_ms, do: :os.system_time(:millisecond)
end
