defmodule Bedrock.Internal.Time do
  @doc """
  """
  @spec now() :: DateTime.t()
  def now, do: DateTime.utc_now()
end
