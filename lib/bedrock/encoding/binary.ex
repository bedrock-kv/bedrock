defmodule Bedrock.Encoding.None do
  @moduledoc false

  @behaviour Bedrock.Encoding

  @spec pack(value :: binary()) :: binary()
  def pack(value) when is_binary(value), do: value

  @spec unpack(packed :: binary()) :: binary()
  def unpack(packed), do: packed
end
