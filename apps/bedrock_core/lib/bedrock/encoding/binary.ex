defmodule Bedrock.Encoding.None do
  @moduledoc false

  @behaviour Bedrock.Encoding

  def pack(value) when is_binary(value), do: value
  def unpack(packed), do: packed
end
