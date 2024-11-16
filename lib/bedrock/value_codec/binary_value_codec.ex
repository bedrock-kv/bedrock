defmodule Bedrock.ValueCodec.BinaryValueCodec do
  @behaviour Bedrock.ValueCodec

  @impl true
  def encode_value(value) when is_binary(value), do: {:ok, value}

  @impl true
  def decode_value(encoded_value), do: {:ok, encoded_value}
end
