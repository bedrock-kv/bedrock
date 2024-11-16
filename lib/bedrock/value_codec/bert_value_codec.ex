defmodule Bedrock.ValueCodec.BertValueCodec do
  @behaviour Bedrock.ValueCodec

  @impl true
  def encode_value(value), do: {:ok, :erlang.term_to_binary(value)}

  @impl true
  def decode_value(encoded_value), do: {:ok, :erlang.binary_to_term(encoded_value)}
end
