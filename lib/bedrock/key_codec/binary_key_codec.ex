defmodule Bedrock.KeyCodec.BinaryKeyCodec do
  @behaviour Bedrock.KeyCodec

  @impl true
  def encode_key(key) when is_binary(key), do: {:ok, key}
  def encode_key(_), do: {:error, :invalid_key}

  @impl true
  def decode_key(encoded_key) when is_binary(encoded_key), do: {:ok, encoded_key}
  def decode_key(_), do: {:error, :invalid_key}
end
