defmodule Bedrock.KeyCodec do
  @callback encode_key(term()) :: {:ok, binary()} | {:error, :invalid_key}
  @callback decode_key(binary()) :: {:ok, term()} | {:error, :invalid_key}
end
