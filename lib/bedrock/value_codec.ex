defmodule Bedrock.ValueCodec do
  @callback encode_value(term()) :: {:ok, binary()}
  @callback decode_value(binary()) :: {:ok, term()}
end
