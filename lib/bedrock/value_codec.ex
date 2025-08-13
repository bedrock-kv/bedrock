defmodule Bedrock.ValueCodec do
  @moduledoc false

  @callback encode_value(term()) :: {:ok, binary()}
  @callback decode_value(binary()) :: {:ok, term()}
end
