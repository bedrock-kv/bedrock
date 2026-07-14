defmodule Bedrock.Encoding.BERT do
  @moduledoc false

  @behaviour Bedrock.Encoding

  @spec pack(value :: any()) :: binary()
  def pack(value), do: :erlang.term_to_binary(value)

  @spec unpack(packed :: binary()) :: any()
  def unpack(packed), do: :erlang.binary_to_term(packed)
end
