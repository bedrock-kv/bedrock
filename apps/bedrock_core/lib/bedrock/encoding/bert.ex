defmodule Bedrock.Encoding.BERT do
  @moduledoc false

  @behaviour Bedrock.Encoding

  def pack(value), do: :erlang.term_to_binary(value)
  def unpack(packed), do: :erlang.binary_to_term(packed)
end
