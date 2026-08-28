defmodule Bedrock.Encoding.BERT do
  @moduledoc """
  Erlang term serialization, for values that are arbitrary Elixir terms.

      iex> Bedrock.Encoding.BERT.unpack(Bedrock.Encoding.BERT.pack(%{cents: 500}))
      %{cents: 500}

  ## Use this for values, not keys

  The external term format is **not order-preserving**: the byte ordering of
  two packed terms says nothing about the ordering of the terms themselves. A
  keyspace using it as a `:key_encoding` would still store and fetch
  individual keys correctly, but range reads would return them in an order
  unrelated to their logical one. Reach for `Bedrock.Encoding.Tuple` when the
  ordering matters, which for keys it almost always does.

  ## Decoding untrusted bytes

  `unpack/1` calls `:erlang.binary_to_term/1` without the `:safe` option, so
  decoding attacker-controlled bytes can create atoms — an unbounded resource —
  and reconstruct pids and funs. Values written by your own application are
  fine; do not point this at bytes from somewhere you do not control.
  """

  @behaviour Bedrock.Encoding

  @spec pack(value :: any()) :: binary()
  def pack(value), do: :erlang.term_to_binary(value)

  @spec unpack(packed :: binary()) :: any()
  def unpack(packed), do: :erlang.binary_to_term(packed)
end
