defmodule Bedrock.Raft.TransactionID do
  @moduledoc """
  Transactions are the basic unit of ordering in the Raft log. They are
  represented as a tuple of the election term and the index of the transaction
  or as a binary-encoded version of the tuple. The binary encoding is designed
  to sort in the same order as the tuple transaction so that simple comparisons
  can be used for either form.
  """
  alias Bedrock.Raft

  @spec new(Raft.election_term(), Raft.index()) :: Raft.tuple_transaction_id()
  def new(term, index), do: {term, index}

  @spec term(Raft.transaction_id()) :: Raft.election_term()
  def term({term, _index}), do: term
  def term(t) when is_binary(t), do: decode(t) |> elem(0)

  @spec index(Raft.transaction_id()) :: Raft.index()
  def index({_term, index}), do: index
  def index(t) when is_binary(t), do: decode(t) |> elem(1)

  @doc """
  Encode a transaction tuple into a binary. We encode each integer part of the
  transaction into a binary, in such a way that the binary-encoded transaction
  will sort in the same order as the tuple transaction.

  Binary transactions pass-through unchanged.
  """
  @spec encode(Raft.transaction_id()) :: Raft.binary_transaction_id()
  def encode({term, index}), do: pack(term) <> pack(index)
  def encode(transaction_id) when is_binary(transaction_id), do: transaction_id

  def pack(v) when v <= 0xFF, do: <<0x15, v::unsigned-big-integer-size(8)>>
  def pack(v) when v <= 0xFFFF, do: <<0x16, v::unsigned-big-integer-size(16)>>
  def pack(v) when v <= 0xFFFFFF, do: <<0x17, v::unsigned-big-integer-size(24)>>
  def pack(v) when v <= 0xFFFFFFFF, do: <<0x18, v::unsigned-big-integer-size(32)>>
  def pack(v) when v <= 0xFFFFFFFFFF, do: <<0x19, v::unsigned-big-integer-size(40)>>
  def pack(v) when v <= 0xFFFFFFFFFFFF, do: <<0x1A, v::unsigned-big-integer-size(48)>>
  def pack(v) when v <= 0xFFFFFFFFFFFFFF, do: <<0x1B, v::unsigned-big-integer-size(56)>>
  def pack(v), do: <<0x1C, v::unsigned-big-integer-size(64)>>

  @doc """
  Decode a binary-encoded transaction into a transaction tuple. Tuple
  transactions pass-through unchanged.
  """
  @spec decode(Raft.transaction_id()) :: Raft.tuple_transaction_id()
  def decode({_term, _index} = transaciton_id), do: transaciton_id

  def decode(value) when is_binary(value) do
    {term, encoded_index} = unpack(value)
    {index, <<>>} = unpack(encoded_index)
    {term, index}
  end

  def unpack(<<0x15, v::unsigned-big-integer-size(8)>> <> r), do: {v, r}
  def unpack(<<0x16, v::unsigned-big-integer-size(16)>> <> r), do: {v, r}
  def unpack(<<0x17, v::unsigned-big-integer-size(24)>> <> r), do: {v, r}
  def unpack(<<0x18, v::unsigned-big-integer-size(32)>> <> r), do: {v, r}
  def unpack(<<0x19, v::unsigned-big-integer-size(40)>> <> r), do: {v, r}
  def unpack(<<0x1A, v::unsigned-big-integer-size(48)>> <> r), do: {v, r}
  def unpack(<<0x1B, v::unsigned-big-integer-size(56)>> <> r), do: {v, r}
  def unpack(<<0x1C, v::unsigned-big-integer-size(64)>> <> r), do: {v, r}
end
