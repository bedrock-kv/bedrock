defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Mutations do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @spec set_key(State.t(), Bedrock.key(), nil | Bedrock.value()) :: State.t()
  def set_key(t, key, value, opts \\ [])

  def set_key(t, key, nil, opts) do
    t.tx
    |> Tx.clear(key, opts)
    |> then(&%{t | tx: &1})
  end

  def set_key(t, key, value, opts) do
    t.tx
    |> Tx.set(key, value, opts)
    |> then(&%{t | tx: &1})
  end

  # Generic atomic operation wrapper
  defp atomic_op(t, operation, key, value) do
    t.tx
    |> Tx.atomic_operation(key, operation, value)
    |> then(&%{t | tx: &1})
  end

  @spec add(State.t(), Bedrock.key(), binary()) :: State.t()
  def add(t, key, value), do: atomic_op(t, :add, key, value)

  @spec min(State.t(), Bedrock.key(), binary()) :: State.t()
  def min(t, key, value), do: atomic_op(t, :min, key, value)

  @spec max(State.t(), Bedrock.key(), binary()) :: State.t()
  def max(t, key, value), do: atomic_op(t, :max, key, value)

  @spec bit_and(State.t(), Bedrock.key(), binary()) :: State.t()
  def bit_and(t, key, value), do: atomic_op(t, :bit_and, key, value)

  @spec bit_or(State.t(), Bedrock.key(), binary()) :: State.t()
  def bit_or(t, key, value), do: atomic_op(t, :bit_or, key, value)

  @spec bit_xor(State.t(), Bedrock.key(), binary()) :: State.t()
  def bit_xor(t, key, value), do: atomic_op(t, :bit_xor, key, value)

  @spec byte_min(State.t(), Bedrock.key(), binary()) :: State.t()
  def byte_min(t, key, value), do: atomic_op(t, :byte_min, key, value)

  @spec byte_max(State.t(), Bedrock.key(), binary()) :: State.t()
  def byte_max(t, key, value), do: atomic_op(t, :byte_max, key, value)

  @spec append_if_fits(State.t(), Bedrock.key(), binary()) :: State.t()
  def append_if_fits(t, key, value), do: atomic_op(t, :append_if_fits, key, value)

  @spec compare_and_clear(State.t(), Bedrock.key(), binary()) :: State.t()
  def compare_and_clear(t, key, expected), do: atomic_op(t, :compare_and_clear, key, expected)

  @spec add_read_conflict_key(State.t(), Bedrock.key()) :: State.t()
  def add_read_conflict_key(t, key) do
    t.tx
    |> Tx.add_read_conflict_key(key)
    |> then(&%{t | tx: &1})
  end

  @spec add_write_conflict_range(State.t(), Bedrock.key(), Bedrock.key()) :: State.t()
  def add_write_conflict_range(t, start_key, end_key) do
    t.tx
    |> Tx.add_write_conflict_range(start_key, end_key)
    |> then(&%{t | tx: &1})
  end
end
