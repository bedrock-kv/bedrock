defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Putting do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @spec set_key(State.t(), Bedrock.key(), nil) :: State.t()
  def set_key(t, key, nil) do
    t.tx
    |> Tx.clear(key)
    |> then(&%{t | tx: &1})
  end

  @spec set_key(State.t(), Bedrock.key(), Bedrock.value()) :: State.t()
  def set_key(t, key, value) do
    t.tx
    |> Tx.set(key, value)
    |> then(&%{t | tx: &1})
  end
end
