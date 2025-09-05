defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Putting do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @spec do_put(State.t(), Bedrock.key(), nil) :: {:ok, State.t()} | :key_error
  def do_put(t, key, nil) do
    if is_binary(key) do
      t.tx
      |> Tx.clear(key)
      |> then(&{:ok, %{t | tx: &1}})
    else
      :key_error
    end
  end

  @spec do_put(State.t(), Bedrock.key(), Bedrock.value()) :: {:ok, State.t()} | :key_error
  def do_put(t, key, value) do
    if is_binary(key) do
      t.tx
      |> Tx.set(key, value)
      |> then(&{:ok, %{t | tx: &1}})
    else
      :key_error
    end
  end
end
