defmodule Bedrock.DataPlane.Log.Limestone.Pushing do
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Log.Limestone.State
  alias Bedrock.DataPlane.Log.Limestone.Transactions

  @spec push(State.t(), Transaction.t(), prev_version :: Bedrock.version()) ::
          {:ok, State.t()} | {:error, :tx_out_of_order | :not_ready}
  def push(t, _transaction, _prev_version) when t.state not in [:ready, :locked],
    do: {:error, :not_ready}

  def push(t, _transaction, prev_version) when t.last_version != prev_version,
    do: {:error, :tx_out_of_order}

  def push(t, transaction, _prev_version) do
    Transactions.append!(t.transactions, transaction)
    {:ok, %{t | last_version: Transaction.version(transaction)}}
  end
end
