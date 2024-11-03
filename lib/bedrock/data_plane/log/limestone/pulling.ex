defmodule Bedrock.DataPlane.Log.Limestone.Pulling do
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Log.Limestone.Subscriptions
  alias Bedrock.DataPlane.Log.Limestone.Transactions
  alias Bedrock.DataPlane.Log.Limestone.State

  @spec pull(
          t :: State.t(),
          last_version :: Bedrock.version(),
          count :: pos_integer(),
          opts :: [
            subscriber_id: String.t(),
            last_durable_version: Bedrock.version()
          ]
        ) ::
          {:ok, [] | [Transaction.t()]} | {:error, :not_ready | :tx_too_new}
  def pull(t, _last_version, _count, _opts)
      when t.state != :ready,
      do: {:error, :not_ready}

  def pull(t, last_version, _count, _opts)
      when last_version > t.last_version,
      do: {:error, :tx_too_new}

  def pull(t, last_version, count, subscriber_id: id = opts) do
    last_durable_version = opts[:last_durable_version] || :start

    t.transactions
    |> Transactions.get(last_version, count)
    |> case do
      [] ->
        t.subscriptions |> Subscriptions.update(id, last_version, last_durable_version)
        {:ok, []}

      transactions ->
        last_version = transactions |> List.last() |> Transaction.version()
        t.subscriptions |> Subscriptions.update(id, last_version, last_durable_version)
        {:ok, transactions}
    end
  end

  def pull(t, last_version, count, _opts) do
    {:ok,
     t.transactions
     |> Transactions.get(last_version, count)}
  end
end
