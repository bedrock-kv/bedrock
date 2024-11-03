defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  @spec push(
          State.t(),
          Transaction.t(),
          last_commit_version :: Bedrock.version(),
          from :: GenServer.from()
        ) ::
          {:ok | :waiting, State.t()}
          | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(t, _, _, from) when t.mode == :locked and from != t.controller,
    do: {:error, :locked}

  def push(t, _, _, _) when t.mode != :running,
    do: {:error, :unavailable}

  def push(t, transaction, expected_version, from)
      when expected_version == t.last_version do
    {:ok, version} = apply_transaction(t.log, transaction)

    GenServer.reply(from, :ok)

    %{t | last_version: version, mode: :running}
    |> apply_pending_transactions()
  end

  def push(t, transaction, expected_version, from)
      when expected_version > t.last_version do
    {:waiting,
     %{
       t
       | pending_transactions:
           Map.put(
             t.pending_transactions,
             expected_version,
             {transaction, from}
           )
     }}
  end

  def push(_, _, _, _from), do: {:error, :tx_out_of_order}

  def apply_pending_transactions(t) do
    case Map.pop(t.pending_transactions, t.last_version) do
      {nil, _} ->
        {:ok, t}

      {{transaction, from}, pending_transactions} ->
        {:ok, version} = apply_transaction(t.log, transaction)

        GenServer.reply(from, :ok)

        %{
          t
          | last_version: version,
            pending_transactions: pending_transactions
        }
        |> apply_pending_transactions()
    end
  end

  def apply_transaction(log, transaction) do
    version = Transaction.version(transaction)
    true = :ets.insert_new(log, {version, transaction})
    {:ok, version}
  end
end
