defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  @type transaction_with_ack_fn :: {Transaction.t(), ack_fn()}
  @type ack_fn :: (-> :ok)

  @spec push(
          State.t(),
          expected_version :: Bedrock.version(),
          transaction_with_ack_fn()
        ) ::
          {:ok | :waiting, State.t()}
          | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(t, _, from) when t.mode == :locked and from != t.director,
    do: {:error, :locked}

  def push(t, _, _) when t.mode != :running,
    do: {:error, :unavailable}

  def push(t, expected_version, transaction_with_ack_fn)
      when expected_version == t.last_version do
    {:ok, version} = apply_transaction(t.log, transaction_with_ack_fn)

    %{t | last_version: version}
    |> apply_pending_transactions()
  end

  def push(t, expected_version, transaction_with_ack_fn)
      when expected_version > t.last_version do
    {:waiting,
     %{
       t
       | pending_transactions:
           Map.put(
             t.pending_transactions,
             expected_version,
             transaction_with_ack_fn
           )
     }}
  end

  def push(_, _, _), do: {:error, :tx_out_of_order}

  def apply_pending_transactions(t) do
    case Map.pop(t.pending_transactions, t.last_version) do
      {nil, _} ->
        {:ok, t}

      {transaction_with_ack_fn, pending_transactions} ->
        {:ok, version} = apply_transaction(t.log, transaction_with_ack_fn)

        %{
          t
          | last_version: version,
            pending_transactions: pending_transactions
        }
        |> apply_pending_transactions()
    end
  end

  def apply_transaction(log, {transaction, ack_fn}) do
    version = Transaction.version(transaction)
    key_values = Transaction.key_values(transaction)

    case :ets.insert_new(log, {version, key_values}) do
      true ->
        ack_fn.()
        {:ok, version}

      false ->
        {:error, :insert_failed}
    end
  end
end
