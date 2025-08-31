defmodule Bedrock.DataPlane.CommitProxy.Batch do
  @moduledoc false

  alias Bedrock.DataPlane.Transaction

  @type reply_fn :: ({:ok, Bedrock.version()} | {:error, :abort} -> :ok)

  @type t :: %__MODULE__{
          started_at: Bedrock.timestamp_in_ms(),
          finalized_at: Bedrock.timestamp_in_ms() | nil,
          last_commit_version: Bedrock.version(),
          commit_version: Bedrock.version(),
          n_transactions: non_neg_integer(),
          buffer: [{index :: non_neg_integer(), reply_fn(), Transaction.encoded()}]
        }
  defstruct started_at: nil,
            finalized_at: nil,
            last_commit_version: nil,
            commit_version: nil,
            n_transactions: 0,
            buffer: []

  @spec new_batch(
          Bedrock.timestamp_in_ms(),
          last_commit_version :: Bedrock.version(),
          commit_version :: Bedrock.version()
        ) :: t()
  def new_batch(started_at, last_commit_version, commit_version) do
    %__MODULE__{
      started_at: started_at,
      last_commit_version: last_commit_version,
      commit_version: commit_version,
      n_transactions: 0,
      buffer: []
    }
  end

  @spec transactions_in_order(t()) :: [
          {index :: non_neg_integer(), reply_fn(), Transaction.encoded()}
        ]
  def transactions_in_order(t), do: Enum.reverse(t.buffer)

  @spec all_callers(t()) :: [reply_fn()]
  def all_callers(t), do: Enum.map(t.buffer, &elem(&1, 1))

  @spec add_transaction(t(), Transaction.encoded(), reply_fn()) :: t()
  def add_transaction(t, transaction, reply_fn) when is_binary(transaction) do
    index = t.n_transactions
    %{t | buffer: [{index, reply_fn, transaction} | t.buffer], n_transactions: index + 1}
  end

  @spec transaction_count(t()) :: non_neg_integer()
  def transaction_count(t), do: t.n_transactions

  @spec set_finalized_at(t(), Bedrock.timestamp_in_ms()) :: t()
  def set_finalized_at(t, finalized_at), do: %{t | finalized_at: finalized_at}
end
