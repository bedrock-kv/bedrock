defmodule Bedrock.DataPlane.CommitProxy.Batch do
  @moduledoc """
  Represents a batch of transactions being processed by the commit proxy.
  """

  alias Bedrock.DataPlane.Transaction

  @type reply_fn :: ({:ok, Bedrock.version(), index :: non_neg_integer()} | {:error, :abort} -> :ok)

  @typedoc """
  Who is committing, carried per transaction: it decides the legal write
  range during pipeline validation (user commits end at the system boundary,
  system commits at the end of the keyspace).
  """
  @type commit_mode :: :user | :system

  @type t :: %__MODULE__{
          started_at: Bedrock.timestamp_in_ms(),
          finalized_at: Bedrock.timestamp_in_ms() | nil,
          last_commit_version: Bedrock.version(),
          commit_version: Bedrock.version(),
          known_committed_version: Bedrock.version() | nil,
          n_transactions: non_neg_integer(),
          buffer: [{index :: non_neg_integer(), reply_fn(), Transaction.encoded(), commit_mode()}]
        }
  defstruct started_at: nil,
            finalized_at: nil,
            last_commit_version: nil,
            commit_version: nil,
            known_committed_version: nil,
            n_transactions: 0,
            buffer: []

  @spec new_batch(
          Bedrock.timestamp_in_ms(),
          last_commit_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          known_committed_version :: Bedrock.version() | nil
        ) :: t()
  def new_batch(started_at, last_commit_version, commit_version, known_committed_version \\ nil) do
    %__MODULE__{
      started_at: started_at,
      last_commit_version: last_commit_version,
      commit_version: commit_version,
      known_committed_version: known_committed_version,
      n_transactions: 0,
      buffer: []
    }
  end

  @spec transactions_in_order(t()) :: [
          {index :: non_neg_integer(), reply_fn(), Transaction.encoded(), commit_mode()}
        ]
  def transactions_in_order(t), do: Enum.reverse(t.buffer)

  @spec all_callers(t()) :: [reply_fn()]
  def all_callers(t), do: Enum.map(t.buffer, &elem(&1, 1))

  @spec add_transaction(t(), Transaction.encoded(), reply_fn(), commit_mode()) :: t()
  def add_transaction(t, transaction, reply_fn, commit_mode)
      when is_binary(transaction) and commit_mode in [:user, :system] do
    index = t.n_transactions
    %{t | buffer: [{index, reply_fn, transaction, commit_mode} | t.buffer], n_transactions: index + 1}
  end

  @spec transaction_count(t()) :: non_neg_integer()
  def transaction_count(t), do: t.n_transactions

  @spec set_finalized_at(t(), Bedrock.timestamp_in_ms()) :: t()
  def set_finalized_at(t, finalized_at), do: %{t | finalized_at: finalized_at}
end
