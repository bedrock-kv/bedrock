defprotocol Bedrock.Raft.Log do
  @moduledoc """
  The interface for the Raft transaction log.
  """

  @type t :: any()

  alias Bedrock.Raft

  @doc """
  Append the given block of transactions to the log, starting at the given
  previous transaction. If we can't find the previous transaction, we return an
  error.
  """
  @spec append_transactions(
          t(),
          prev :: Raft.transaction(),
          transactions :: [Raft.transaction()]
        ) ::
          {:ok, t()} | {:error, :prev_transaction_not_found}
  def append_transactions(t, prev_transaction, transactions)

  @doc """
  Get the initial transaction for the log.
  """
  @spec initial_transaction(t()) :: Raft.transaction()
  def initial_transaction(t)

  @doc """
  Mark all transactions up to and including the given transaction as committed.
  """
  @spec commit_up_to(t(), Raft.transaction()) :: {:ok, t()}
  def commit_up_to(t, transaction)

  @doc """
  Get the newest transaction in the log.
  """
  @spec newest_transaction(t()) :: Raft.transaction()
  def newest_transaction(t)

  @doc """
  Get the newest transaction in the log that has been safely appended to the
  logs of a quorum of peers in the cluster.
  """
  @spec newest_safe_transaction(t()) :: Raft.transaction()
  def newest_safe_transaction(t)

  @doc """
  Does the log contain the given transaction?
  """
  @spec has_transaction?(t(), Raft.transaction()) :: boolean()
  def has_transaction?(t, transaction)

  @doc """
  Get a list of transactions that have occurred up to the the given transaction.
  """
  @spec transactions_to(t(), to :: Raft.transaction() | :newest | :newest_safe) :: [
          Raft.transaction()
        ]
  def transactions_to(t, to)

  @doc """
  Get a list of transactions that have occurred using the given transaction
  as a starting point -- not inclusive of the starting point.
  """
  @spec transactions_from(
          t(),
          from :: Raft.transaction(),
          to :: Raft.tranasaction() | :newest | :newest_safe
        ) :: [Raft.transaction()]
  def transactions_from(t, from, to)
end
