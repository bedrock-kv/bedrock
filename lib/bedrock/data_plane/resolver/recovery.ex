defmodule Bedrock.DataPlane.Resolver.Recovery do
  @moduledoc """
  Recovery logic for rebuilding Resolver interval trees from transaction logs.

  Pulls committed transactions from log servers and applies their writes to
  reconstruct the interval tree state needed for conflict detection. Handles
  batch processing of encoded transactions and coordination with the lock
  token system during recovery scenarios.
  """
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Transaction
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()} | {:error, {:failed_to_pull_log, Log.id(), Log.pull_error()}}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, source_logs, first_version, last_version) do
    Enum.reduce_while(source_logs, t, fn {log_id, log_ref}, t ->
      case pull_transactions(t.tree, log_ref, first_version, last_version) do
        {:ok, tree} ->
          {:cont, %{t | tree: tree}}

        {:error, reason} ->
          {:halt, {:error, {:failed_to_pull_log, log_id, reason}}}
      end
    end)
    |> case do
      {:error, _reason} = error ->
        error

      %{} = t ->
        {:ok, %{t | last_version: last_version, oldest_version: first_version, mode: :running}}
    end
  end

  @spec pull_transactions(
          tree :: Tree.t() | nil,
          log_to_pull :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, Tree.t()}
          | Log.pull_errors()
          | {:error, {:log_unavailable, Log.ref()}}
  def pull_transactions(tree, nil, _first_version, _last_version),
    do: {:ok, tree}

  def pull_transactions(tree, _, first_version, last_version)
      when first_version == last_version,
      do: {:ok, tree}

  def pull_transactions(tree, log_to_pull, first_version, last_version) do
    case Log.pull(log_to_pull, first_version, last_version: last_version) do
      {:ok, []} ->
        {:ok, tree}

      {:ok, transactions} ->
        {tree, last_version_in_batch} = apply_batch_of_transactions(tree, transactions)
        pull_transactions(tree, log_to_pull, last_version_in_batch, last_version)

      {:error, :unavailable} ->
        {:error, {:log_unavailable, log_to_pull}}
    end
  end

  @doc """
  Applies a batch of encoded transactions to the tree. All transactions are expected
  to be in binary encoded format as returned by Log.pull.

  Returns a tuple of {updated_tree, last_version} where last_version is the
  version of the last transaction applied, or nil if no transactions were applied.
  """
  @spec apply_batch_of_transactions(Tree.t() | nil, [EncodedTransaction.t()]) ::
          {Tree.t(), Bedrock.version() | nil}
  def apply_batch_of_transactions(tree, transactions) do
    transactions
    |> Enum.reduce(
      {tree, nil},
      fn encoded_transaction, {tree, _last_version} ->
        decoded_transaction = EncodedTransaction.decode!(encoded_transaction)
        apply_transaction(tree, decoded_transaction)
      end
    )
  end

  @doc """
  Applies a decoded transaction to the tree. The transaction must be in the
  format {version, writes} where writes is a map of key-value pairs.

  Note: This function expects decoded transactions only. For encoded binary
  transactions, use apply_batch_of_transactions/2 which handles decoding.
  """
  @spec apply_transaction(Tree.t() | nil, Transaction.t()) :: {Tree.t(), Bedrock.version()}
  def apply_transaction(tree, {write_version, writes}) do
    {writes |> Enum.reduce(tree, &Tree.insert(&2, &1, write_version)), write_version}
  end
end
