defmodule Bedrock.DataPlane.Resolver.Recovery do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Transaction

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version() | :start,
          last_version :: Bedrock.version_vector()
        ) ::
          {:ok, State.t()} | {:error, reason :: term()}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, source_log, first_version, last_version) do
    case pull_transactions(t.tree, source_log, first_version, last_version) do
      {:ok, tree} ->
        {:ok, %{t | tree: tree, oldest_version: first_version, last_version: last_version}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec pull_transactions(
          tree :: Tree.t() | nil,
          log_to_pull :: Log.ref(),
          first_version :: Bedrock.version() | :start,
          last_version :: Bedrock.version()
        ) ::
          {:ok, Tree.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_to_pull :: Log.ref()}}
  def pull_transactions(tree, nil, :start, 0),
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
        {:error, {:source_log_unavailable, log_to_pull}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec apply_batch_of_transactions(Tree.t(), [Transaction.t()]) :: {Tree.t(), Bedrock.version()}
  def apply_batch_of_transactions(tree, transactions) do
    transactions
    |> Enum.reduce(
      {tree, nil},
      fn transaction, {tree, _last_version} ->
        apply_transaction(tree, transaction)
      end
    )
  end

  @spec apply_transaction(Tree.t(), Transaction.t()) :: {Tree.t(), Bedrock.version()}
  def apply_transaction(tree, transaction) do
    {write_version, writes} = transaction |> Transaction.decode()
    {writes |> Enum.reduce(tree, &Tree.insert(&2, &1, write_version)), write_version}
  end
end
