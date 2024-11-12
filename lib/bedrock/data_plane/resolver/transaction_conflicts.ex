defmodule Bedrock.DataPlane.Resolver.ConflictResolution do
  @moduledoc """
  A conflict detection system with read and write versioning, using interval trees
  for efficient range-based conflict detection. Each transaction operates on a specific
  version of the database, and conflicts are detected based on version mismatches.
  """
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree

  @doc """
  Commits a batch of transactions to the interval tree, returning the updated
  tree and a list of the indexes of transactions that were aborted due to
  conflicts. Every transaction that can be applied, is.

  Each transaction is checked for conflicts using read and write versions.

  ## Parameters

    - tree: The interval tree of transactions to check against.
    - transactions: A list of transactions, each with read/write versions
      and operations (reads/writes) to resolve.

  ## Returns

    - A tuple with the updated tree and a list of transaction indexes that were aborted.

  Transactions are rolled back in the order they are processed when conflicts are detected.
  """
  @spec resolve(Tree.t(), [Resolver.transaction()], write_version :: Bedrock.version()) ::
          {Tree.t(), aborted :: [non_neg_integer()]}
  def resolve(tree, [], _), do: {tree, []}

  def resolve(tree, transactions, write_version) do
    {tree, failed_indexes} =
      transactions
      |> Enum.with_index()
      |> Enum.reduce({tree, []}, fn {tx, index}, {tree, failed} ->
        tree
        |> try_resolve_transaction(tx, write_version)
        |> case do
          {:ok, tree} -> {tree, failed}
          :abort -> {tree, [index | failed]}
        end
      end)

    {tree, failed_indexes}
  end

  @spec try_resolve_transaction(Tree.t(), Resolver.transaction(), Bedrock.version()) ::
          {:ok, Tree.t()} | :abort
  def try_resolve_transaction(tree, transaction, write_version) do
    if tree |> conflict?(transaction, write_version) do
      :abort
    else
      {:ok, tree |> apply_transaction(transaction, write_version)}
    end
  end

  @spec conflict?(Tree.t(), Resolver.transaction(), Bedrock.version()) :: boolean()
  def conflict?(tree, {read_info, writes}, write_version) do
    write_conflict?(tree, writes, write_version) or
      read_write_conflict?(tree, read_info)
  end

  @spec write_conflict?(Tree.t(), [Bedrock.key() | Bedrock.key_range()], Bedrock.version()) ::
          boolean()
  def write_conflict?(tree, writes, write_version) do
    predicate = version_lt(write_version)
    Enum.any?(writes, &Tree.overlap?(tree, &1, predicate))
  end

  @spec read_write_conflict?(
          Tree.t(),
          nil | {Bedrock.version(), [Bedrock.key() | Bedrock.key_range()]}
        ) ::
          boolean()
  def read_write_conflict?(_, nil), do: false

  def read_write_conflict?(tree, {read_version, reads}) do
    predicate = version_lt(read_version)
    Enum.any?(reads, &Tree.overlap?(tree, &1, predicate))
  end

  @spec version_lt(Bedrock.version()) :: (Bedrock.version() -> boolean())
  def version_lt(version), do: &(&1 > version)

  @spec apply_transaction(Tree.t(), Resolver.transaction(), Bedrock.version()) :: Tree.t()
  def apply_transaction(tree, {_, writes}, write_version),
    do: writes |> Enum.reduce(tree, &Tree.insert(&2, &1, write_version))

  @spec remove_old_transactions(Tree.t(), Bedrock.version()) :: Tree.t()
  def remove_old_transactions(tree, min_version),
    do: tree |> Tree.filter_by_value(&(&1 > min_version))
end
