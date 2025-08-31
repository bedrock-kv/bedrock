defmodule Bedrock.DataPlane.Resolver.ConflictResolution do
  @moduledoc """
  Core conflict detection logic for the Resolver using interval trees.

  Processes transaction batches to detect read-write and write-write conflicts by
  checking for overlapping key ranges across different versions. Returns updated
  interval trees and lists of aborted transaction indices.

  Each transaction is checked against the interval tree to determine if its reads
  or writes conflict with previously committed transactions at later versions.
  """
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Transaction

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
  @spec resolve(Tree.t(), [Resolver.transaction_summary()], write_version :: Bedrock.version()) ::
          {Tree.t(), aborted :: [non_neg_integer()]}
  def resolve(tree, [], _), do: {tree, []}

  def resolve(tree, transactions, write_version) do
    {tree, failed_indexes} =
      transactions
      |> Enum.with_index()
      |> Enum.reduce({tree, []}, fn {tx, index}, {tree, failed} ->
        tree
        |> try_to_resolve_transaction(tx, write_version)
        |> case do
          {:ok, tree} -> {tree, failed}
          :abort -> {tree, [index | failed]}
        end
      end)

    {tree, failed_indexes}
  end

  @spec try_to_resolve_transaction(Tree.t(), Resolver.transaction_summary(), Bedrock.version()) ::
          {:ok, Tree.t()} | :abort
  def try_to_resolve_transaction(tree, transaction, write_version) do
    if conflict?(tree, transaction, write_version) do
      :abort
    else
      {:ok, apply_transaction(tree, transaction, write_version)}
    end
  end

  @spec conflict?(Tree.t(), Resolver.transaction_summary(), Bedrock.version()) :: boolean()
  def conflict?(tree, transaction, write_version) do
    {read_info, writes} = extract_conflicts(transaction)

    write_conflict?(tree, writes, write_version) or
      read_write_conflict?(tree, read_info)
  end

  # Extract conflicts from binary transaction
  defp extract_conflicts(binary_transaction) when is_binary(binary_transaction) do
    # Extract write conflicts from binary transaction
    writes =
      case Transaction.extract_write_conflicts(binary_transaction) do
        {:ok, write_conflicts} -> Enum.map(write_conflicts, fn {key, _end_key} -> key end)
        {:error, _} -> []
      end

    # Extract read conflicts from binary transaction
    read_info =
      case Transaction.extract_read_conflicts(binary_transaction) do
        {:ok, {nil, []}} ->
          nil

        {:ok, {read_version, read_conflicts}} ->
          read_keys = Enum.map(read_conflicts, fn {key, _end_key} -> key end)
          {read_version, read_keys}

        {:error, _} ->
          nil
      end

    {read_info, writes}
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

  @spec apply_transaction(Tree.t(), Resolver.transaction_summary(), Bedrock.version()) :: Tree.t()
  def apply_transaction(tree, transaction, write_version) do
    {_read_info, writes} = extract_conflicts(transaction)
    Enum.reduce(writes, tree, &Tree.insert(&2, &1, write_version))
  end

  @spec remove_old_transactions(Tree.t(), Bedrock.version()) :: Tree.t()
  def remove_old_transactions(tree, min_version), do: Tree.filter_by_value(tree, &(&1 > min_version))
end
