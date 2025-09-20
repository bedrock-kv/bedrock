defmodule Bedrock.DataPlane.Resolver.ConflictResolution do
  @moduledoc """
  Core conflict detection logic for the Resolver using interval trees.

  Processes transaction batches to detect read-write and write-write conflicts by
  checking for overlapping key ranges across different versions. Returns updated
  interval trees and lists of aborted transaction indices.

  Each transaction is checked against the interval tree to determine if its reads
  or writes conflict with previously committed transactions at later versions.
  """
  alias Bedrock.DataPlane.Resolver.VersionedConflicts
  alias Bedrock.DataPlane.Transaction

  @doc """
  Commits a batch of transactions using versioned conflicts, returning the updated
  conflicts structure and a list of the indexes of transactions that were aborted due to
  conflicts. Every transaction that can be applied, is.

  Each transaction is checked for conflicts using read and write versions.

  ## Parameters

    - conflicts: The versioned conflicts structure to check against.
    - transactions: A list of transactions, each with read/write versions
      and operations (reads/writes) to resolve.

  ## Returns

    - A tuple with the updated conflicts and a list of transaction indexes that were aborted.

  Transactions are rolled back in the order they are processed when conflicts are detected.
  """
  @spec resolve(VersionedConflicts.t(), [Transaction.encoded()], write_version :: Bedrock.version()) ::
          {VersionedConflicts.t(), aborted :: [non_neg_integer()]}
  def resolve(conflicts, [], _), do: {conflicts, []}

  def resolve(conflicts, transactions, write_version) do
    {final_conflicts, failed_indexes} =
      transactions
      |> Enum.with_index()
      |> Enum.reduce({conflicts, []}, fn {tx, index}, {acc_conflicts, failed} ->
        acc_conflicts
        |> try_to_resolve_transaction(tx, write_version)
        |> case do
          {:ok, new_conflicts} -> {new_conflicts, failed}
          :abort -> {acc_conflicts, [index | failed]}
        end
      end)

    {final_conflicts, failed_indexes}
  end

  @spec try_to_resolve_transaction(VersionedConflicts.t(), Transaction.encoded(), Bedrock.version()) ::
          {:ok, VersionedConflicts.t()} | :abort
  def try_to_resolve_transaction(conflicts, transaction, write_version) do
    if conflict?(conflicts, transaction, write_version) do
      :abort
    else
      {:ok, apply_transaction(conflicts, transaction, write_version)}
    end
  end

  @spec conflict?(VersionedConflicts.t(), Transaction.encoded(), Bedrock.version()) :: boolean()
  def conflict?(conflicts, transaction, write_version) do
    {read_info, writes} = extract_conflicts(transaction)

    write_conflict?(conflicts, writes, write_version) or
      read_write_conflict?(conflicts, read_info)
  end

  # Extract conflicts from binary transaction using optimized single-pass approach
  defp extract_conflicts(binary_transaction) when is_binary(binary_transaction) do
    case Transaction.read_write_conflicts(binary_transaction) do
      {:ok, {read_info, write_conflicts}} ->
        case read_info do
          {nil, []} ->
            {nil, write_conflicts}

          {read_version, read_conflicts} ->
            {{read_version, read_conflicts}, write_conflicts}
        end

      {:error, _} ->
        {nil, []}
    end
  end

  @spec write_conflict?(VersionedConflicts.t(), [Bedrock.key_range()], Bedrock.version()) ::
          boolean()
  def write_conflict?(conflicts, writes, write_version) do
    VersionedConflicts.conflict?(conflicts, writes, write_version)
  end

  @spec read_write_conflict?(
          VersionedConflicts.t(),
          nil | {Bedrock.version(), [Bedrock.key_range()]}
        ) ::
          boolean()
  def read_write_conflict?(_, nil), do: false

  def read_write_conflict?(conflicts, {read_version, reads}) do
    VersionedConflicts.conflict?(conflicts, reads, read_version)
  end

  @spec version_lt(Bedrock.version()) :: (Bedrock.version() -> boolean())
  def version_lt(version), do: &(&1 > version)

  @spec apply_transaction(VersionedConflicts.t(), Transaction.encoded(), Bedrock.version()) :: VersionedConflicts.t()
  def apply_transaction(conflicts, transaction, write_version) do
    {_read_info, writes} = extract_conflicts(transaction)
    VersionedConflicts.add_conflicts(conflicts, writes, write_version)
  end

  @spec remove_old_transactions(VersionedConflicts.t(), Bedrock.version()) :: VersionedConflicts.t()
  def remove_old_transactions(conflicts, min_version),
    do: VersionedConflicts.remove_old_conflicts(conflicts, min_version)
end
