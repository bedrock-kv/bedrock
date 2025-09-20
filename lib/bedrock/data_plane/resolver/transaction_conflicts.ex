defmodule Bedrock.DataPlane.Resolver.ConflictResolution do
  @moduledoc """
  Core conflict detection logic for the Resolver using interval trees.

  Processes transaction batches to detect read-write and write-write conflicts by
  checking for overlapping key ranges across different versions. Returns updated
  interval trees and lists of aborted transaction indices.

  Each transaction is checked against the interval tree to determine if its reads
  or writes conflict with previously committed transactions at later versions.
  """
  alias Bedrock.DataPlane.Resolver.Conflicts
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
  @spec resolve(Conflicts.t(), [Transaction.encoded()], write_version :: Bedrock.version()) ::
          {Conflicts.t(), aborted :: [non_neg_integer()]}
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

  @spec try_to_resolve_transaction(Conflicts.t(), Transaction.encoded(), Bedrock.version()) ::
          {:ok, Conflicts.t()} | :abort
  def try_to_resolve_transaction(conflicts, transaction, write_version) do
    with {read_info, writes} <- extract_conflicts(transaction),
         :ok <- check_read_conflicts(conflicts, read_info) do
      {:ok, Conflicts.add_conflicts(conflicts, writes, write_version)}
    end
  end

  defp check_read_conflicts(conflicts, {read_version, reads}),
    do: Conflicts.check_conflicts(conflicts, reads, read_version)

  defp check_read_conflicts(_conflicts, _), do: :ok

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

  @spec remove_old_transactions(Conflicts.t(), Bedrock.version()) :: Conflicts.t()
  def remove_old_transactions(conflicts, min_version), do: Conflicts.remove_old_conflicts(conflicts, min_version)
end
