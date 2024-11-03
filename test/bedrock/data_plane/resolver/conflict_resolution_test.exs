defmodule Bedrock.DataPlane.Resolver.ConflictResolutionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.DataPlane.Resolver.ConflictResolution,
    only: [
      commit: 3,
      conflict?: 3,
      try_commit_transaction: 3
    ]

  # Generate a random alphanumeric string of length 5 or less.
  def key_generator() do
    string(:alphanumeric, min_length: 1, max_length: 5)
  end

  # Generate a range where the start is always less than the end. If we happen
  # to generate the same value, we just return the value.
  def range_generator() do
    key_generator()
    |> StreamData.bind(fn v1 ->
      key_generator()
      |> StreamData.map(fn
        v2 when v1 > v2 -> {v2, v1}
        v2 when v1 == v2 -> v1
        v2 -> {v1, v2}
      end)
    end)
  end

  # Produce a key about 99% of the time, and a range about 1% of the time.
  def key_or_range_generator() do
    one_of([range_generator() | 1..99 |> Enum.map(fn _ -> key_generator() end) |> Enum.to_list()])
  end

  def reads_and_writes_generator() do
    gen all(
          reads <- list_of(key_or_range_generator(), min_length: 1, max_length: 10),
          writes <- list_of(key_or_range_generator(), min_length: 1, max_length: 5)
        ) do
      {reads, writes}
    end
  end

  property "commit/2 commits transactions without conflicts and aborts those with conflicts" do
    check all(
            reads_and_writes <-
              list_of(reads_and_writes_generator(), min_length: 10, max_length: 40),
            write_version <- integer(1_000_000..100_000_000)
          ) do
      initial_tree = nil

      # Generate transactions with read and write sets. The write_version is
      # used to generate the read version for each transaction. The read
      # version is used to detect conflicts between reads and writes, and must
      # be some number that is lower than the index of the transaction.
      transactions =
        reads_and_writes
        |> Enum.with_index()
        |> Enum.map(fn {{reads, writes}, index} ->
          {rem(write_version, index + 1) - 1, reads, writes}
        end)

      {_final_tree, failed_indexes} = commit(initial_tree, transactions, write_version)

      # They can't *all* fail...
      assert length(failed_indexes) < length(transactions)

      # Ensure that the failed indexes are the ones that have conflicts.
      Enum.each(failed_indexes, fn index ->
        transactions_up_to_failure = Enum.take(transactions, index)

        {tree, failed_indexes} = commit(initial_tree, transactions_up_to_failure, write_version)

        # The first transaction has an empty tree and should never conflict
        # with anything.
        assert index != 0

        # The transactions up to the failed index should not include the one
        # that has failed (since we're not supposed to have processed it yet.)
        assert index not in failed_indexes

        # Pull out the transaction that failed.
        failed_transaction = Enum.at(transactions, index)

        # The failed transaction should have conflicts either on writes or
        # due to version mismatch.
        assert conflict?(tree, failed_transaction, write_version)

        # If we try the transaction that failed, it should abort.
        assert :abort = try_commit_transaction(tree, failed_transaction, index)
      end)
    end
  end
end
