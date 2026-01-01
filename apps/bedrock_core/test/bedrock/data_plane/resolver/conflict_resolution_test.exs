defmodule Bedrock.DataPlane.Resolver.ConflictResolutionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.DataPlane.Resolver.ConflictResolution,
    only: [
      resolve: 3,
      try_to_resolve_transaction: 3
    ]

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Transaction

  # Generate a random alphanumeric string of length 1-5 characters for use as database keys
  def key_generator do
    string(:alphanumeric, min_length: 1, max_length: 5)
  end

  # Generate a range where start <= end. Returns a single key if both values are equal,
  # otherwise returns a properly ordered {start_key, end_key} tuple
  def range_generator do
    StreamData.bind(key_generator(), fn v1 ->
      StreamData.map(key_generator(), fn
        v2 when v1 > v2 -> {v2, v1}
        v2 when v1 == v2 -> v1
        v2 -> {v1, v2}
      end)
    end)
  end

  # Generate keys (99% probability) or ranges (1% probability) to simulate
  # realistic database access patterns with occasional range operations
  def key_or_range_generator do
    one_of([range_generator() | 1..99 |> Enum.map(fn _ -> key_generator() end) |> Enum.to_list()])
  end

  # Generate realistic read/write patterns for transactions:
  # - Reads: 1-10 keys/ranges (simulating queries)
  # - Writes: 1-5 keys/ranges (simulating updates)
  def reads_and_writes_generator do
    gen all(
          reads <- list_of(key_or_range_generator(), min_length: 1, max_length: 10),
          writes <- list_of(key_or_range_generator(), min_length: 1, max_length: 5)
        ) do
      {reads, writes}
    end
  end

  describe "version merging optimization" do
    test "add_conflicts merges when version matches top version" do
      conflicts = Conflicts.new()

      # Add first conflict at version 100
      conflicts = Conflicts.add_conflicts(conflicts, [{"key1", "key1\0"}], 100)
      assert [version_entry] = conflicts.versions
      assert {100, points1, _tree1} = version_entry
      assert MapSet.member?(points1, "key1")

      # Add second conflict at same version 100 - should merge
      conflicts = Conflicts.add_conflicts(conflicts, [{"key2", "key2\0"}], 100)
      # Still only one entry
      assert [version_entry] = conflicts.versions
      assert {100, merged_points, _merged_tree} = version_entry
      assert MapSet.member?(merged_points, "key1")
      assert MapSet.member?(merged_points, "key2")

      # Add conflict at different version 200 - should create new entry
      conflicts = Conflicts.add_conflicts(conflicts, [{"key3", "key3\0"}], 200)
      # Now two entries
      assert [entry200, entry100] = conflicts.versions
      assert {200, points200, _tree200} = entry200
      assert {100, points100, _tree100} = entry100
      assert MapSet.member?(points200, "key3")
      assert MapSet.member?(points100, "key1")
      assert MapSet.member?(points100, "key2")
    end
  end

  describe "resolve/3 edge cases" do
    test "handles transaction with malformed binary gracefully" do
      conflicts = Conflicts.new()

      # Use an invalid binary that can't be parsed as a transaction
      malformed_transaction = <<255, 255, 255>>

      # Should not crash, treating it as a transaction with no conflicts
      write_version = Bedrock.DataPlane.Version.from_integer(100)
      {_new_conflicts, failed_indexes} = resolve(conflicts, [malformed_transaction], write_version)

      # No failures since malformed transaction has no conflicts to check
      # The error path in extract_conflicts returns {nil, []}, which means no reads and no writes
      assert failed_indexes == []
    end

    test "resolves transaction with read_version and read_conflicts" do
      conflicts = Conflicts.new()

      # Create a transaction with read version and read conflicts
      transaction_map = %{
        mutations: [{:set, "key1", "value"}],
        read_conflicts: [{"key1", "key2"}],
        write_conflicts: [{"key1", "key2"}],
        read_version: Bedrock.DataPlane.Version.from_integer(50)
      }

      transaction = Transaction.encode(transaction_map)

      # Should successfully resolve (no prior conflicts)
      write_version = Bedrock.DataPlane.Version.from_integer(100)
      {new_conflicts, failed_indexes} = resolve(conflicts, [transaction], write_version)

      assert failed_indexes == []
      # New conflict should be added for the write
      assert new_conflicts != conflicts
    end
  end

  property "commit/2 commits transactions without conflicts and aborts those with conflicts" do
    check all(
            reads_and_writes <-
              list_of(reads_and_writes_generator(), min_length: 10, max_length: 40),
            write_version <- integer(1_000_000..100_000_000)
          ) do
      initial_conflicts = Conflicts.new()

      # Generate binary transactions with read and write conflicts. The write_version is
      # used to generate the read version for each transaction. The read
      # version is used to detect conflicts between reads and writes, and must
      # be some number that is lower than the index of the transaction.
      transactions =
        reads_and_writes
        |> Enum.with_index()
        |> Enum.map(fn {{reads, writes}, index} ->
          read_version = rem(write_version, index + 1) - 1
          read_version_binary = if read_version >= 0, do: Bedrock.DataPlane.Version.from_integer(read_version)

          # Convert read keys/ranges to conflicts
          read_conflicts =
            Enum.map(reads, fn
              key when is_binary(key) -> {key, key <> "\0"}
              {start_key, end_key} -> {start_key, end_key}
            end)

          # Convert write keys/ranges to conflicts
          write_conflicts =
            Enum.map(writes, fn
              key when is_binary(key) -> {key, key <> "\0"}
              {start_key, end_key} -> {start_key, end_key}
            end)

          # Create transaction map
          transaction_map = %{
            mutations:
              Enum.map(writes, fn
                key when is_binary(key) -> {:set, key, "value"}
                # Use start key for ranges
                {start_key, _end_key} -> {:set, start_key, "value"}
              end),
            read_conflicts: read_conflicts,
            write_conflicts: write_conflicts,
            read_version: read_version_binary
          }

          # Encode to binary
          Transaction.encode(transaction_map)
        end)

      # Pattern match the resolve result to extract failed transaction indexes
      assert {_final_conflicts, failed_indexes} = resolve(initial_conflicts, transactions, write_version)

      # They can't *all* fail...
      assert length(failed_indexes) < length(transactions)

      # Ensure that the failed indexes are the ones that have conflicts.
      Enum.each(failed_indexes, fn index ->
        transactions_up_to_failure = Enum.take(transactions, index)

        # Resolve transactions up to the failure point to build the conflict structure
        assert {conflicts, failed_indexes} = resolve(initial_conflicts, transactions_up_to_failure, write_version)

        # The first transaction has an empty conflicts structure and should never conflict
        # with anything.
        assert index != 0

        # The transactions up to the failed index should not include the one
        # that has failed (since we're not supposed to have processed it yet.)
        refute index in failed_indexes

        # Pull out the transaction that failed.
        failed_transaction = Enum.at(transactions, index)

        # The failed transaction should have read-write conflicts
        case Transaction.read_write_conflicts(failed_transaction) do
          {:ok, {read_info, _writes}} ->
            # Only check read-write conflicts
            case read_info do
              {read_version, reads} ->
                assert :abort == Conflicts.check_conflicts(conflicts, reads, read_version)

              _ ->
                :ok
            end

          _ ->
            assert false, "Failed to extract conflicts from transaction"
        end

        # The failed transaction should abort when attempted against the conflict structure
        assert :abort = try_to_resolve_transaction(conflicts, failed_transaction, index)
      end)
    end
  end
end
