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

    test "handles CRC-valid transaction with truncated conflict data gracefully" do
      conflicts = Conflicts.new()

      # Fabricate a transaction whose READ_CONFLICTS section claims 5 ranges
      # but contains no range data. CRCs remain valid (add_section recomputes
      # them), so only conflict payload decoding fails. Previously this
      # crashed the resolver with a CaseClauseError; now it degrades to
      # a transaction with no conflicts.
      binary = Transaction.encode(%{mutations: [{:set, "key", "value"}]})
      {:ok, corrupted} = Transaction.add_section(binary, 0x02, <<12_345::signed-big-64, 5::unsigned-big-32>>)

      write_version = Bedrock.DataPlane.Version.from_integer(100)

      assert {:ok, %Conflicts{}} = try_to_resolve_transaction(conflicts, corrupted, write_version)

      {_new_conflicts, failed_indexes} = resolve(conflicts, [corrupted], write_version)
      assert failed_indexes == []
    end

    test "resolves transaction with read_version and read_conflicts" do
      conflicts = Conflicts.new()

      # Create a transaction with read version and read conflicts
      read_version = Bedrock.DataPlane.Version.from_integer(50)

      transaction_map = %{
        mutations: [{:set, "key1", "value"}],
        read_conflicts: {read_version, [{"key1", "key2"}]},
        write_conflicts: [{"key1", "key2"}]
      }

      transaction = Transaction.encode(transaction_map)

      # The read conflicts have to survive encoding, or every assertion below is
      # about a transaction that reads nothing.
      assert {:ok, {{^read_version, [{"key1", "key2"}]}, [{"key1", "key2"}]}} =
               Transaction.read_write_conflicts(transaction)

      # Should successfully resolve (no prior conflicts)
      write_version = Bedrock.DataPlane.Version.from_integer(100)
      {new_conflicts, failed_indexes} = resolve(conflicts, [transaction], write_version)

      assert failed_indexes == []
      # New conflict should be added for the write
      assert new_conflicts != conflicts

      # ...and the same read now aborts against the write it just recorded.
      assert :abort = try_to_resolve_transaction(new_conflicts, transaction, write_version)
    end
  end

  property "commit/2 commits transactions without conflicts and aborts those with conflicts" do
    check all(
            reads_and_writes <-
              list_of(reads_and_writes_generator(), min_length: 10, max_length: 40),
            write_version_int <- integer(1_000_000..100_000_000)
          ) do
      initial_conflicts = Conflicts.new()

      # Writes are recorded at the same version the reads are compared against,
      # so both have to be Version binaries: mixing an integer in here makes
      # every `v > version` comparison fall through term ordering and nothing
      # ever conflicts.
      write_version = Bedrock.DataPlane.Version.from_integer(write_version_int)

      # Generate binary transactions with read and write conflicts. The write_version is
      # used to generate the read version for each transaction. The read
      # version is used to detect conflicts between reads and writes, and must
      # be some number that is lower than the index of the transaction.
      transactions =
        reads_and_writes
        |> Enum.with_index()
        |> Enum.map(fn {{reads, writes}, index} ->
          read_version = rem(write_version_int, index + 1) - 1
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

          # Read conflicts only encode as a {read_version, ranges} pair; a
          # transaction with no read version carries no read conflicts at all.
          read_conflicts_section =
            if read_version_binary, do: {read_version_binary, read_conflicts}, else: {nil, []}

          # Create transaction map
          transaction_map = %{
            mutations:
              Enum.map(writes, fn
                key when is_binary(key) -> {:set, key, "value"}
                # Use start key for ranges
                {start_key, _end_key} -> {:set, start_key, "value"}
              end),
            read_conflicts: read_conflicts_section,
            write_conflicts: write_conflicts
          }

          # Encode to binary
          encoded = Transaction.encode(transaction_map)

          # The reads have to survive encoding, or the abort assertions below
          # are checked against a transaction that reads nothing.
          assert {:ok, {^read_conflicts_section, ^write_conflicts}} = Transaction.read_write_conflicts(encoded)

          encoded
        end)

      # Pattern match the resolve result to extract failed transaction indexes
      assert {final_conflicts, failed_indexes} = resolve(initial_conflicts, transactions, write_version)

      # They can't *all* fail...
      assert length(failed_indexes) < length(transactions)

      # Replay the batch against the conflict structure the resolver would have
      # had when it reached each transaction. A transaction must be in
      # failed_indexes exactly when its reads overlap a write recorded at a
      # later version - checking only one direction lets a resolver that never
      # aborts anything satisfy the property.
      replayed_conflicts =
        transactions
        |> Enum.with_index()
        |> Enum.reduce(initial_conflicts, fn {transaction, index}, conflicts ->
          assert {:ok, {read_info, _writes}} = Transaction.read_write_conflicts(transaction)

          expected =
            case read_info do
              # No read version means no reads to conflict; nothing can abort it.
              {nil, []} -> :ok
              {read_version, reads} -> Conflicts.check_conflicts(conflicts, reads, read_version)
            end

          case expected do
            :abort ->
              assert index in failed_indexes
              assert :abort = try_to_resolve_transaction(conflicts, transaction, write_version)
              # An aborted transaction records nothing, so the next one sees the
              # same conflicts.
              conflicts

            :ok ->
              refute index in failed_indexes
              assert {:ok, next_conflicts} = try_to_resolve_transaction(conflicts, transaction, write_version)
              next_conflicts
          end
        end)

      # The replay agrees index by index, so it must land on the same structure
      # the batch resolve produced.
      assert replayed_conflicts == final_conflicts
    end
  end
end
