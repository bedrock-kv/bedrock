defmodule Bedrock.DataPlane.Resolver.ConflictResolutionTest do
  use ExUnit.Case, async: true

  import Bedrock.DataPlane.Resolver.ConflictResolution,
    only: [
      resolve: 3,
      try_to_resolve_transaction: 3
    ]

  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Transaction

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
      transaction_map = %{
        mutations: [{:set, "key1", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(50), [{"key1", "key2"}]},
        write_conflicts: [{"key1", "key2"}]
      }

      transaction = Transaction.encode(transaction_map)

      assert Transaction.read_conflicts(transaction) ==
               {:ok, {Bedrock.DataPlane.Version.from_integer(50), [{"key1", "key2"}]}}

      # Should successfully resolve (no prior conflicts)
      write_version = Bedrock.DataPlane.Version.from_integer(100)
      {new_conflicts, failed_indexes} = resolve(conflicts, [transaction], write_version)

      assert failed_indexes == []
      # New conflict should be added for the write
      assert new_conflicts != conflicts
    end
  end
end
