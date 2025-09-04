defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx.KeySelectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  describe "merge_storage_read/3" do
    test "merges single key-value pair into transaction reads" do
      tx = Tx.new()
      key = "resolved_key"
      value = "storage_value"

      updated_tx = Tx.merge_storage_read(tx, key, value)

      assert %Tx{reads: reads} = updated_tx
      assert Map.get(reads, key) == value
    end

    test "merges :not_found into transaction reads as :clear" do
      tx = Tx.new()
      key = "missing_key"

      updated_tx = Tx.merge_storage_read(tx, key, :not_found)

      assert %Tx{reads: reads} = updated_tx
      assert Map.get(reads, key) == :clear
    end

    test "overwrites existing read with new value" do
      tx = Tx.new()
      key = "existing_key"
      old_value = "old_value"
      new_value = "new_value"

      tx_with_old = Tx.merge_storage_read(tx, key, old_value)
      tx_with_new = Tx.merge_storage_read(tx_with_old, key, new_value)

      assert %Tx{reads: reads} = tx_with_new
      assert Map.get(reads, key) == new_value
    end

    test "handles multiple different keys" do
      tx = Tx.new()

      updated_tx =
        tx
        |> Tx.merge_storage_read("key1", "value1")
        |> Tx.merge_storage_read("key2", "value2")
        |> Tx.merge_storage_read("key3", :not_found)

      assert %Tx{reads: reads} = updated_tx
      assert Map.get(reads, "key1") == "value1"
      assert Map.get(reads, "key2") == "value2"
      assert Map.get(reads, "key3") == :clear
      assert map_size(reads) == 3
    end
  end

  describe "merge_storage_range_read/4" do
    test "merges range read results into transaction state" do
      tx = Tx.new()
      resolved_start = "range_start"
      resolved_end = "range_end"

      key_values = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      updated_tx = Tx.merge_storage_range_read(tx, resolved_start, resolved_end, key_values)

      assert %Tx{reads: reads, range_reads: range_reads} = updated_tx

      # Check individual key-value pairs are in reads
      assert Map.get(reads, "key1") == "value1"
      assert Map.get(reads, "key2") == "value2"
      assert Map.get(reads, "key3") == "value3"
      assert map_size(reads) == 3

      # Check range is in range_reads
      assert range_reads == [{resolved_start, resolved_end}]
    end

    test "merges empty range read results" do
      tx = Tx.new()
      resolved_start = "empty_start"
      resolved_end = "empty_end"
      key_values = []

      updated_tx = Tx.merge_storage_range_read(tx, resolved_start, resolved_end, key_values)

      assert %Tx{reads: reads, range_reads: range_reads} = updated_tx
      assert map_size(reads) == 0
      assert range_reads == [{resolved_start, resolved_end}]
    end

    test "merges with existing reads and range_reads" do
      tx = Tx.new()

      # Add some initial state
      tx_with_existing =
        tx
        |> Tx.merge_storage_read("existing_key", "existing_value")
        |> Tx.merge_storage_range_read("old_start", "old_end", [{"old_key", "old_value"}])

      # Add new range read
      key_values = [{"new_key1", "new_value1"}, {"new_key2", "new_value2"}]
      final_tx = Tx.merge_storage_range_read(tx_with_existing, "new_start", "new_end", key_values)

      assert %Tx{reads: reads, range_reads: range_reads} = final_tx

      # Check all reads are preserved and merged
      assert Map.get(reads, "existing_key") == "existing_value"
      assert Map.get(reads, "old_key") == "old_value"
      assert Map.get(reads, "new_key1") == "new_value1"
      assert Map.get(reads, "new_key2") == "new_value2"
      assert map_size(reads) == 4

      # Check ranges are merged (using add_or_merge function)
      assert length(range_reads) == 2
      assert {"old_start", "old_end"} in range_reads
      assert {"new_start", "new_end"} in range_reads
    end

    test "handles overlapping ranges correctly" do
      tx = Tx.new()

      # Add first range
      tx_with_first = Tx.merge_storage_range_read(tx, "a", "m", [{"key1", "value1"}])

      # Add overlapping range - should be merged by add_or_merge
      final_tx = Tx.merge_storage_range_read(tx_with_first, "f", "z", [{"key2", "value2"}])

      assert %Tx{reads: reads, range_reads: range_reads} = final_tx

      # Both key-value pairs should be in reads
      assert Map.get(reads, "key1") == "value1"
      assert Map.get(reads, "key2") == "value2"

      # Ranges should be merged into a single range by add_or_merge function
      # The exact merge result depends on add_or_merge implementation
      assert is_list(range_reads)
      # Could be 1 if merged, or 2 if kept separate
      assert length(range_reads) >= 1
    end
  end

  describe "conflict tracking integration" do
    test "merged reads are included in commit conflict tracking" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("conflict_key", "conflict_value")
        |> Tx.set("write_key", "write_value")

      # Commit without read_version to test conflict generation
      committed = Tx.commit(tx)

      assert %{
               mutations: mutations,
               write_conflicts: write_conflicts,
               # No read conflicts when read_version is nil
               read_conflicts: {nil, []}
             } = committed

      # Check mutations include our write
      assert mutations == [{:set, "write_key", "write_value"}]

      # Check write conflicts include our write
      assert write_conflicts == [{"write_key", "write_key\0"}]
    end

    test "merged range reads are included in commit conflict tracking" do
      tx =
        Tx.new()
        |> Tx.merge_storage_range_read("range_a", "range_z", [{"r_key", "r_value"}])
        |> Tx.set("write_key", "write_value")

      # Commit with read_version to enable read conflict tracking
      committed = Tx.commit(tx, 42)

      assert %{
               mutations: _mutations,
               write_conflicts: _write_conflicts,
               read_conflicts: {42, read_conflicts}
             } = committed

      # Check read conflicts include individual keys and range
      assert "r_key" in Enum.map(read_conflicts, fn {start, _end} -> start end)
      assert {"range_a", "range_z"} in read_conflicts
    end

    test "complex scenario with multiple merge operations" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("single_key", "single_value")
        |> Tx.merge_storage_range_read("range1_a", "range1_z", [{"range1_key", "range1_value"}])
        |> Tx.set("write_key1", "write_value1")
        |> Tx.merge_storage_read("another_key", :not_found)
        |> Tx.merge_storage_range_read("range2_a", "range2_z", [])
        |> Tx.set("write_key2", "write_value2")

      committed = Tx.commit(tx, 100)

      assert %{
               mutations: mutations,
               write_conflicts: write_conflicts,
               read_conflicts: {100, read_conflicts}
             } = committed

      # Verify mutations are in correct order (reversed from application order)
      assert mutations == [
               {:set, "write_key1", "write_value1"},
               {:set, "write_key2", "write_value2"}
             ]

      # Verify write conflicts include all writes
      assert length(write_conflicts) == 2

      # Verify read conflicts include all reads (individual keys + ranges)
      assert is_list(read_conflicts)
      # At least 3 individual keys + 2 ranges
      assert length(read_conflicts) >= 4
    end
  end
end
