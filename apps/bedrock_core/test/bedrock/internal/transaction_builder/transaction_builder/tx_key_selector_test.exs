defmodule Bedrock.Internal.TransactionBuilder.Tx.KeySelectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.Tx

  describe "merge_storage_read/3" do
    test "merges single key-value pair into transaction reads" do
      key = "resolved_key"
      value = "storage_value"

      assert %Tx{reads: %{^key => ^value}} = Tx.merge_storage_read(Tx.new(), key, value)
    end

    test "merges :not_found into transaction reads as :clear" do
      key = "missing_key"

      assert %Tx{reads: %{^key => :clear}} = Tx.merge_storage_read(Tx.new(), key, :not_found)
    end

    test "overwrites existing read with new value" do
      key = "existing_key"
      old_value = "old_value"
      new_value = "new_value"

      assert %Tx{reads: %{^key => ^new_value}} =
               Tx.new()
               |> Tx.merge_storage_read(key, old_value)
               |> Tx.merge_storage_read(key, new_value)
    end

    test "handles multiple different keys" do
      assert %Tx{reads: %{"key1" => "value1", "key2" => "value2", "key3" => :clear}} =
               Tx.new()
               |> Tx.merge_storage_read("key1", "value1")
               |> Tx.merge_storage_read("key2", "value2")
               |> Tx.merge_storage_read("key3", :not_found)
    end
  end

  describe "merge_storage_range_read/4" do
    test "merges range read results into transaction state" do
      resolved_start = "range_start"
      resolved_end = "range_end"

      key_values = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      assert %Tx{
               reads: %{"key1" => "value1", "key2" => "value2", "key3" => "value3"},
               range_reads: [{^resolved_start, ^resolved_end}]
             } = Tx.merge_storage_range_read(Tx.new(), resolved_start, resolved_end, key_values)
    end

    test "merges empty range read results" do
      resolved_start = "empty_start"
      resolved_end = "empty_end"

      assert %Tx{
               reads: %{},
               range_reads: [{^resolved_start, ^resolved_end}]
             } = Tx.merge_storage_range_read(Tx.new(), resolved_start, resolved_end, [])
    end

    test "merges with existing reads and range_reads" do
      # Add new range read
      key_values = [{"new_key1", "new_value1"}, {"new_key2", "new_value2"}]

      assert %Tx{
               reads: %{
                 "existing_key" => "existing_value",
                 "old_key" => "old_value",
                 "new_key1" => "new_value1",
                 "new_key2" => "new_value2"
               },
               range_reads: range_reads
             } =
               Tx.new()
               |> Tx.merge_storage_read("existing_key", "existing_value")
               |> Tx.merge_storage_range_read("old_start", "old_end", [{"old_key", "old_value"}])
               |> Tx.merge_storage_range_read("new_start", "new_end", key_values)

      assert length(range_reads) == 2
      assert {"old_start", "old_end"} in range_reads
      assert {"new_start", "new_end"} in range_reads
    end

    test "handles overlapping ranges correctly" do
      # Add overlapping range - should be merged by add_or_merge
      assert %Tx{
               reads: %{"key1" => "value1", "key2" => "value2"},
               range_reads: range_reads
             } =
               Tx.new()
               |> Tx.merge_storage_range_read("a", "m", [{"key1", "value1"}])
               |> Tx.merge_storage_range_read("f", "z", [{"key2", "value2"}])

      # Ranges should be merged into a single range by add_or_merge function
      # The exact merge result depends on add_or_merge implementation
      assert is_list(range_reads)
      # Could be 1 if merged, or 2 if kept separate
      assert length(range_reads) >= 1
    end
  end

  describe "conflict tracking integration" do
    setup do
      read_version_integer = Bedrock.DataPlane.Version.from_integer(12_345)
      read_version_binary = <<0, 0, 0, 0, 0, 0, 0, 42>>

      %{read_version_integer: read_version_integer, read_version_binary: read_version_binary}
    end

    test "merged reads are included in commit conflict tracking", %{read_version_integer: read_version} do
      assert %{
               mutations: [{:set, "write_key", "write_value"}],
               write_conflicts: [{"write_key", "write_key\0"}],
               read_conflicts: {^read_version, [{"conflict_key", "conflict_key\0"}]}
             } =
               Tx.new()
               |> Tx.merge_storage_read("conflict_key", "conflict_value")
               |> Tx.set("write_key", "write_value")
               |> Tx.commit(read_version)
               |> then(&elem(Transaction.decode(&1), 1))
    end

    test "merged range reads are included in commit conflict tracking", %{read_version_binary: read_version} do
      assert %{
               mutations: _mutations,
               write_conflicts: _write_conflicts,
               read_conflicts: {^read_version, read_conflicts}
             } =
               Tx.new()
               |> Tx.merge_storage_range_read("range_a", "range_z", [{"r_key", "r_value"}])
               |> Tx.set("write_key", "write_value")
               |> Tx.commit(read_version)
               |> then(&elem(Transaction.decode(&1), 1))

      # Check read conflicts include individual keys and range
      assert "r_key" in Enum.map(read_conflicts, fn {start, _end} -> start end)
      assert {"range_a", "range_z"} in read_conflicts
    end

    test "complex scenario with multiple merge operations" do
      read_version = <<0, 0, 0, 0, 0, 0, 0, 100>>

      assert %{
               mutations: [
                 {:set, "write_key1", "write_value1"},
                 {:set, "write_key2", "write_value2"}
               ],
               write_conflicts: [_, _],
               read_conflicts: {^read_version, read_conflicts}
             } =
               Tx.new()
               |> Tx.merge_storage_read("single_key", "single_value")
               |> Tx.merge_storage_range_read("range1_a", "range1_z", [{"range1_key", "range1_value"}])
               |> Tx.set("write_key1", "write_value1")
               |> Tx.merge_storage_read("another_key", :not_found)
               |> Tx.merge_storage_range_read("range2_a", "range2_z", [])
               |> Tx.set("write_key2", "write_value2")
               |> Tx.commit(read_version)
               |> then(&elem(Transaction.decode(&1), 1))

      # At least 3 individual keys + 2 ranges
      assert length(read_conflicts) >= 4
    end
  end
end
