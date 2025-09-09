defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ConsolidationTest do
  @moduledoc """
  Test that verifies the consolidation of range scanning mechanisms fixed the atomic operations bug.

  This test specifically verifies that when scan_pending_writes is called (which now uses
  merge_ordered_results_bounded instead of the old collect_writes_in_range), atomic operations
  are computed correctly instead of returning :atomic_operation placeholders.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.Internal.Atomics

  test "scan_pending_writes now computes atomic values instead of placeholders" do
    # This test verifies that the consolidation fixed the atomic operations bug
    # by checking that atomic operations return computed values, not :atomic_operation

    # Create a transaction with various atomic operations
    tx =
      Tx.new()
      |> Tx.atomic_operation("key1", :add, <<5::64-little>>)
      |> Tx.atomic_operation("key2", :min, <<100::64-little>>)
      |> Tx.set("key3", "regular_value")
      |> Tx.atomic_operation("key4", :max, <<50::64-little>>)

    # Test the public merge_storage_range_with_writes function which internally
    # calls scan_pending_writes in the {[], false} case (empty storage, no more data)
    {_updated_tx, results} =
      Tx.merge_storage_range_with_writes(
        tx,
        # Empty storage results
        [],
        # No more data in storage
        false,
        # Query range
        {"key0", "key9"},
        # Shard range (unbounded)
        {"key0", :end}
      )

    # Verify we got results (not empty)
    assert length(results) == 4

    # Extract the values and verify they're computed, not placeholders
    result_values = Enum.map(results, fn {_key, value} -> value end)

    # None of the results should be :atomic_operation placeholders
    refute :atomic_operation in result_values

    # Verify the actual computed values
    results_map = Map.new(results)

    # key1: add to empty (nil storage) should be add(<<>>, <<5>>)
    assert results_map["key1"] == Atomics.add(<<>>, <<5::64-little>>)

    # key2: min with empty should be min(<<>>, <<100>>)
    assert results_map["key2"] == Atomics.min(<<>>, <<100::64-little>>)

    # key3: regular set should be unchanged
    assert results_map["key3"] == "regular_value"

    # key4: max with empty should be max(<<>>, <<50>>)
    assert results_map["key4"] == Atomics.max(<<>>, <<50::64-little>>)
  end

  test "consolidation maintains correct ordering" do
    # Verify that the consolidation maintains the same ordering as the original
    tx =
      Tx.new()
      |> Tx.set("b", "value_b")
      |> Tx.set("a", "value_a")
      |> Tx.set("d", "value_d")
      |> Tx.set("c", "value_c")

    {_updated_tx, results} =
      Tx.merge_storage_range_with_writes(
        tx,
        # Empty storage
        [],
        # No more data
        false,
        # Query range
        {"a", "z"},
        # Shard range
        {"a", :end}
      )

    # Results should be in key order (gb_trees iteration order)
    keys = Enum.map(results, fn {key, _value} -> key end)
    assert keys == ["a", "b", "c", "d"]
  end
end
