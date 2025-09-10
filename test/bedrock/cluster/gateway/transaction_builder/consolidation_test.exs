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

  # Helper function to extract common merge pattern with empty storage
  defp merge_with_empty_storage(tx, query_range) do
    Tx.merge_storage_range_with_writes(
      tx,
      # Empty storage results
      [],
      # No more data in storage
      false,
      # Query range
      query_range,
      # Shard range (unbounded)
      {elem(query_range, 0), :end}
    )
  end

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
    expected_key1 = Atomics.add(<<>>, <<5::64-little>>)
    expected_key2 = Atomics.min(<<>>, <<100::64-little>>)
    expected_key4 = Atomics.max(<<>>, <<50::64-little>>)

    # Pattern match the entire result structure to verify all values at once
    assert {_updated_tx,
            [
              {"key1", ^expected_key1},
              {"key2", ^expected_key2},
              {"key3", "regular_value"},
              {"key4", ^expected_key4}
            ]} = merge_with_empty_storage(tx, {"key0", "key9"})
  end

  test "consolidation maintains correct ordering" do
    # Verify that the consolidation maintains the same ordering as the original
    tx =
      Tx.new()
      |> Tx.set("b", "value_b")
      |> Tx.set("a", "value_a")
      |> Tx.set("d", "value_d")
      |> Tx.set("c", "value_c")

    # Pattern match to verify both structure and ordering in one assertion
    assert {_updated_tx,
            [
              {"a", "value_a"},
              {"b", "value_b"},
              {"c", "value_c"},
              {"d", "value_d"}
            ]} = merge_with_empty_storage(tx, {"a", "z"})
  end
end
