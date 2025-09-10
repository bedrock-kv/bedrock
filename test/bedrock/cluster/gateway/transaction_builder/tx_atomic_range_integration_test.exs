defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxAtomicRangeIntegrationTest do
  @moduledoc """
  Integration tests that would have caught the range/atomics issues.

  These tests simulate the exact scenario where merge_ordered_results_bounded
  was failing: merging transaction writes with storage results in range queries.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.Internal.Atomics

  describe "atomic operations in simulated range queries" do
    test "atomic operations are computed, not returned as placeholders" do
      # This test would have failed with the original broken implementation
      # because it was returning :atomic_operation instead of computed values

      _storage_results = [
        {"key1", "existing_value"},
        {"key3", "another_value"}
      ]

      # Create a transaction with atomic operations on keys that overlap with storage
      tx =
        Tx.new()
        # Should add to "existing_value"
        |> Tx.atomic_operation("key1", :add, <<5>>)
        # Should add to empty (no storage)
        |> Tx.atomic_operation("key2", :add, <<10>>)
        # Should override storage value
        |> Tx.set("key3", "new_value")
        # Should min with empty (no storage)
        |> Tx.atomic_operation("key4", :min, <<100>>)

      # Simulate what happens in range queries - this would fail before the fix
      # Assert computed values (not placeholders) using pattern matching
      assert_atomic_operation_computes(tx, "key1", "existing_value", Atomics.add("existing_value", <<5>>))
      assert_atomic_operation_computes(tx, "key2", nil, Atomics.add(<<>>, <<10>>))
      # Non-atomic
      assert "new_value" = simulate_merge_with_storage(tx, "key3", "another_value")
      assert_atomic_operation_computes(tx, "key4", nil, Atomics.min(<<>>, <<100>>))
    end

    test "repeatable_read returns computed atomic values, not placeholders" do
      # This test verifies that repeatable_read computes atomic values correctly
      # when a key has both read and atomic write operations

      tx =
        Tx.new()
        |> put_in([Access.key(:reads)], Map.put(%{}, "counter", <<42::64-little>>))
        |> Tx.atomic_operation("counter", :add, <<10::64-little>>)

      # This should return the computed value, not :atomic_operation
      expected = Atomics.add(<<42::64-little>>, <<10::64-little>>)
      assert ^expected = Tx.repeatable_read(tx, "counter")
    end

    test "all atomic operation types compute correctly in range scenarios" do
      # Test all atomic operations to ensure comprehensive coverage
      operations = [
        {:add, <<5>>, fn base -> Atomics.add(base, <<5>>) end},
        {:min, <<100>>, fn base -> Atomics.min(base, <<100>>) end},
        {:max, <<50>>, fn base -> Atomics.max(base, <<50>>) end},
        {:bit_and, <<0xFF>>, fn base -> Atomics.bit_and(base, <<0xFF>>) end},
        {:bit_or, <<0x0F>>, fn base -> Atomics.bit_or(base, <<0x0F>>) end},
        {:bit_xor, <<0x55>>, fn base -> Atomics.bit_xor(base, <<0x55>>) end},
        {:byte_min, "hello", fn base -> Atomics.byte_min(base, "hello") end},
        {:byte_max, "world", fn base -> Atomics.byte_max(base, "world") end},
        {:append_if_fits, "suffix", fn base -> Atomics.append_if_fits(base, "suffix") end},
        {:compare_and_clear, "expected",
         fn base ->
           if base == "expected", do: <<>>, else: base
         end}
      ]

      for {{op, operand, expected_fn}, index} <- Enum.with_index(operations) do
        key = "test_key_#{index}"
        storage_value = "base_value_#{index}"

        # Create transaction with the atomic operation - simplified pattern
        tx = Tx.atomic_operation(Tx.new(), key, op, operand)

        # Simulate merge with storage and assert computed result (not placeholder)
        expected = expected_fn.(storage_value)

        assert ^expected = simulate_merge_with_storage(tx, key, storage_value),
               "Operation #{op} failed: expected #{inspect(expected)}"
      end
    end

    test "edge case: multiple atomic operations on same key" do
      # Test that only the last operation is applied (due to conflict removal)
      tx =
        Tx.new()
        |> Tx.atomic_operation("key", :add, <<5>>)
        # Should override the first add
        |> Tx.atomic_operation("key", :add, <<10>>)
        # Should override the second add
        |> Tx.atomic_operation("key", :min, <<3>>)

      storage_value = <<50>>
      expected = Atomics.min(storage_value, <<3>>)
      assert ^expected = simulate_merge_with_storage(tx, "key", storage_value)
    end
  end

  # Helper function that simulates what merge_ordered_results_bounded does internally
  defp simulate_merge_with_storage(tx, key, storage_value) do
    case :gb_trees.lookup(key, tx.writes) do
      {:value, tx_value} ->
        Tx.apply_atomic_to_storage_value(tx_value, storage_value)

      :none ->
        # No transaction write for this key, return storage value
        storage_value
    end
  end

  # Helper to create transaction with atomic operation and assert computed result
  defp assert_atomic_operation_computes(tx, key, storage_value, expected) do
    assert ^expected = simulate_merge_with_storage(tx, key, storage_value)
  end
end
