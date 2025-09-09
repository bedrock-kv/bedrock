defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxAtomicRangePropertyTest do
  @moduledoc """
  Property tests for atomic operations in range queries.

  These tests would have caught the issues where merge_ordered_results_bounded
  was returning :atomic_operation placeholders instead of computing actual values.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.Internal.Atomics

  describe "atomic operations in range queries" do
    property "atomic add operations compute correct values when merged with storage" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              storage_value <- binary(max_length: 8),
              operand <- binary(max_length: 8),
              max_runs: 100
            ) do
        # Simulate what happens in range queries: apply atomic to storage value
        expected_result = Atomics.add(storage_value, operand)

        # Test the internal function that was broken
        actual_result = Tx.apply_atomic_to_storage_value({:add, operand}, storage_value)

        assert actual_result == expected_result,
               "Atomic add failed: storage=#{inspect(storage_value)}, operand=#{inspect(operand)}"
      end
    end

    property "atomic min/max operations compute correct values" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              storage_value <- binary(max_length: 8),
              operand <- binary(max_length: 8),
              operation <- member_of([:min, :max]),
              max_runs: 100
            ) do
        # Compute expected result
        expected_result =
          case operation do
            :min -> Atomics.min(storage_value, operand)
            :max -> Atomics.max(storage_value, operand)
          end

        # Test the internal function
        actual_result = Tx.apply_atomic_to_storage_value({operation, operand}, storage_value)

        assert actual_result == expected_result,
               "Atomic #{operation} failed: storage=#{inspect(storage_value)}, operand=#{inspect(operand)}"
      end
    end

    property "bitwise atomic operations compute correct values" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              storage_value <- binary(max_length: 8),
              operand <- binary(max_length: 8),
              operation <- member_of([:bit_and, :bit_or, :bit_xor]),
              max_runs: 100
            ) do
        # Compute expected result
        expected_result =
          case operation do
            :bit_and -> Atomics.bit_and(storage_value, operand)
            :bit_or -> Atomics.bit_or(storage_value, operand)
            :bit_xor -> Atomics.bit_xor(storage_value, operand)
          end

        # Test the internal function
        actual_result = Tx.apply_atomic_to_storage_value({operation, operand}, storage_value)

        assert actual_result == expected_result,
               "Atomic #{operation} failed: storage=#{inspect(storage_value)}, operand=#{inspect(operand)}"
      end
    end

    property "byte operations compute correct values" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              storage_value <- binary(max_length: 8),
              operand <- binary(max_length: 8),
              operation <- member_of([:byte_min, :byte_max]),
              max_runs: 100
            ) do
        # Compute expected result
        expected_result =
          case operation do
            :byte_min -> Atomics.byte_min(storage_value, operand)
            :byte_max -> Atomics.byte_max(storage_value, operand)
          end

        # Test the internal function
        actual_result = Tx.apply_atomic_to_storage_value({operation, operand}, storage_value)

        assert actual_result == expected_result,
               "Atomic #{operation} failed: storage=#{inspect(storage_value)}, operand=#{inspect(operand)}"
      end
    end

    property "append_if_fits operations compute correct values" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              # Smaller to test append limits
              storage_value <- binary(max_length: 4),
              operand <- binary(max_length: 4),
              max_runs: 100
            ) do
        # Compute expected result
        expected_result = Atomics.append_if_fits(storage_value, operand)

        # Test the internal function
        actual_result = Tx.apply_atomic_to_storage_value({:append_if_fits, operand}, storage_value)

        assert actual_result == expected_result,
               "Atomic append_if_fits failed: storage=#{inspect(storage_value)}, operand=#{inspect(operand)}"
      end
    end

    property "compare_and_clear operations compute correct values" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              storage_value <- binary(max_length: 8),
              expected_value <- binary(max_length: 8),
              max_runs: 100
            ) do
        # Compute expected result
        expected_result = if storage_value == expected_value, do: <<>>, else: storage_value

        # Test the internal function
        actual_result = Tx.apply_atomic_to_storage_value({:compare_and_clear, expected_value}, storage_value)

        assert actual_result == expected_result,
               "Atomic compare_and_clear failed: storage=#{inspect(storage_value)}, expected=#{inspect(expected_value)}"
      end
    end

    property "atomic operations with nil storage values use empty binary" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              operand <- binary(max_length: 8),
              operation <-
                member_of([:add, :min, :max, :bit_and, :bit_or, :bit_xor, :byte_min, :byte_max, :append_if_fits]),
              max_runs: 50
            ) do
        # Test that nil storage values are treated as empty binary
        expected_result =
          case operation do
            :add -> Atomics.add(<<>>, operand)
            :min -> Atomics.min(<<>>, operand)
            :max -> Atomics.max(<<>>, operand)
            :bit_and -> Atomics.bit_and(<<>>, operand)
            :bit_or -> Atomics.bit_or(<<>>, operand)
            :bit_xor -> Atomics.bit_xor(<<>>, operand)
            :byte_min -> Atomics.byte_min(<<>>, operand)
            :byte_max -> Atomics.byte_max(<<>>, operand)
            :append_if_fits -> Atomics.append_if_fits(<<>>, operand)
          end

        # Test the internal function with nil storage
        actual_result = Tx.apply_atomic_to_storage_value({operation, operand}, nil)

        assert actual_result == expected_result,
               "Atomic #{operation} with nil storage failed: operand=#{inspect(operand)}"
      end
    end

    property "non-atomic operations pass through unchanged" do
      check all(
              _key <- binary(min_length: 1, max_length: 20),
              value <- binary(max_length: 20),
              storage_value <- one_of([binary(max_length: 20), constant(nil)]),
              max_runs: 50
            ) do
        # Non-atomic operations (set, clear) should pass through unchanged
        assert Tx.apply_atomic_to_storage_value(value, storage_value) == value
        assert Tx.apply_atomic_to_storage_value(:clear, storage_value) == :clear
      end
    end

    property "repeatable_read computes atomic values correctly" do
      check all(
              key <- binary(min_length: 1, max_length: 20),
              read_value <- binary(max_length: 8),
              operand <- binary(max_length: 8),
              operation <- member_of([:add, :min, :max, :bit_and, :bit_or, :bit_xor]),
              max_runs: 100
            ) do
        # Create a transaction that reads a value, then applies an atomic operation
        base_tx = put_in(Tx.new(), [Access.key(:reads)], Map.put(%{}, key, read_value))

        tx =
          case operation do
            :add -> Tx.atomic_operation(base_tx, key, :add, operand)
            :min -> Tx.atomic_operation(base_tx, key, :min, operand)
            :max -> Tx.atomic_operation(base_tx, key, :max, operand)
            :bit_and -> Tx.atomic_operation(base_tx, key, :bit_and, operand)
            :bit_or -> Tx.atomic_operation(base_tx, key, :bit_or, operand)
            :bit_xor -> Tx.atomic_operation(base_tx, key, :bit_xor, operand)
          end

        # Compute expected result
        expected_result =
          case operation do
            :add -> Atomics.add(read_value, operand)
            :min -> Atomics.min(read_value, operand)
            :max -> Atomics.max(read_value, operand)
            :bit_and -> Atomics.bit_and(read_value, operand)
            :bit_or -> Atomics.bit_or(read_value, operand)
            :bit_xor -> Atomics.bit_xor(read_value, operand)
          end

        # Test repeatable_read returns computed atomic value
        actual_result = Tx.repeatable_read(tx, key)

        assert actual_result == expected_result,
               "repeatable_read atomic #{operation} failed: read=#{inspect(read_value)}, operand=#{inspect(operand)}"
      end
    end
  end
end
