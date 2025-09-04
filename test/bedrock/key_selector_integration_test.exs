defmodule Bedrock.KeySelectorIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.KeySelector

  @moduletag :integration

  describe "end-to-end KeySelector functionality" do
    test "KeySelector construction and manipulation" do
      # Test all four canonical KeySelector types
      gte = KeySelector.first_greater_or_equal("base")
      gt = KeySelector.first_greater_than("base")
      lte = KeySelector.last_less_or_equal("base")
      lt = KeySelector.last_less_than("base")

      # Verify correct construction
      assert %KeySelector{key: "base", or_equal: true, offset: 0} = gte
      assert %KeySelector{key: "base", or_equal: false, offset: 1} = gt
      assert %KeySelector{key: "base", or_equal: true, offset: -1} = lte
      assert %KeySelector{key: "base", or_equal: false, offset: 0} = lt

      # Test offset manipulation
      forward = KeySelector.add(gte, 5)
      backward = KeySelector.subtract(gte, 3)

      assert %KeySelector{key: "base", or_equal: true, offset: 5} = forward
      assert %KeySelector{key: "base", or_equal: true, offset: -3} = backward

      # Test offset predicates
      assert KeySelector.zero_offset?(gte)
      assert KeySelector.positive_offset?(forward)
      assert KeySelector.negative_offset?(backward)
      assert KeySelector.negative_offset?(lte)

      refute KeySelector.zero_offset?(forward)
      refute KeySelector.positive_offset?(gte)
      refute KeySelector.negative_offset?(gte)
    end

    test "KeySelector string representations" do
      # Test string representations for all types
      selectors = [
        KeySelector.first_greater_or_equal("start"),
        KeySelector.first_greater_than("begin"),
        KeySelector.last_less_or_equal("finish"),
        KeySelector.last_less_than("end"),
        "offset_test" |> KeySelector.first_greater_or_equal() |> KeySelector.add(3),
        "negative_test" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-2)
      ]

      expected_strings = [
        ~s{first_greater_or_equal("start")},
        ~s{first_greater_than("begin")},
        ~s{last_less_or_equal("finish")},
        ~s{last_less_than("end")},
        ~s{first_greater_or_equal("offset_test") + 3},
        ~s{first_greater_or_equal("negative_test") - 2}
      ]

      actual_strings = Enum.map(selectors, &KeySelector.to_string/1)

      expected_strings
      |> Enum.zip(actual_strings)
      |> Enum.each(fn {expected, actual} ->
        assert actual == expected
      end)

      # Test String.Chars protocol
      string_chars_result = to_string(KeySelector.first_greater_or_equal("protocol_test"))
      assert string_chars_result == ~s{first_greater_or_equal("protocol_test")}
    end

    test "Transaction integration with merge_storage_read" do
      # Test that KeySelector results can be properly integrated into transactions
      tx = Tx.new()

      # Simulate KeySelector resolution results
      resolved_key = "resolved:key:001"
      resolved_value = "resolved_value_data"

      # Merge the resolved KeySelector result
      updated_tx = Tx.merge_storage_read(tx, resolved_key, resolved_value)

      # Verify the result is in the transaction reads
      assert %Tx{reads: reads} = updated_tx
      assert Map.get(reads, resolved_key) == resolved_value

      # Test with :not_found result
      missing_key = "missing:key"
      tx_with_missing = Tx.merge_storage_read(updated_tx, missing_key, :not_found)

      assert %Tx{reads: reads_with_missing} = tx_with_missing
      assert Map.get(reads_with_missing, missing_key) == :clear
      assert Map.get(reads_with_missing, resolved_key) == resolved_value
    end

    test "Transaction integration with merge_storage_range_read" do
      # Test range KeySelector integration
      tx = Tx.new()

      resolved_start = "range:start:001"
      resolved_end = "range:end:999"

      range_results = [
        {"range:item:100", "value_100"},
        {"range:item:200", "value_200"},
        {"range:item:300", "value_300"}
      ]

      # Merge the range results
      updated_tx = Tx.merge_storage_range_read(tx, resolved_start, resolved_end, range_results)

      # Verify individual items are in reads
      assert %Tx{reads: reads, range_reads: range_reads} = updated_tx
      assert Map.get(reads, "range:item:100") == "value_100"
      assert Map.get(reads, "range:item:200") == "value_200"
      assert Map.get(reads, "range:item:300") == "value_300"

      # Verify range is tracked for conflicts
      assert {resolved_start, resolved_end} in range_reads
    end

    test "KeySelector types cover all FoundationDB semantics" do
      # Verify we support the complete FoundationDB KeySelector semantic space

      # Forward selectors (key <= target <= ∞)
      forward_inclusive = KeySelector.first_greater_or_equal("forward_base")
      forward_exclusive = KeySelector.first_greater_than("forward_base")

      # Backward selectors (-∞ <= target <= key)
      backward_inclusive = KeySelector.last_less_or_equal("backward_base")
      backward_exclusive = KeySelector.last_less_than("backward_base")

      # All should be valid KeySelector structs
      all_selectors = [forward_inclusive, forward_exclusive, backward_inclusive, backward_exclusive]

      assert Enum.all?(all_selectors, fn selector ->
               %KeySelector{} = selector
               is_binary(selector.key) and is_boolean(selector.or_equal) and is_integer(selector.offset)
             end)

      # Test offset ranges from large negative to large positive
      extreme_forward = KeySelector.add(forward_inclusive, 10_000)
      extreme_backward = KeySelector.add(forward_inclusive, -10_000)

      assert extreme_forward.offset == 10_000
      assert extreme_backward.offset == -10_000

      # Should not crash on extreme values
      assert %KeySelector{} = extreme_forward
      assert %KeySelector{} = extreme_backward
    end

    test "KeySelector error handling" do
      # Test proper error handling for invalid operations

      # Construction with invalid key type
      assert_raise FunctionClauseError, fn ->
        KeySelector.first_greater_or_equal(123)
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.first_greater_than(:atom)
      end

      # Offset manipulation with invalid offset type
      selector = KeySelector.first_greater_or_equal("test")

      assert_raise FunctionClauseError, fn ->
        KeySelector.add(selector, "not_integer")
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.subtract(selector, 3.14)
      end
    end

    test "KeySelector binary key support" do
      # Test that KeySelectors work with arbitrary binary keys
      binary_keys = [
        # Empty binary
        <<>>,
        # Null byte
        <<0>>,
        # High byte
        <<255>>,
        # Mixed bytes
        <<0, 1, 2, 3, 255>>,
        # Normal string
        "regular_string",
        # Key with separators
        "key:with:colons",
        # Unicode key
        "🚀🔑📦"
      ]

      # All binary keys should work with KeySelectors
      selectors = Enum.map(binary_keys, &KeySelector.first_greater_or_equal/1)

      assert length(selectors) == length(binary_keys)

      binary_keys
      |> Enum.zip(selectors)
      |> Enum.each(fn {key, selector} ->
        assert %KeySelector{key: ^key, or_equal: true, offset: 0} = selector
      end)

      # String representation should handle binary keys correctly
      string_reps = Enum.map(selectors, &KeySelector.to_string/1)
      assert Enum.all?(string_reps, &is_binary/1)
    end

    test "complex KeySelector manipulation chains" do
      # Test complex chains of operations
      result =
        "base_key"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(10)
        |> KeySelector.subtract(3)
        |> KeySelector.add(-2)
        |> KeySelector.subtract(-1)

      # Net effect: +10 -3 -2 +1 = +6
      assert %KeySelector{key: "base_key", or_equal: true, offset: 6} = result

      # Test that operations are associative
      step_by_step =
        "test"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(5)
        |> KeySelector.add(3)

      all_at_once =
        "test"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(8)

      assert step_by_step.offset == all_at_once.offset

      # Test subtraction equivalence
      subtract_result = KeySelector.subtract(KeySelector.first_greater_or_equal("key"), 5)
      add_negative_result = KeySelector.add(KeySelector.first_greater_or_equal("key"), -5)

      assert subtract_result == add_negative_result
    end

    test "KeySelector semantic equivalence" do
      # Test that different constructions can produce equivalent selectors

      # These should be functionally equivalent for resolution purposes
      gt_construction = KeySelector.first_greater_than("key")
      gte_plus_one = "key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1)

      # Same key, same final offset
      assert gt_construction.key == gte_plus_one.key
      assert gt_construction.offset == gte_plus_one.offset

      # But different or_equal values (internal representation differs)
      assert gt_construction.or_equal == false
      assert gte_plus_one.or_equal == true

      # String representations should normalize to the same canonical form
      gt_string = KeySelector.to_string(gt_construction)
      gte_plus_one_string = KeySelector.to_string(gte_plus_one)

      # Both should represent the same logical position
      assert gt_string == ~s{first_greater_than("key")}
      assert gte_plus_one_string == ~s{first_greater_or_equal("key") + 1}
    end
  end

  describe "KeySelector resolution algorithm validation" do
    test "forward and backward selector directionality" do
      # Test that forward and backward selectors have correct offset signs

      # Forward selectors should have non-negative base offsets
      forward_selectors = [
        # offset: 0
        KeySelector.first_greater_or_equal("key"),
        # offset: 1
        KeySelector.first_greater_than("key")
      ]

      assert Enum.all?(forward_selectors, fn sel -> sel.offset >= 0 end)

      # Backward selectors should have non-positive base offsets
      backward_selectors = [
        # offset: 0
        KeySelector.last_less_than("key"),
        # offset: -1
        KeySelector.last_less_or_equal("key")
      ]

      # Note: last_less_than has offset 0, last_less_or_equal has offset -1
      # last_less_than
      assert Enum.at(backward_selectors, 0).offset == 0
      # last_less_or_equal
      assert Enum.at(backward_selectors, 1).offset == -1
    end

    test "offset directionality is preserved through manipulation" do
      # Test that offset manipulation preserves expected directionality

      # offset: 0
      base = KeySelector.first_greater_or_equal("key")

      # Forward movement should increase offset
      forward = KeySelector.add(base, 5)
      assert forward.offset > base.offset
      assert forward.offset == 5

      # Backward movement should decrease offset
      backward = KeySelector.subtract(base, 3)
      assert backward.offset < base.offset
      assert backward.offset == -3

      # Chain operations
      chained = base |> KeySelector.add(10) |> KeySelector.subtract(7)
      assert chained.offset == 3

      # Round trip should return to original
      round_trip = base |> KeySelector.add(100) |> KeySelector.subtract(100)
      assert round_trip.offset == base.offset
    end
  end
end
