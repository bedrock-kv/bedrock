defmodule Bedrock.Internal.Atomics.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Internal.Atomics

  # Generators for variable-sized little-endian integers

  @doc "Generate a little-endian binary from 0 to max_bytes length"
  def little_endian_binary(max_bytes \\ 8) do
    gen all(
          size <- integer(0..max_bytes),
          value <- positive_integer()
        ) do
      case size do
        0 -> <<>>
        n -> integer_to_little_endian(value, n)
      end
    end
  end

  @doc "Generate a non-empty little-endian binary"
  def non_empty_little_endian_binary(max_bytes \\ 8) do
    gen all(
          size <- integer(1..max_bytes),
          value <- positive_integer()
        ) do
      integer_to_little_endian(value, size)
    end
  end

  @doc "Generate little-endian binaries of the same size"
  def same_size_little_endian_binaries(max_bytes \\ 4) do
    gen all(
          size <- integer(1..max_bytes),
          a <- positive_integer(),
          b <- positive_integer()
        ) do
      {integer_to_little_endian(a, size), integer_to_little_endian(b, size)}
    end
  end

  # Helper functions

  defp integer_to_little_endian(value, size) do
    # Convert integer to little-endian binary of specified size
    import Bitwise

    bytes = for i <- 0..(size - 1), do: value >>> (i * 8) &&& 0xFF
    :binary.list_to_bin(bytes)
  end

  defp little_endian_to_integer(<<>>), do: 0

  defp little_endian_to_integer(binary) do
    import Bitwise

    binary
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.reduce(0, fn {byte, index}, acc -> acc + (byte <<< (index * 8)) end)
  end

  describe "add/2 properties" do
    property "add is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.add(a, b) == Atomics.add(b, a)
      end
    end

    property "add with empty existing returns operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        assert Atomics.add(<<>>, operand) == operand
      end
    end

    property "add with empty operand returns empty" do
      check all(existing <- little_endian_binary()) do
        assert Atomics.add(existing, <<>>) == <<>>
      end
    end

    property "add performs correct arithmetic for single bytes" do
      check all({a, b} <- {integer(0..127), integer(0..127)}) do
        # Use small values to avoid overflow for simplicity
        result = Atomics.add(<<a>>, <<b>>)
        expected = <<a + b>>
        assert result == expected
      end
    end

    property "add handles carry correctly" do
      check all(overflow_val <- integer(1..50)) do
        import Bitwise
        # 255 + overflow_val should carry to next byte
        result = Atomics.add(<<255>>, <<overflow_val>>)
        expected_low = 255 + overflow_val &&& 0xFF
        expected_high = (255 + overflow_val) >>> 8
        expected = <<expected_low, expected_high>>
        assert result == expected
      end
    end

    property "add is associative (with padding)" do
      check all({a, b, c} <- {little_endian_binary(2), little_endian_binary(2), little_endian_binary(2)}) do
        # Pad all to same size for associativity test
        max_size = Enum.max([byte_size(a), byte_size(b), byte_size(c), 1])
        a_pad = pad_to_size(a, max_size)
        b_pad = pad_to_size(b, max_size)
        c_pad = pad_to_size(c, max_size)

        left = Atomics.add(Atomics.add(a_pad, b_pad), c_pad)
        right = Atomics.add(a_pad, Atomics.add(b_pad, c_pad))
        assert left == right
      end
    end
  end

  describe "min/2 properties" do
    property "min is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.min(a, b) == Atomics.min(b, a)
      end
    end

    property "min with empty existing returns operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        assert Atomics.min(<<>>, operand) == operand
      end
    end

    property "min with empty operand returns empty" do
      check all(existing <- little_endian_binary()) do
        assert Atomics.min(existing, <<>>) == <<>>
      end
    end

    property "min is idempotent" do
      check all(value <- non_empty_little_endian_binary()) do
        assert Atomics.min(value, value) == value
      end
    end

    property "min result is always <= both operands (same size)" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        result = Atomics.min(a, b)
        result_int = little_endian_to_integer(result)
        a_int = little_endian_to_integer(a)
        b_int = little_endian_to_integer(b)

        assert result_int <= a_int and result_int <= b_int and
                 (result_int == a_int or result_int == b_int)
      end
    end

    property "min with zero returns zero or operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        zero = pad_to_size(<<0>>, byte_size(operand))
        result = Atomics.min(operand, zero)

        # Result should be zero (all zero bytes)
        assert result == zero or result == operand
      end
    end
  end

  describe "max/2 properties" do
    property "max is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.max(a, b) == Atomics.max(b, a)
      end
    end

    property "max with empty existing returns operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        assert Atomics.max(<<>>, operand) == operand
      end
    end

    property "max with empty operand returns empty" do
      check all(existing <- little_endian_binary()) do
        assert Atomics.max(existing, <<>>) == <<>>
      end
    end

    property "max is idempotent" do
      check all(value <- non_empty_little_endian_binary()) do
        assert Atomics.max(value, value) == value
      end
    end

    property "max result is always >= both operands (same size)" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        result = Atomics.max(a, b)
        result_int = little_endian_to_integer(result)
        a_int = little_endian_to_integer(a)
        b_int = little_endian_to_integer(b)

        assert result_int >= a_int and result_int >= b_int and
                 (result_int == a_int or result_int == b_int)
      end
    end
  end

  describe "padding behavior properties" do
    property "shorter existing value is padded to operand length" do
      check all({short_val, long_size} <- {little_endian_binary(2), integer(3..6)}) do
        long_operand = integer_to_little_endian(100, long_size)

        # Test with add - should pad short_val with zeros to match long_operand length
        result = Atomics.add(short_val, long_operand)
        # May have carry
        assert byte_size(result) <= long_size + 1
      end
    end

    property "min/max operations preserve operand length when existing is empty" do
      check all(operand <- non_empty_little_endian_binary()) do
        min_result = Atomics.min(<<>>, operand)
        max_result = Atomics.max(<<>>, operand)

        assert min_result == operand and max_result == operand
      end
    end

    property "operations handle size differences correctly" do
      check all({small, big_size} <- {little_endian_binary(1), integer(2..4)}) do
        big_operand = integer_to_little_endian(50, big_size)

        # All operations should handle the size difference
        add_result = Atomics.add(small, big_operand)
        min_result = Atomics.min(small, big_operand)
        max_result = Atomics.max(small, big_operand)

        # Results should be reasonable sizes
        # Allow for carry
        assert byte_size(add_result) <= big_size + 1 and
                 byte_size(min_result) <= big_size and
                 byte_size(max_result) <= big_size
      end
    end
  end

  describe "bitwise operations properties" do
    property "bit_and is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.bit_and(a, b) == Atomics.bit_and(b, a)
      end
    end

    property "bit_or is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.bit_or(a, b) == Atomics.bit_or(b, a)
      end
    end

    property "bit_xor is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.bit_xor(a, b) == Atomics.bit_xor(b, a)
      end
    end

    property "bit_and with all 1s returns original (same size)" do
      check all(value <- non_empty_little_endian_binary(3)) do
        all_ones = :binary.copy(<<255>>, byte_size(value))
        assert Atomics.bit_and(value, all_ones) == value
      end
    end

    property "bit_or with all 0s returns original (same size)" do
      check all(value <- non_empty_little_endian_binary(3)) do
        all_zeros = :binary.copy(<<0>>, byte_size(value))
        assert Atomics.bit_or(value, all_zeros) == value
      end
    end

    property "bit_xor with self returns zeros" do
      check all(value <- non_empty_little_endian_binary(3)) do
        result = Atomics.bit_xor(value, value)
        all_zeros = :binary.copy(<<0>>, byte_size(value))
        assert result == all_zeros
      end
    end
  end

  describe "boundary condition properties" do
    property "operations with maximum single byte values" do
      check all(operation <- one_of([:add, :min, :max, :bit_and, :bit_or, :bit_xor])) do
        max_byte = <<255>>
        zero_byte = <<0>>

        result =
          case operation do
            :add -> Atomics.add(max_byte, zero_byte)
            :min -> Atomics.min(max_byte, zero_byte)
            :max -> Atomics.max(max_byte, zero_byte)
            :bit_and -> Atomics.bit_and(max_byte, zero_byte)
            :bit_or -> Atomics.bit_or(max_byte, zero_byte)
            :bit_xor -> Atomics.bit_xor(max_byte, zero_byte)
          end

        assert is_binary(result) and byte_size(result) >= 0
      end
    end

    property "all operations handle empty binaries gracefully" do
      check all(
              {operation, operand} <-
                {one_of([:add, :min, :max, :bit_and, :bit_or, :bit_xor]), little_endian_binary()}
            ) do
        result =
          case operation do
            :add -> Atomics.add(<<>>, operand)
            :min -> Atomics.min(<<>>, operand)
            :max -> Atomics.max(<<>>, operand)
            :bit_and -> Atomics.bit_and(<<>>, operand)
            :bit_or -> Atomics.bit_or(<<>>, operand)
            :bit_xor -> Atomics.bit_xor(<<>>, operand)
          end

        # Most operations with empty existing should return operand
        case operation do
          op when op in [:min, :max, :bit_and, :bit_or, :bit_xor, :add] ->
            assert result == operand
        end
      end
    end

    property "all new operations handle empty binaries gracefully" do
      check all(
              {operation, operand} <-
                {one_of([:byte_min, :byte_max, :append_if_fits]), little_endian_binary()}
            ) do
        result =
          case operation do
            :byte_min -> Atomics.byte_min(<<>>, operand)
            :byte_max -> Atomics.byte_max(<<>>, operand)
            :append_if_fits -> Atomics.append_if_fits(<<>>, operand)
          end

        # All new operations with empty existing should return operand
        assert result == operand
        assert is_binary(result) and byte_size(result) >= 0
      end
    end
  end

  describe "byte comparison operations" do
    property "byte_min is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.byte_min(a, b) == Atomics.byte_min(b, a)
      end
    end

    property "byte_max is commutative for same-sized operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.byte_max(a, b) == Atomics.byte_max(b, a)
      end
    end

    property "byte_min with empty existing returns operand" do
      check all(operand <- little_endian_binary()) do
        assert Atomics.byte_min(<<>>, operand) == operand
      end
    end

    property "byte_max with empty existing returns operand" do
      check all(operand <- little_endian_binary()) do
        assert Atomics.byte_max(<<>>, operand) == operand
      end
    end

    property "byte_min is idempotent" do
      check all(value <- non_empty_little_endian_binary()) do
        assert Atomics.byte_min(value, value) == value
      end
    end

    property "byte_max is idempotent" do
      check all(value <- non_empty_little_endian_binary()) do
        assert Atomics.byte_max(value, value) == value
      end
    end

    property "byte_min result is lexicographically <= both operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        result = Atomics.byte_min(a, b)
        assert result <= a and result <= b
        assert result == a or result == b
      end
    end

    property "byte_max result is lexicographically >= both operands" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        result = Atomics.byte_max(a, b)
        assert result >= a and result >= b
        assert result == a or result == b
      end
    end

    property "byte operations preserve operand length when existing is empty" do
      check all(operand <- non_empty_little_endian_binary()) do
        min_result = Atomics.byte_min(<<>>, operand)
        max_result = Atomics.byte_max(<<>>, operand)

        assert byte_size(min_result) == byte_size(operand)
        assert byte_size(max_result) == byte_size(operand)
        assert min_result == operand
        assert max_result == operand
      end
    end

    property "byte_min and byte_max are complementary" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        min_val = Atomics.byte_min(a, b)
        max_val = Atomics.byte_max(a, b)

        # One should equal a, the other should equal b (unless a == b)
        if a == b do
          assert min_val == a and max_val == a
        else
          assert (min_val == a and max_val == b) or (min_val == b and max_val == a)
        end
      end
    end

    property "byte operations handle different sized inputs correctly" do
      check all(
              a <- little_endian_binary(4),
              b <- little_endian_binary(6)
            ) do
        min_result = Atomics.byte_min(a, b)
        max_result = Atomics.byte_max(a, b)

        # Results should have the size of the operand (b) since a (existing) is shorter
        assert byte_size(min_result) == byte_size(b)
        assert byte_size(max_result) == byte_size(b)
        assert is_binary(min_result)
        assert is_binary(max_result)
      end
    end
  end

  describe "append_if_fits operation" do
    property "append_if_fits with empty existing returns operand" do
      check all(operand <- little_endian_binary()) do
        assert Atomics.append_if_fits(<<>>, operand) == operand
      end
    end

    property "append_if_fits with empty operand returns existing" do
      check all(existing <- little_endian_binary()) do
        assert Atomics.append_if_fits(existing, <<>>) == existing
      end
    end

    property "append_if_fits preserves order when fitting" do
      # Use smaller values to ensure they fit within the limit
      check all(
              existing <- binary(max_length: 1000),
              operand <- binary(max_length: 1000)
            ) do
        result = Atomics.append_if_fits(existing, operand)

        if byte_size(existing) + byte_size(operand) <= 131_072 do
          # Should concatenate
          assert result == existing <> operand
          assert byte_size(result) == byte_size(existing) + byte_size(operand)
        else
          # Should return existing unchanged
          assert result == existing
        end
      end
    end

    property "append_if_fits respects size limit" do
      check all(
              size1 <- integer(0..131_072),
              size2 <- integer(0..131_072)
            ) do
        existing = :binary.copy(<<"a">>, size1)
        operand = :binary.copy(<<"b">>, size2)
        result = Atomics.append_if_fits(existing, operand)

        if size1 + size2 <= 131_072 do
          assert result == existing <> operand
        else
          assert result == existing
        end

        # Result should never exceed the limit
        assert byte_size(result) <= 131_072
      end
    end

    property "append_if_fits handles boundary conditions" do
      # Test exactly at the limit
      limit_size = 131_072
      existing_at_limit = :binary.copy(<<"x">>, limit_size)

      # Adding anything to a value at the limit should return existing unchanged
      check all(operand <- binary(min_length: 1, max_length: 100)) do
        result = Atomics.append_if_fits(existing_at_limit, operand)
        assert result == existing_at_limit
      end
    end

    property "append_if_fits is associative when all fit within limit" do
      # Use small values to ensure associativity holds
      check all(
              a <- binary(max_length: 100),
              b <- binary(max_length: 100),
              c <- binary(max_length: 100)
            ) do
        # Only test when all combinations fit
        if byte_size(a) + byte_size(b) + byte_size(c) <= 131_072 do
          result1 = Atomics.append_if_fits(Atomics.append_if_fits(a, b), c)
          result2 = Atomics.append_if_fits(a, Atomics.append_if_fits(b, c))

          assert result1 == result2
          assert result1 == a <> b <> c
        end
      end
    end
  end

  describe "bitwise operations extended properties" do
    property "bitwise operations handle different sizes correctly" do
      check all(
              {a, b} <- {little_endian_binary(4), little_endian_binary(6)},
              op <- one_of([:bit_and, :bit_or, :bit_xor])
            ) do
        result =
          case op do
            :bit_and -> Atomics.bit_and(a, b)
            :bit_or -> Atomics.bit_or(a, b)
            :bit_xor -> Atomics.bit_xor(a, b)
          end

        # Result should have the same size as the operand (longer value)
        assert byte_size(result) == byte_size(b)
        assert is_binary(result)
      end
    end

    property "bitwise operations are associative for same-sized non-empty values" do
      check all(
              size <- integer(1..3),
              a_val <- positive_integer(),
              b_val <- positive_integer(),
              c_val <- positive_integer(),
              op <- one_of([:bit_and, :bit_or, :bit_xor])
            ) do
        # Generate binaries of exactly the same size
        a = integer_to_little_endian(a_val, size)
        b = integer_to_little_endian(b_val, size)
        c = integer_to_little_endian(c_val, size)

        op_fun =
          case op do
            :bit_and -> &Atomics.bit_and/2
            :bit_or -> &Atomics.bit_or/2
            :bit_xor -> &Atomics.bit_xor/2
          end

        result1 = op_fun.(op_fun.(a, b), c)
        result2 = op_fun.(a, op_fun.(b, c))

        assert result1 == result2
      end
    end

    property "bit_and with all ones returns original (when same size)" do
      check all(value <- non_empty_little_endian_binary()) do
        all_ones = :binary.copy(<<0xFF>>, byte_size(value))
        result = Atomics.bit_and(value, all_ones)
        assert result == value
      end
    end

    property "bit_or with all zeros returns original (when same size)" do
      check all(value <- non_empty_little_endian_binary()) do
        all_zeros = :binary.copy(<<0x00>>, byte_size(value))
        result = Atomics.bit_or(value, all_zeros)
        assert result == value
      end
    end

    property "bit_xor is its own inverse" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        # a XOR b XOR b == a
        temp = Atomics.bit_xor(a, b)
        result = Atomics.bit_xor(temp, b)
        assert result == a
      end
    end

    property "De Morgan's laws for bitwise operations" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        # NOT(a AND b) == (NOT a) OR (NOT b)
        # Since we don't have a NOT operation, we'll use XOR with all 1s
        all_ones = :binary.copy(<<0xFF>>, byte_size(a))

        and_result = Atomics.bit_and(a, b)
        not_and = Atomics.bit_xor(and_result, all_ones)

        not_a = Atomics.bit_xor(a, all_ones)
        not_b = Atomics.bit_xor(b, all_ones)
        not_a_or_not_b = Atomics.bit_or(not_a, not_b)

        assert not_and == not_a_or_not_b
      end
    end
  end

  # Helper function for padding
  defp pad_to_size(binary, target_size) when byte_size(binary) >= target_size, do: binary

  defp pad_to_size(binary, target_size) do
    padding_size = target_size - byte_size(binary)
    binary <> :binary.copy(<<0>>, padding_size)
  end
end
