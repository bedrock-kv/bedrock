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

  describe "core operation properties" do
    property "arithmetic and comparison operations are commutative" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.add(a, b) == Atomics.add(b, a)
        assert Atomics.min(a, b) == Atomics.min(b, a)
        assert Atomics.max(a, b) == Atomics.max(b, a)
        assert Atomics.byte_min(a, b) == Atomics.byte_min(b, a)
        assert Atomics.byte_max(a, b) == Atomics.byte_max(b, a)
      end
    end

    property "operations with empty existing return operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        assert operand == Atomics.add(<<>>, operand)
        assert operand == Atomics.min(<<>>, operand)
        assert operand == Atomics.max(<<>>, operand)
        assert operand == Atomics.byte_min(<<>>, operand)
        assert operand == Atomics.byte_max(<<>>, operand)
        assert operand == Atomics.append_if_fits(<<>>, operand)
      end
    end

    property "operations with empty operand have predictable behavior" do
      check all(existing <- little_endian_binary()) do
        assert <<>> == Atomics.add(existing, <<>>)
        assert <<>> == Atomics.min(existing, <<>>)
        assert <<>> == Atomics.max(existing, <<>>)
        assert existing == Atomics.append_if_fits(existing, <<>>)
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
        sum = 255 + overflow_val
        assert <<^sum::little-16>> = Atomics.add(<<255>>, <<overflow_val>>)
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

    property "idempotent operations return same value" do
      check all(value <- non_empty_little_endian_binary()) do
        assert value == Atomics.min(value, value)
        assert value == Atomics.max(value, value)
        assert value == Atomics.byte_min(value, value)
        assert value == Atomics.byte_max(value, value)
      end
    end

    property "min/max results are within operand bounds" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        min_result = Atomics.min(a, b)
        max_result = Atomics.max(a, b)

        min_int = little_endian_to_integer(min_result)
        max_int = little_endian_to_integer(max_result)
        a_int = little_endian_to_integer(a)
        b_int = little_endian_to_integer(b)

        assert min_int <= a_int and min_int <= b_int and (min_int == a_int or min_int == b_int)
        assert max_int >= a_int and max_int >= b_int and (max_int == a_int or max_int == b_int)
      end
    end

    property "min with zero returns zero or operand" do
      check all(operand <- non_empty_little_endian_binary()) do
        zero = pad_to_size(<<0>>, byte_size(operand))
        result = Atomics.min(operand, zero)

        assert result == zero or result == operand
      end
    end
  end

  describe "padding and size handling properties" do
    property "shorter existing value is padded to operand length" do
      check all({short_val, long_size} <- {little_endian_binary(2), integer(3..6)}) do
        long_operand = integer_to_little_endian(100, long_size)
        result = Atomics.add(short_val, long_operand)

        assert byte_size(result) <= long_size + 1
      end
    end

    property "operations handle size differences correctly" do
      check all({small, big_size} <- {little_endian_binary(1), integer(2..4)}) do
        big_operand = integer_to_little_endian(50, big_size)

        assert {add_result, min_result, max_result} = {
                 Atomics.add(small, big_operand),
                 Atomics.min(small, big_operand),
                 Atomics.max(small, big_operand)
               }

        assert byte_size(add_result) <= big_size + 1
        assert byte_size(min_result) <= big_size
        assert byte_size(max_result) <= big_size
      end
    end
  end

  describe "bitwise operations properties" do
    property "bitwise operations are commutative" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert Atomics.bit_and(a, b) == Atomics.bit_and(b, a)
        assert Atomics.bit_or(a, b) == Atomics.bit_or(b, a)
        assert Atomics.bit_xor(a, b) == Atomics.bit_xor(b, a)
      end
    end

    property "bitwise identity operations" do
      check all(value <- non_empty_little_endian_binary(3)) do
        all_ones = :binary.copy(<<255>>, byte_size(value))
        all_zeros = :binary.copy(<<0>>, byte_size(value))

        assert value == Atomics.bit_and(value, all_ones)
        assert value == Atomics.bit_or(value, all_zeros)
        assert all_zeros == Atomics.bit_xor(value, value)
      end
    end
  end

  describe "boundary condition properties" do
    property "operations with maximum single byte values produce valid results" do
      operations = [
        {:add, &Atomics.add/2},
        {:min, &Atomics.min/2},
        {:max, &Atomics.max/2},
        {:bit_and, &Atomics.bit_and/2},
        {:bit_or, &Atomics.bit_or/2},
        {:bit_xor, &Atomics.bit_xor/2}
      ]

      check all({_name, op_fun} <- one_of(Enum.map(operations, &constant/1))) do
        result = op_fun.(<<255>>, <<0>>)
        assert is_binary(result) and byte_size(result) >= 0
      end
    end

    property "core operations handle empty binaries gracefully" do
      operations = [
        &Atomics.add/2,
        &Atomics.min/2,
        &Atomics.max/2,
        &Atomics.bit_and/2,
        &Atomics.bit_or/2,
        &Atomics.bit_xor/2
      ]

      check all({op_fun, operand} <- {one_of(Enum.map(operations, &constant/1)), little_endian_binary()}) do
        assert ^operand = op_fun.(<<>>, operand)
      end
    end

    property "extended operations handle empty binaries gracefully" do
      operations = [&Atomics.byte_min/2, &Atomics.byte_max/2, &Atomics.append_if_fits/2]

      check all({op_fun, operand} <- {one_of(Enum.map(operations, &constant/1)), little_endian_binary()}) do
        assert ^operand = op_fun.(<<>>, operand)
        assert is_binary(operand) and byte_size(operand) >= 0
      end
    end
  end

  describe "byte comparison operations" do
    property "byte operations maintain lexicographic ordering" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        min_result = Atomics.byte_min(a, b)
        max_result = Atomics.byte_max(a, b)

        assert min_result <= a and min_result <= b
        assert min_result == a or min_result == b
        assert max_result >= a and max_result >= b
        assert max_result == a or max_result == b
      end
    end

    property "byte operations preserve operand length when existing is empty" do
      check all(operand <- non_empty_little_endian_binary()) do
        assert %{size: size, value: operand} = %{
                 size: byte_size(operand),
                 value: Atomics.byte_min(<<>>, operand)
               }

        assert %{size: ^size, value: ^operand} = %{
                 size: byte_size(Atomics.byte_max(<<>>, operand)),
                 value: Atomics.byte_max(<<>>, operand)
               }
      end
    end

    property "byte_min and byte_max are complementary" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert {min_val, max_val} = {Atomics.byte_min(a, b), Atomics.byte_max(a, b)}

        if a == b do
          assert {^a, ^a} = {min_val, max_val}
        else
          assert {min_val, max_val} == {a, b} or {min_val, max_val} == {b, a}
        end
      end
    end

    property "byte operations handle different sized inputs correctly" do
      check all(
              a <- little_endian_binary(4),
              b <- little_endian_binary(6)
            ) do
        assert {min_result, max_result} = {Atomics.byte_min(a, b), Atomics.byte_max(a, b)}
        target_size = byte_size(b)

        assert {^target_size, ^target_size} = {byte_size(min_result), byte_size(max_result)}
        assert is_binary(min_result) and is_binary(max_result)
      end
    end
  end

  describe "append_if_fits operation" do
    property "append_if_fits preserves order when fitting" do
      check all(
              existing <- binary(max_length: 1000),
              operand <- binary(max_length: 1000)
            ) do
        result = Atomics.append_if_fits(existing, operand)
        total_size = byte_size(existing) + byte_size(operand)

        if total_size <= 131_072 do
          assert {^result, ^total_size} = {existing <> operand, byte_size(result)}
        else
          assert ^existing = result
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

        expected = if size1 + size2 <= 131_072, do: existing <> operand, else: existing
        assert {^expected, true} = {result, byte_size(result) <= 131_072}
      end
    end

    property "append_if_fits handles boundary conditions" do
      existing_at_limit = :binary.copy(<<"x">>, 131_072)

      check all(operand <- binary(min_length: 1, max_length: 100)) do
        assert ^existing_at_limit = Atomics.append_if_fits(existing_at_limit, operand)
      end
    end

    property "append_if_fits is associative when all fit within limit" do
      check all(
              a <- binary(max_length: 100),
              b <- binary(max_length: 100),
              c <- binary(max_length: 100)
            ) do
        if byte_size(a) + byte_size(b) + byte_size(c) <= 131_072 do
          assert {result1, result2} = {
                   Atomics.append_if_fits(Atomics.append_if_fits(a, b), c),
                   Atomics.append_if_fits(a, Atomics.append_if_fits(b, c))
                 }

          expected = a <> b <> c
          assert {^expected, ^expected} = {result1, result2}
        end
      end
    end
  end

  describe "bitwise operations extended properties" do
    property "bitwise operations handle different sizes correctly" do
      operations = [&Atomics.bit_and/2, &Atomics.bit_or/2, &Atomics.bit_xor/2]

      check all(
              {a, b} <- {little_endian_binary(4), little_endian_binary(6)},
              op_fun <- one_of(Enum.map(operations, &constant/1))
            ) do
        result = op_fun.(a, b)
        expected_size = byte_size(b)

        assert {^expected_size, true} = {byte_size(result), is_binary(result)}
      end
    end

    property "bitwise operations are associative for same-sized non-empty values" do
      operations = [&Atomics.bit_and/2, &Atomics.bit_or/2, &Atomics.bit_xor/2]

      check all(
              size <- integer(1..3),
              {a_val, b_val, c_val} <- {positive_integer(), positive_integer(), positive_integer()},
              op_fun <- one_of(Enum.map(operations, &constant/1))
            ) do
        {a, b, c} = {
          integer_to_little_endian(a_val, size),
          integer_to_little_endian(b_val, size),
          integer_to_little_endian(c_val, size)
        }

        assert {result1, result2} = {op_fun.(op_fun.(a, b), c), op_fun.(a, op_fun.(b, c))}
        assert ^result1 = result2
      end
    end

    property "bitwise identity operations (extended)" do
      check all(value <- non_empty_little_endian_binary()) do
        all_ones = :binary.copy(<<0xFF>>, byte_size(value))
        all_zeros = :binary.copy(<<0x00>>, byte_size(value))

        assert {^value, ^value} = {
                 Atomics.bit_and(value, all_ones),
                 Atomics.bit_or(value, all_zeros)
               }
      end
    end

    property "bit_xor is its own inverse" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        assert ^a = Atomics.bit_xor(Atomics.bit_xor(a, b), b)
      end
    end

    property "De Morgan's laws for bitwise operations" do
      check all({a, b} <- same_size_little_endian_binaries()) do
        all_ones = :binary.copy(<<0xFF>>, byte_size(a))

        # NOT(a AND b) == (NOT a) OR (NOT b) using XOR with all 1s for NOT
        not_and = Atomics.bit_xor(Atomics.bit_and(a, b), all_ones)

        not_a_or_not_b =
          Atomics.bit_or(
            Atomics.bit_xor(a, all_ones),
            Atomics.bit_xor(b, all_ones)
          )

        assert ^not_and = not_a_or_not_b
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
