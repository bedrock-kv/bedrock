defmodule Bedrock.Key.TupleTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.Encoding.Tuple, as: Key

  doctest Key

  # Helper function to test roundtrip property
  defp assert_roundtrip(data) do
    assert data |> Key.pack() |> Key.unpack() == data
  end

  # Helper function to test deterministic packing
  defp assert_deterministic_pack(data) do
    packed1 = Key.pack(data)
    packed2 = Key.pack(data)
    assert packed1 == packed2
  end

  property "pack and unpack basic data types" do
    check all(
            data <-
              one_of([
                constant(nil),
                integer(),
                float(),
                binary()
              ])
          ) do
      assert_roundtrip(data)
    end
  end

  property "pack and unpack binaries with null bytes" do
    check all(binary <- binary(min_length: 1)) do
      # Insert some null bytes randomly in the binary
      binary_with_nulls =
        binary
        |> :binary.bin_to_list()
        |> Enum.intersperse(0x00)
        |> :binary.list_to_bin()

      packed = Key.pack(binary_with_nulls)
      unpacked = Key.unpack(packed)
      assert unpacked == binary_with_nulls
    end
  end

  property "pack and unpack tuples (simple and nested)" do
    check all(
            tuple <-
              one_of([
                # Simple tuples
                tuple({integer()}),
                tuple({float()}),
                tuple({binary()}),
                tuple({integer(), binary()}),
                tuple({integer(), float(), binary()}),
                # Nested tuples
                tuple({integer(), tuple({float(), binary()})}),
                tuple({tuple({integer(), binary()}), float()}),
                tuple({tuple({integer()}), tuple({binary()}), float()})
              ])
          ) do
      assert_roundtrip(tuple)
    end
  end

  property "pack and unpack comprehensive data types with deterministic results" do
    check all(
            data <-
              one_of([
                constant(nil),
                integer(),
                float(),
                binary(),
                tuple({integer(), binary()}),
                tuple({nil, float(), binary()}),
                tuple({integer(), tuple({binary(), nil})})
              ])
          ) do
      # Test roundtrip property
      assert_roundtrip(data)
      # Test deterministic packing
      assert_deterministic_pack(data)
    end
  end

  property "packed data maintains ordering for integers" do
    check all({int1, int2} <- {integer(), integer()}) do
      packed1 = Key.pack(int1)
      packed2 = Key.pack(int2)

      # Binary comparison should preserve integer ordering
      binary_comparison = if packed1 < packed2, do: :lt, else: if(packed1 > packed2, do: :gt, else: :eq)
      integer_comparison = if int1 < int2, do: :lt, else: if(int1 > int2, do: :gt, else: :eq)

      assert binary_comparison == integer_comparison
    end
  end

  property "bounded data types roundtrip correctly" do
    check all(
            data <-
              one_of([
                constant(nil),
                integer(-1000..1000),
                float(min: -1000.0, max: 1000.0),
                binary(max_length: 100),
                tuple({integer(-100..100), binary(max_length: 50)}),
                tuple({integer(-100..100), float(min: -100.0, max: 100.0), binary(max_length: 50)})
              ])
          ) do
      assert_roundtrip(data)
    end
  end

  describe "pack/1" do
    test "always returns binary for any supported data type" do
      test_cases = [42, "hello", {1, 2, 3}, nil, [], <<>>]

      for data <- test_cases do
        assert is_binary(Key.pack(data))
      end
    end
  end

  describe "edge cases" do
    test "handles boundary values correctly" do
      edge_cases = [
        {0, "zero integer"},
        {<<>>, "empty binary"},
        {{}, "empty tuple"},
        {{42}, "single element tuple"},
        {<<0, 0, 0>>, "binary with null bytes"}
      ]

      for {data, _description} <- edge_cases do
        assert_roundtrip(data)
      end
    end
  end

  describe "error handling" do
    test "raises ArgumentError for unsupported data types" do
      unsupported_types = [:atom, %{key: :value}]

      for data <- unsupported_types do
        assert_raise ArgumentError, fn -> Key.pack(data) end
      end
    end

    test "supports lists (no error expected)" do
      assert is_binary(Key.pack([1, 2, 3]))
    end
  end

  describe "lists vs tuples encoding" do
    test "maintains distinct encodings with proper ordering" do
      test_data = ["users", 123, "active"]
      list = test_data
      tuple = List.to_tuple(test_data)

      # Pattern match to verify encoding differences and ordering
      list_packed = Key.pack(list)
      tuple_packed = Key.pack(tuple)

      assert list_packed != tuple_packed
      # lists > tuples in Elixir ordering
      assert list_packed > tuple_packed
    end

    test "handles empty containers correctly" do
      assert Key.pack([]) != Key.pack({})
      assert Key.pack([]) > Key.pack({})
    end

    test "maintains roundtrip integrity for both types" do
      test_data = ["users", 123, "active"]

      # Both lists and tuples should roundtrip correctly
      assert_roundtrip(test_data)
      assert_roundtrip(List.to_tuple(test_data))
    end

    test "supports conversion between list and tuple representations" do
      list = ["users", 123, "active"]
      tuple = List.to_tuple(list)

      # Verify conversion works through pack/unpack cycle
      assert tuple |> Key.pack() |> Key.unpack() |> Tuple.to_list() == list
    end
  end

  describe "nested structures" do
    test "handles deeply nested tuples" do
      nested = {1, {2, {3, {4, {5}}}}}
      assert_roundtrip(nested)
    end

    test "handles deeply nested lists" do
      nested = [1, [2, [3, [4, [5]]]]]
      assert_roundtrip(nested)
    end

    test "handles mixed nested structures" do
      mixed = {1, [2, {3, [4, {5}]}]}
      assert_roundtrip(mixed)
    end

    test "handles tuples with various data types" do
      tuple = {nil, 42, "string", 3.14, {}, []}
      assert_roundtrip(tuple)
    end

    test "handles lists with various data types" do
      list = [nil, 42, "string", 3.14, {}, []]
      assert_roundtrip(list)
    end
  end

  describe "binary encoding edge cases" do
    test "handles binary starting with null byte" do
      binary = <<0, 1, 2, 3>>
      assert_roundtrip(binary)
    end

    test "handles binary ending with null byte" do
      binary = <<1, 2, 3, 0>>
      assert_roundtrip(binary)
    end

    test "handles binary with multiple consecutive null bytes" do
      binary = <<1, 0, 0, 0, 2>>
      assert_roundtrip(binary)
    end

    test "handles binary with only null bytes" do
      binary = <<0, 0, 0>>
      assert_roundtrip(binary)
    end

    test "handles empty binary in tuple" do
      tuple = {<<>>, "value"}
      assert_roundtrip(tuple)
    end

    test "handles empty binary in list" do
      list = [<<>>, "value"]
      assert_roundtrip(list)
    end
  end

  describe "integer encoding ranges" do
    test "handles 8-bit positive integers" do
      for i <- [1, 127, 255] do
        assert_roundtrip(i)
      end
    end

    test "handles 16-bit positive integers" do
      for i <- [256, 32_767, 65_535] do
        assert_roundtrip(i)
      end
    end

    test "handles 24-bit positive integers" do
      for i <- [65_536, 8_388_607, 16_777_215] do
        assert_roundtrip(i)
      end
    end

    test "handles 32-bit positive integers" do
      for i <- [16_777_216, 2_147_483_647, 4_294_967_295] do
        assert_roundtrip(i)
      end
    end

    test "handles 8-bit negative integers" do
      for i <- [-1, -127, -255] do
        assert_roundtrip(i)
      end
    end

    test "handles 16-bit negative integers" do
      for i <- [-256, -32_767, -65_535] do
        assert_roundtrip(i)
      end
    end

    test "handles 24-bit negative integers" do
      for i <- [-65_536, -8_388_607, -16_777_215] do
        assert_roundtrip(i)
      end
    end

    test "handles 32-bit negative integers" do
      for i <- [-16_777_216, -2_147_483_647, -4_294_967_295] do
        assert_roundtrip(i)
      end
    end

    test "handles boundary values for all integer sizes" do
      boundaries = [
        0xFF,
        0x100,
        0xFFFF,
        0x10000,
        0xFFFFFF,
        0x1000000,
        0xFFFFFFFF,
        0x100000000,
        -0xFF,
        -0x100,
        -0xFFFF,
        -0x10000,
        -0xFFFFFF,
        -0x1000000,
        -0xFFFFFFFF,
        -0x100000000
      ]

      for value <- boundaries do
        assert_roundtrip(value)
      end
    end
  end

  describe "float encoding" do
    test "handles positive floats" do
      for f <- [0.0, 1.0, 3.14159, 1.0e10, 1.0e100] do
        assert_roundtrip(f)
      end
    end

    test "handles negative floats" do
      for f <- [-1.0, -3.14159, -1.0e10, -1.0e100] do
        assert_roundtrip(f)
      end
    end

    test "handles very small floats" do
      for f <- [1.0e-10, 1.0e-100, 1.0e-308] do
        assert_roundtrip(f)
      end
    end
  end

  describe "ordering preservation" do
    property "maintains ordering for positive integers" do
      check all({int1, int2} <- {positive_integer(), positive_integer()}) do
        packed1 = Key.pack({int1})
        packed2 = Key.pack({int2})

        cond do
          int1 < int2 -> assert packed1 < packed2
          int1 > int2 -> assert packed1 > packed2
          true -> assert packed1 == packed2
        end
      end
    end

    property "maintains ordering for negative integers" do
      check all(
              int1 <- integer(-1_000_000..-1),
              int2 <- integer(-1_000_000..-1)
            ) do
        packed1 = Key.pack({int1})
        packed2 = Key.pack({int2})

        cond do
          int1 < int2 -> assert packed1 < packed2
          int1 > int2 -> assert packed1 > packed2
          true -> assert packed1 == packed2
        end
      end
    end

    property "maintains ordering for binaries" do
      check all(
              bin1 <- binary(),
              bin2 <- binary()
            ) do
        packed1 = Key.pack({bin1})
        packed2 = Key.pack({bin2})

        cond do
          bin1 < bin2 -> assert packed1 < packed2
          bin1 > bin2 -> assert packed1 > packed2
          true -> assert packed1 == packed2
        end
      end
    end

    test "maintains ordering for tuples with multiple elements" do
      tuples = [
        {1, "a"},
        {1, "b"},
        {2, "a"},
        {2, "b"}
      ]

      packed = Enum.map(tuples, &Key.pack/1)

      # Verify packed tuples maintain the same order
      assert Enum.sort(packed) == packed
    end
  end

  property "packed data contains no unescaped null terminators in value sections" do
    check all(
            data <-
              one_of([
                binary(),
                tuple({binary()}),
                list_of(binary(), max_length: 5)
              ])
          ) do
      packed = Key.pack(data)

      # The packed data should be a valid binary
      assert is_binary(packed)

      # We should be able to unpack it successfully
      unpacked = Key.unpack(packed)
      assert unpacked == data
    end
  end

  property "packing is deterministic across multiple calls" do
    check all(
            data <-
              one_of([
                integer(),
                binary(),
                tuple({integer(), binary()}),
                list_of(integer(), max_length: 10)
              ])
          ) do
      packed1 = Key.pack(data)
      packed2 = Key.pack(data)
      packed3 = Key.pack(data)

      assert packed1 == packed2
      assert packed2 == packed3
    end
  end

  describe "error conditions" do
    test "raises on malformed packed data" do
      malformed = <<0xFF, 0xFF, 0xFF>>
      assert_raise ArgumentError, fn -> Key.unpack(malformed) end
    end

    test "raises on packed data with extra bytes" do
      packed = Key.pack(42)
      packed_with_extra = packed <> <<0xFF>>
      assert_raise ArgumentError, ~r/Extra data after key/, fn -> Key.unpack(packed_with_extra) end
    end

    test "raises on unexpected end of binary during unpacking" do
      # Incomplete binary tag
      incomplete = <<0x01>>
      assert_raise ArgumentError, ~r/Unexpected end/, fn -> Key.unpack(incomplete) end
    end

    test "raises on unsupported atom type" do
      assert_raise ArgumentError, ~r/Unsupported data type/, fn -> Key.pack(:atom) end
    end

    test "raises on unsupported map type" do
      assert_raise ArgumentError, ~r/Unsupported data type/, fn -> Key.pack(%{key: :value}) end
    end
  end
end
