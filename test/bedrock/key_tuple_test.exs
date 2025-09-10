defmodule Bedrock.Key.TupleTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.Key

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
end
