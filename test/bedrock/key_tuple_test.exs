defmodule Bedrock.Key.TupleTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.Key

  doctest Key

  property "pack and unpack nil" do
    check all(_x <- constant(nil)) do
      packed = Key.pack(nil)
      unpacked = Key.unpack(packed)
      assert unpacked == nil
    end
  end

  property "pack and unpack integers" do
    check all(int <- integer()) do
      packed = Key.pack(int)
      unpacked = Key.unpack(packed)
      assert unpacked == int
    end
  end

  property "pack and unpack floats" do
    check all(float <- float()) do
      packed = Key.pack(float)
      unpacked = Key.unpack(packed)
      assert unpacked == float
    end
  end

  property "pack and unpack binaries" do
    check all(binary <- binary()) do
      packed = Key.pack(binary)
      unpacked = Key.unpack(packed)
      assert unpacked == binary
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

  property "pack and unpack simple tuples" do
    check all(
            tuple <-
              one_of([
                tuple({integer()}),
                tuple({float()}),
                tuple({binary()}),
                tuple({integer(), binary()}),
                tuple({integer(), float(), binary()})
              ])
          ) do
      packed = Key.pack(tuple)
      unpacked = Key.unpack(packed)
      assert unpacked == tuple
    end
  end

  property "pack and unpack nested tuples" do
    check all(
            nested_tuple <-
              one_of([
                tuple({integer(), tuple({float(), binary()})}),
                tuple({tuple({integer(), binary()}), float()}),
                tuple({tuple({integer()}), tuple({binary()}), float()})
              ])
          ) do
      packed = Key.pack(nested_tuple)
      unpacked = Key.unpack(packed)
      assert unpacked == nested_tuple
    end
  end

  property "pack and unpack mixed data types" do
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
      packed = Key.pack(data)
      unpacked = Key.unpack(packed)
      assert unpacked == data
    end
  end

  property "packed data produces deterministic results" do
    check all(
            data <-
              one_of([
                integer(),
                float(),
                binary(),
                tuple({integer(), binary()})
              ])
          ) do
      packed1 = Key.pack(data)
      packed2 = Key.pack(data)
      assert packed1 == packed2
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

  property "roundtrip property - pack then unpack returns original" do
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
      assert data |> Key.pack() |> Key.unpack() == data
    end
  end

  test "pack returns binary" do
    assert is_binary(Key.pack(42))
    assert is_binary(Key.pack("hello"))
    assert is_binary(Key.pack({1, 2, 3}))
    assert is_binary(Key.pack(nil))
  end

  test "specific edge cases" do
    # Test zero
    assert 0 |> Key.pack() |> Key.unpack() == 0

    # Test empty binary
    assert <<>> |> Key.pack() |> Key.unpack() == <<>>

    # Test empty tuple
    assert {} |> Key.pack() |> Key.unpack() == {}

    # Test single element tuple
    assert {42} |> Key.pack() |> Key.unpack() == {42}

    # Test binary with only null bytes
    null_binary = <<0, 0, 0>>
    assert null_binary |> Key.pack() |> Key.unpack() == null_binary
  end

  test "unsupported data types raise errors" do
    assert_raise ArgumentError, fn ->
      Key.pack(:atom)
    end

    assert_raise ArgumentError, fn ->
      Key.pack(%{key: :value})
    end

    # Lists are now supported, so no error expected
    assert is_binary(Key.pack([1, 2, 3]))
  end

  describe "pack/1 with lists" do
    test "packs lists differently from tuples (separate encodings)" do
      list = ["users", 123, "active"]
      tuple = {"users", 123, "active"}

      # Lists and tuples now have different encodings
      assert Key.pack(list) != Key.pack(tuple)
      # But list encoding should be > tuple encoding (lists > tuples in Elixir)
      assert Key.pack(list) > Key.pack(tuple)
    end

    test "handles empty lists vs empty tuples" do
      empty_list = []
      empty_tuple = {}

      # Different encodings for empty containers too
      assert Key.pack(empty_list) != Key.pack(empty_tuple)
      assert Key.pack(empty_list) > Key.pack(empty_tuple)
    end

    test "round trip: list -> pack -> unpack" do
      list = ["users", 123, "active"]
      packed = Key.pack(list)
      assert Key.unpack(packed) == list
    end

    test "round trip: tuple -> pack -> unpack" do
      tuple = {"users", 123, "active"}
      packed = Key.pack(tuple)
      assert Key.unpack(packed) == tuple
    end

    test "list to tuple conversion" do
      list = ["users", 123, "active"]
      tuple = List.to_tuple(list)
      # Lists and tuples have different encodings but can be converted
      assert Key.pack(list) != Key.pack(tuple)
      assert tuple |> Key.pack() |> Key.unpack() |> Tuple.to_list() == list
    end
  end
end
