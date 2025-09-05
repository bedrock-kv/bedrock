defmodule Bedrock.TupleTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.Tuple

  property "pack and unpack nil" do
    check all(_x <- constant(nil)) do
      packed = Tuple.pack(nil)
      unpacked = Tuple.unpack(packed)
      assert unpacked == nil
    end
  end

  property "pack and unpack integers" do
    check all(int <- integer()) do
      packed = Tuple.pack(int)
      unpacked = Tuple.unpack(packed)
      assert unpacked == int
    end
  end

  property "pack and unpack floats" do
    check all(float <- float()) do
      packed = Tuple.pack(float)
      unpacked = Tuple.unpack(packed)
      assert unpacked == float
    end
  end

  property "pack and unpack binaries" do
    check all(binary <- binary()) do
      packed = Tuple.pack(binary)
      unpacked = Tuple.unpack(packed)
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

      packed = Tuple.pack(binary_with_nulls)
      unpacked = Tuple.unpack(packed)
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
      packed = Tuple.pack(tuple)
      unpacked = Tuple.unpack(packed)
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
      packed = Tuple.pack(nested_tuple)
      unpacked = Tuple.unpack(packed)
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
      packed = Tuple.pack(data)
      unpacked = Tuple.unpack(packed)
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
      packed1 = Tuple.pack(data)
      packed2 = Tuple.pack(data)
      assert packed1 == packed2
    end
  end

  property "packed data maintains ordering for integers" do
    check all({int1, int2} <- {integer(), integer()}) do
      packed1 = Tuple.pack(int1)
      packed2 = Tuple.pack(int2)

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
      assert data |> Tuple.pack() |> Tuple.unpack() == data
    end
  end

  test "pack returns binary" do
    assert is_binary(Tuple.pack(42))
    assert is_binary(Tuple.pack("hello"))
    assert is_binary(Tuple.pack({1, 2, 3}))
    assert is_binary(Tuple.pack(nil))
  end

  test "specific edge cases" do
    # Test zero
    assert 0 |> Tuple.pack() |> Tuple.unpack() == 0

    # Test empty binary
    assert <<>> |> Tuple.pack() |> Tuple.unpack() == <<>>

    # Test empty tuple
    assert {} |> Tuple.pack() |> Tuple.unpack() == {}

    # Test single element tuple
    assert {42} |> Tuple.pack() |> Tuple.unpack() == {42}

    # Test binary with only null bytes
    null_binary = <<0, 0, 0>>
    assert null_binary |> Tuple.pack() |> Tuple.unpack() == null_binary
  end

  test "unsupported data types raise errors" do
    assert_raise ArgumentError, fn ->
      Tuple.pack(:atom)
    end

    assert_raise ArgumentError, fn ->
      Tuple.pack(%{key: :value})
    end

    assert_raise ArgumentError, fn ->
      Tuple.pack([1, 2, 3])
    end
  end
end
