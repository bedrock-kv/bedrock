defmodule Bedrock.Encoding.TupleTest do
  use ExUnit.Case, async: true

  alias Bedrock.Encoding.Tuple, as: TupleEncoding

  describe "pack/1 and unpack/1 for integers" do
    test "encodes and decodes zero" do
      assert 0 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0
    end

    test "encodes and decodes small positive integers (1 byte)" do
      assert 1 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 1
      assert 255 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 255
    end

    test "encodes and decodes 2-byte positive integers" do
      assert 256 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 256
      assert 0xFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0xFFFF
    end

    test "encodes and decodes 3-byte positive integers" do
      assert 0x10000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0x10000
      assert 0xFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0xFFFFFF
    end

    test "encodes and decodes 4-byte positive integers" do
      assert 0x1000000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0x1000000
      assert 0xFFFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0xFFFFFFFF
    end

    test "encodes and decodes 5-byte positive integers" do
      assert 0x100000000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0x100000000
      assert 0xFFFFFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == 0xFFFFFFFFFF
    end

    test "encodes and decodes 6-byte positive integers" do
      # 48-bit values
      value_6byte = 0x1000000000000
      max_6byte = 0xFFFFFFFFFFFF
      assert value_6byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_6byte
      assert max_6byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_6byte
    end

    test "encodes and decodes 7-byte positive integers" do
      # 56-bit values
      value_7byte = 0x10000000000000
      max_7byte = 0xFFFFFFFFFFFFFF
      assert value_7byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_7byte
      assert max_7byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_7byte
    end

    test "encodes and decodes 8-byte positive integers" do
      # 64-bit values
      value_8byte = 0x100000000000000
      max_8byte = 0xFFFFFFFFFFFFFFFF
      assert value_8byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_8byte
      assert max_8byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_8byte
    end

    test "encodes and decodes small negative integers (1 byte)" do
      assert -1 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -1
      assert -255 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -255
    end

    test "encodes and decodes 2-byte negative integers" do
      assert -256 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -256
      assert -0xFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0xFFFF
    end

    test "encodes and decodes 3-byte negative integers" do
      assert -0x10000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0x10000
      assert -0xFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0xFFFFFF
    end

    test "encodes and decodes 4-byte negative integers" do
      assert -0x1000000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0x1000000
      assert -0xFFFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0xFFFFFFFF
    end

    test "encodes and decodes 5-byte negative integers" do
      assert -0x100000000 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0x100000000
      assert -0xFFFFFFFFFF |> TupleEncoding.pack() |> TupleEncoding.unpack() == -0xFFFFFFFFFF
    end

    test "encodes and decodes 6-byte negative integers" do
      # 48-bit negative values
      value_6byte = -0x1000000000000
      max_6byte = -0xFFFFFFFFFFFF
      assert value_6byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_6byte
      assert max_6byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_6byte
    end

    test "encodes and decodes 7-byte negative integers" do
      # 56-bit negative values
      value_7byte = -0x10000000000000
      max_7byte = -0xFFFFFFFFFFFFFF
      assert value_7byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_7byte
      assert max_7byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_7byte
    end

    test "encodes and decodes 8-byte negative integers" do
      # 64-bit negative values
      value_8byte = -0x100000000000000
      max_8byte = -0xFFFFFFFFFFFFFFFF
      assert value_8byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == value_8byte
      assert max_8byte |> TupleEncoding.pack() |> TupleEncoding.unpack() == max_8byte
    end
  end

  describe "pack/1 and unpack/1 for other types" do
    test "encodes and decodes nil" do
      assert nil |> TupleEncoding.pack() |> TupleEncoding.unpack() == nil
    end

    test "encodes and decodes binaries" do
      assert "hello" |> TupleEncoding.pack() |> TupleEncoding.unpack() == "hello"
      assert <<0, 1, 2>> |> TupleEncoding.pack() |> TupleEncoding.unpack() == <<0, 1, 2>>
    end

    test "encodes and decodes floats" do
      assert 3.14 |> TupleEncoding.pack() |> TupleEncoding.unpack() == 3.14
      assert -2.5 |> TupleEncoding.pack() |> TupleEncoding.unpack() == -2.5
    end

    test "encodes and decodes lists" do
      assert [1, 2, 3] |> TupleEncoding.pack() |> TupleEncoding.unpack() == [1, 2, 3]
      assert [] |> TupleEncoding.pack() |> TupleEncoding.unpack() == []
    end

    test "encodes and decodes tuples" do
      assert {1, 2, 3} |> TupleEncoding.pack() |> TupleEncoding.unpack() == {1, 2, 3}
      assert {} |> TupleEncoding.pack() |> TupleEncoding.unpack() == {}
    end

    test "encodes and decodes nested structures" do
      complex = {1, "hello", [2, 3], {4, 5}}
      assert complex |> TupleEncoding.pack() |> TupleEncoding.unpack() == complex
    end
  end

  describe "error handling" do
    test "raises on unsupported type during packing" do
      assert_raise ArgumentError, ~r/Unsupported data type/, fn ->
        TupleEncoding.pack(%{key: :value})
      end
    end

    test "raises on malformed data during unpacking" do
      # Invalid tag
      assert_raise ArgumentError, ~r/Unsupported or malformed data/, fn ->
        TupleEncoding.unpack(<<0xFF>>)
      end
    end

    test "raises on extra data after value" do
      # Valid integer followed by extra bytes
      packed_int = TupleEncoding.pack(42)
      invalid = packed_int <> <<0xFF, 0xFF>>

      assert_raise ArgumentError, ~r/Extra data after key/, fn ->
        TupleEncoding.unpack(invalid)
      end
    end
  end
end
