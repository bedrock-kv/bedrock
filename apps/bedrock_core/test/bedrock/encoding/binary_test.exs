defmodule Bedrock.Encoding.NoneTest do
  use ExUnit.Case, async: true

  alias Bedrock.Encoding.None

  describe "pack/1" do
    test "returns binary value unchanged" do
      assert None.pack(<<"test">>) == <<"test">>
      assert None.pack(<<1, 2, 3>>) == <<1, 2, 3>>
      assert None.pack(<<>>) == <<>>
    end
  end

  describe "unpack/1" do
    test "returns packed value unchanged" do
      assert None.unpack(<<"test">>) == <<"test">>
      assert None.unpack(<<1, 2, 3>>) == <<1, 2, 3>>
      assert None.unpack(<<>>) == <<>>
    end
  end

  describe "round-trip" do
    test "pack and unpack are identity operations" do
      binaries = [
        <<"hello">>,
        <<0, 1, 2, 3, 4, 5>>,
        <<255, 254, 253>>,
        <<"complex binary with special chars: \n\t\r">>,
        <<>>
      ]

      for binary <- binaries do
        assert binary |> None.pack() |> None.unpack() == binary
      end
    end
  end
end
