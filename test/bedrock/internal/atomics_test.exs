defmodule Bedrock.Internal.AtomicsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Atomics

  describe "add/2" do
    test "adds little-endian binary values" do
      assert Atomics.add(<<5>>, <<3>>) == <<8>>
    end

    test "adds with carry propagation" do
      assert Atomics.add(<<255>>, <<1>>) == <<0, 1>>
    end

    test "handles missing existing value" do
      assert Atomics.add(<<>>, <<5, 0>>) == <<5, 0>>
    end

    test "handles empty operand" do
      assert Atomics.add(<<5, 0>>, <<>>) == <<>>
    end

    test "pads existing to match operand length" do
      assert Atomics.add(<<5>>, <<3, 1>>) == <<8, 1>>
    end
  end

  describe "min/2" do
    test "returns minimum of two values" do
      assert Atomics.min(<<5>>, <<3>>) == <<3>>
    end

    test "returns operand when existing is missing" do
      assert Atomics.min(<<>>, <<10, 0>>) == <<10, 0>>
    end

    test "handles empty operand" do
      assert Atomics.min(<<5>>, <<>>) == <<>>
    end

    test "pads existing to operand length for comparison" do
      assert Atomics.min(<<5>>, <<0, 1>>) == <<5, 0>>
    end

    test "compares multi-byte values correctly" do
      # 256 (<<0, 1>>) vs 255 (<<255>>) - 255 padded should be minimum
      assert Atomics.min(<<255>>, <<0, 1>>) == <<255, 0>>
    end
  end

  describe "max/2" do
    test "returns maximum of two values" do
      assert Atomics.max(<<5>>, <<3>>) == <<5>>
    end

    test "returns operand when existing is missing" do
      assert Atomics.max(<<>>, <<10, 0>>) == <<10, 0>>
    end

    test "handles empty operand" do
      assert Atomics.max(<<5>>, <<>>) == <<>>
    end

    test "pads existing to operand length for comparison" do
      assert Atomics.max(<<5>>, <<0, 1>>) == <<0, 1>>
    end

    test "compares multi-byte values correctly" do
      # 256 (<<0, 1>>) vs 255 (<<255>>) - 256 should be maximum
      assert Atomics.max(<<255>>, <<0, 1>>) == <<0, 1>>
      # 65537 (<<1, 0, 1>>) vs 65536 (<<0, 0, 1>>) - 65537 should be maximum
      assert Atomics.max(<<0, 0, 1>>, <<1, 0, 1>>) == <<1, 0, 1>>
    end
  end
end
