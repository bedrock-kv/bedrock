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

    test "handles multi-byte carry with overflow" do
      # 255 + 1 = 256 (overflow from first byte to second)
      assert Atomics.add(<<255>>, <<1>>) == <<0, 1>>

      # Multiple carry propagation: 255,255 + 1 = 256,0 = 0,0,1
      assert Atomics.add(<<255, 255>>, <<1, 0>>) == <<0, 0, 1>>
    end

    test "handles carry propagation through padded zeros and remaining operand bytes" do
      # Existing is shorter, gets padded to match operand length
      # Example: <<255>> padded to <<255, 0, 0>> + <<1, 255, 255>>
      # After padding, both sides are equal length so lines 42-44 won't be hit directly
      # But carries propagate through: 256 → 256 → 256 → final carry
      assert Atomics.add(<<255>>, <<1, 255, 255>>) == <<0, 0, 0, 1>>
    end

    test "handles empty existing with multi-byte operand and carry" do
      # To hit lines 42-44, we need existing to run out while operand has bytes
      # This happens when existing is truly shorter and padding uses zeros:
      # <<>> padded to <<>> + <<255, 255>> stays as <<>>
      # Actually, padding would make it <<0, 0>>, so let's test the edge case
      # where existing is empty from the start
      assert Atomics.add(<<>>, <<255, 255>>) == <<255, 255>>
    end

    test "handles carry propagation with shorter existing value" do
      # When existing is shorter than operand, it gets padded with zeros
      # and carry propagates through the padded portion
      #
      # Example: <<254>> + <<2, 1>>
      # After padding: <<254, 0>> + <<2, 1>>
      # Byte 0: 254 + 2 + 0 = 256 → result byte 0, carry 1
      # Byte 1: 0 + 1 + 1 = 2 → result byte 2, carry 0
      # Result: <<0, 2>>
      assert Atomics.add(<<254>>, <<2, 1>>) == <<0, 2>>

      # Another case with more dramatic carry:
      # <<255>> + <<255, 1>>
      # After padding: <<255, 0>> + <<255, 1>>
      # Byte 0: 255 + 255 + 0 = 510 → result 254, carry 1
      # Byte 1: 0 + 1 + 1 = 2 → result 2, carry 0
      # Result: <<254, 2>>
      assert Atomics.add(<<255>>, <<255, 1>>) == <<254, 2>>
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

  describe "compare_and_clear/2" do
    test "clears when values match" do
      # Exact match should clear (return empty binary)
      assert Atomics.compare_and_clear(<<5>>, <<5>>) == <<>>
      assert Atomics.compare_and_clear(<<1, 2, 3>>, <<1, 2, 3>>) == <<>>
    end

    test "keeps existing when values don't match" do
      # No match should keep existing value
      assert Atomics.compare_and_clear(<<5>>, <<3>>) == <<5>>
      assert Atomics.compare_and_clear(<<1, 2>>, <<1, 3>>) == <<1, 2>>
    end

    test "handles empty binaries" do
      # Both empty should clear
      assert Atomics.compare_and_clear(<<>>, <<>>) == <<>>
    end

    test "non-empty vs empty doesn't match" do
      # Different lengths don't match
      assert Atomics.compare_and_clear(<<5>>, <<>>) == <<5>>
      assert Atomics.compare_and_clear(<<>>, <<5>>) == <<>>
    end
  end
end
