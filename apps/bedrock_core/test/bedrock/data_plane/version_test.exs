defmodule Bedrock.DataPlane.VersionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Version

  describe "zero/0" do
    test "returns zero version" do
      assert Version.zero() == <<0::unsigned-big-64>>
    end

    test "zero version is 8 bytes" do
      assert byte_size(Version.zero()) == 8
    end
  end

  describe "first?/1" do
    test "returns true for zero version" do
      assert Version.first?(Version.zero()) == true
    end

    test "returns false for non-zero version" do
      assert Version.first?(Version.from_integer(1)) == false
      assert Version.first?(Version.from_integer(100)) == false
    end

    test "returns false for maximum version" do
      assert Version.first?(Version.from_integer(0xFFFFFFFFFFFFFFFF)) == false
    end
  end

  describe "from_integer/1" do
    test "converts 0 to version" do
      assert Version.from_integer(0) == <<0::unsigned-big-64>>
    end

    test "converts positive integer to version" do
      assert Version.from_integer(42) == <<42::unsigned-big-64>>
    end

    test "converts large integer to version" do
      assert Version.from_integer(0xFFFFFFFFFFFFFFFF) == <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
    end

    test "result is always 8 bytes" do
      assert byte_size(Version.from_integer(0)) == 8
      assert byte_size(Version.from_integer(1)) == 8
      assert byte_size(Version.from_integer(0xFFFFFFFFFFFFFFFF)) == 8
    end

    property "always produces 8-byte version" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFF)) do
        version = Version.from_integer(int)
        assert byte_size(version) == 8
      end
    end
  end

  describe "from_bytes/1" do
    test "accepts 8-byte binary" do
      bytes = <<1, 2, 3, 4, 5, 6, 7, 8>>
      assert Version.from_bytes(bytes) == bytes
    end

    test "identity function for valid version" do
      version = Version.from_integer(12_345)
      assert Version.from_bytes(version) == version
    end

    property "identity for all valid versions" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFF)) do
        version = Version.from_integer(int)
        assert Version.from_bytes(version) == version
      end
    end
  end

  describe "to_integer/1" do
    test "converts zero version to 0" do
      assert Version.to_integer(Version.zero()) == 0
    end

    test "converts version to integer" do
      assert Version.to_integer(<<42::unsigned-big-64>>) == 42
    end

    test "converts max version to max integer" do
      assert Version.to_integer(<<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>) == 0xFFFFFFFFFFFFFFFF
    end

    property "inverse of from_integer" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFF)) do
        version = Version.from_integer(int)
        assert Version.to_integer(version) == int
      end
    end
  end

  describe "to_string/1" do
    test "converts nil to string" do
      assert Version.to_string(nil) == "nil"
    end

    test "converts zero version to string" do
      result = Version.to_string(Version.zero())
      assert result == "<0,0,0,0,0,0,0,0>"
    end

    test "converts version to readable string" do
      version = <<1, 2, 3, 4, 5, 6, 7, 8>>
      result = Version.to_string(version)
      assert result == "<1,2,3,4,5,6,7,8>"
    end

    test "handles maximum values in bytes" do
      version = <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
      result = Version.to_string(version)
      assert result == "<255,255,255,255,255,255,255,255>"
    end
  end

  describe "increment/1" do
    test "increments zero version" do
      assert Version.increment(Version.zero()) == Version.from_integer(1)
    end

    test "increments version by 1" do
      assert Version.increment(Version.from_integer(42)) == Version.from_integer(43)
    end

    test "increments preserves version format" do
      result = Version.increment(Version.from_integer(100))
      assert byte_size(result) == 8
      assert Version.to_integer(result) == 101
    end

    property "always increments by exactly 1" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFE)) do
        version = Version.from_integer(int)
        incremented = Version.increment(version)
        assert Version.to_integer(incremented) == int + 1
      end
    end
  end

  describe "newer?/2" do
    test "returns true when first version is newer" do
      assert Version.newer?(Version.from_integer(10), Version.from_integer(5)) == true
    end

    test "returns false when first version is older" do
      assert Version.newer?(Version.from_integer(5), Version.from_integer(10)) == false
    end

    test "returns false when versions are equal" do
      version = Version.from_integer(42)
      assert Version.newer?(version, version) == false
    end

    property "newer is anti-symmetric" do
      check all(
              int1 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int2 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int1 != int2
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)

        assert Version.newer?(v1, v2) != Version.newer?(v2, v1)
      end
    end
  end

  describe "older?/2" do
    test "returns true when first version is older" do
      assert Version.older?(Version.from_integer(5), Version.from_integer(10)) == true
    end

    test "returns false when first version is newer" do
      assert Version.older?(Version.from_integer(10), Version.from_integer(5)) == false
    end

    test "returns false when versions are equal" do
      version = Version.from_integer(42)
      assert Version.older?(version, version) == false
    end

    property "older is opposite of newer" do
      check all(
              int1 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int2 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int1 != int2
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)

        assert Version.older?(v1, v2) == Version.newer?(v2, v1)
      end
    end
  end

  describe "compare/2" do
    test "returns :lt when first is less than second" do
      assert Version.compare(Version.from_integer(5), Version.from_integer(10)) == :lt
    end

    test "returns :gt when first is greater than second" do
      assert Version.compare(Version.from_integer(10), Version.from_integer(5)) == :gt
    end

    test "returns :eq when versions are equal" do
      version = Version.from_integer(42)
      assert Version.compare(version, version) == :eq
    end

    property "compare is consistent with integer comparison" do
      check all(
              int1 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int2 <- integer(0..0xFFFFFFFFFFFFFFFF)
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)
        result = Version.compare(v1, v2)

        cond do
          int1 < int2 -> assert result == :lt
          int1 > int2 -> assert result == :gt
          int1 == int2 -> assert result == :eq
        end
      end
    end

    property "compare is transitive" do
      check all(
              int1 <- integer(0..1000),
              int2 <- integer(0..1000),
              int3 <- integer(0..1000)
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)
        v3 = Version.from_integer(int3)

        # If v1 < v2 and v2 < v3, then v1 < v3
        if Version.compare(v1, v2) == :lt and Version.compare(v2, v3) == :lt do
          assert Version.compare(v1, v3) == :lt
        end
      end
    end
  end

  describe "valid?/1" do
    test "returns true for valid 8-byte version" do
      assert Version.valid?(<<0::unsigned-big-64>>) == true
      assert Version.valid?(Version.from_integer(42)) == true
    end

    test "returns false for non-binary" do
      assert Version.valid?(nil) == false
      assert Version.valid?(42) == false
      assert Version.valid?(:atom) == false
      assert Version.valid?([1, 2, 3]) == false
    end

    test "returns false for wrong-sized binary" do
      assert Version.valid?(<<1>>) == false
      assert Version.valid?(<<1, 2>>) == false
      assert Version.valid?(<<1, 2, 3, 4>>) == false
      assert Version.valid?(<<1, 2, 3, 4, 5, 6, 7>>) == false
      assert Version.valid?(<<1, 2, 3, 4, 5, 6, 7, 8, 9>>) == false
    end

    property "all from_integer results are valid" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFF)) do
        version = Version.from_integer(int)
        assert Version.valid?(version) == true
      end
    end
  end

  describe "add/2" do
    test "adds positive offset to version" do
      version = Version.from_integer(10)
      result = Version.add(version, 5)
      assert Version.to_integer(result) == 15
    end

    test "adds negative offset to version" do
      version = Version.from_integer(10)
      result = Version.add(version, -5)
      assert Version.to_integer(result) == 5
    end

    test "adding zero returns same version value" do
      version = Version.from_integer(42)
      result = Version.add(version, 0)
      assert Version.to_integer(result) == 42
    end

    test "raises on overflow" do
      version = Version.from_integer(0xFFFFFFFFFFFFFFFF)

      assert_raise ArgumentError, ~r/version arithmetic overflow/, fn ->
        Version.add(version, 1)
      end
    end

    test "raises on underflow" do
      version = Version.from_integer(5)

      assert_raise ArgumentError, ~r/version arithmetic overflow/, fn ->
        Version.add(version, -10)
      end
    end

    property "add maintains version validity" do
      check all(
              int <- integer(100..1000),
              offset <- integer(-50..50)
            ) do
        version = Version.from_integer(int)
        result = Version.add(version, offset)
        assert Version.valid?(result)
        assert Version.to_integer(result) == int + offset
      end
    end
  end

  describe "subtract/2" do
    test "subtracts offset from version" do
      version = Version.from_integer(10)
      result = Version.subtract(version, 5)
      assert Version.to_integer(result) == 5
    end

    test "subtract is inverse of add for positive offset" do
      version = Version.from_integer(100)
      added = Version.add(version, 10)
      result = Version.subtract(added, 10)
      assert result == version
    end

    test "raises on underflow" do
      version = Version.from_integer(5)

      assert_raise ArgumentError, ~r/version arithmetic overflow/, fn ->
        Version.subtract(version, 10)
      end
    end

    property "subtract is equivalent to add with negative offset" do
      check all(
              int <- integer(100..1000),
              offset <- integer(0..50)
            ) do
        version = Version.from_integer(int)
        result1 = Version.subtract(version, offset)
        result2 = Version.add(version, -offset)
        assert result1 == result2
      end
    end
  end

  describe "distance/2" do
    test "calculates distance between versions" do
      v1 = Version.from_integer(10)
      v2 = Version.from_integer(5)
      assert Version.distance(v1, v2) == 5
    end

    test "distance can be negative" do
      v1 = Version.from_integer(5)
      v2 = Version.from_integer(10)
      assert Version.distance(v1, v2) == -5
    end

    test "distance from version to itself is zero" do
      version = Version.from_integer(42)
      assert Version.distance(version, version) == 0
    end

    property "distance is anti-symmetric" do
      check all(
              int1 <- integer(0..10_000),
              int2 <- integer(0..10_000)
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)
        assert Version.distance(v1, v2) == -Version.distance(v2, v1)
      end
    end

    property "distance matches integer difference" do
      check all(
              int1 <- integer(0..0xFFFFFFFFFFFFFFFF),
              int2 <- integer(0..0xFFFFFFFFFFFFFFFF)
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)
        assert Version.distance(v1, v2) == int1 - int2
      end
    end
  end

  describe "sequence/2" do
    test "generates sequence of versions" do
      base = Version.from_integer(10)
      sequence = Version.sequence(base, 3)

      assert length(sequence) == 3
      assert Enum.at(sequence, 0) == Version.from_integer(11)
      assert Enum.at(sequence, 1) == Version.from_integer(12)
      assert Enum.at(sequence, 2) == Version.from_integer(13)
    end

    test "generates sequence from zero" do
      sequence = Version.sequence(Version.zero(), 5)

      assert length(sequence) == 5
      assert Enum.at(sequence, 0) == Version.from_integer(1)
      assert Enum.at(sequence, 4) == Version.from_integer(5)
    end

    test "generates single version sequence" do
      base = Version.from_integer(100)
      sequence = Version.sequence(base, 1)

      assert length(sequence) == 1
      assert Enum.at(sequence, 0) == Version.from_integer(101)
    end

    test "raises on overflow" do
      base = Version.from_integer(0xFFFFFFFFFFFFFFFF - 5)

      assert_raise ArgumentError, ~r/version sequence overflow/, fn ->
        Version.sequence(base, 10)
      end
    end

    property "sequence has correct length" do
      check all(
              int <- integer(0..1000),
              count <- integer(1..100)
            ) do
        base = Version.from_integer(int)
        sequence = Version.sequence(base, count)
        assert length(sequence) == count
      end
    end

    property "sequence is consecutive" do
      check all(
              int <- integer(0..1000),
              count <- integer(1..50)
            ) do
        base = Version.from_integer(int)
        sequence = Version.sequence(base, count)

        # Each element should be base + index
        sequence
        |> Enum.with_index(1)
        |> Enum.each(fn {version, index} ->
          assert Version.to_integer(version) == int + index
        end)
      end
    end

    property "sequence elements are all valid versions" do
      check all(
              int <- integer(0..1000),
              count <- integer(1..50)
            ) do
        base = Version.from_integer(int)
        sequence = Version.sequence(base, count)

        Enum.each(sequence, fn version ->
          assert Version.valid?(version)
        end)
      end
    end
  end

  describe "version arithmetic properties" do
    property "increment is equivalent to add 1" do
      check all(int <- integer(0..0xFFFFFFFFFFFFFFFE)) do
        version = Version.from_integer(int)
        assert Version.increment(version) == Version.add(version, 1)
      end
    end

    property "adding distances gives target version" do
      check all(
              int1 <- integer(0..10_000),
              int2 <- integer(0..10_000)
            ) do
        v1 = Version.from_integer(int1)
        v2 = Version.from_integer(int2)
        distance = Version.distance(v2, v1)

        # v1 + distance(v2, v1) = v2
        result = Version.add(v1, distance)
        assert result == v2
      end
    end
  end

  describe "edge cases" do
    test "handles zero version" do
      zero = Version.zero()
      assert Version.to_integer(zero) == 0
      assert Version.first?(zero) == true
      assert Version.valid?(zero) == true
    end

    test "handles maximum version" do
      max_version = Version.from_integer(0xFFFFFFFFFFFFFFFF)
      assert Version.to_integer(max_version) == 0xFFFFFFFFFFFFFFFF
      assert Version.valid?(max_version) == true
      assert Version.first?(max_version) == false
    end

    test "version ordering works across full range" do
      versions = [
        Version.from_integer(0),
        Version.from_integer(1),
        Version.from_integer(0xFF),
        Version.from_integer(0xFFFF),
        Version.from_integer(0xFFFFFFFF),
        Version.from_integer(0xFFFFFFFFFFFFFFFF)
      ]

      sorted = Enum.sort(versions)
      assert sorted == versions
    end
  end

  describe "is_version/1 guard" do
    import Version, only: [is_version: 1]

    # Helper function that uses the guard
    defp check_version(v) when is_version(v), do: :valid
    defp check_version(_), do: :invalid

    test "accepts valid 8-byte version" do
      assert check_version(Version.zero()) == :valid
      assert check_version(Version.from_integer(42)) == :valid
      assert check_version(<<1, 2, 3, 4, 5, 6, 7, 8>>) == :valid
    end

    test "rejects non-binary values" do
      assert check_version(nil) == :invalid
      assert check_version(42) == :invalid
      assert check_version(:atom) == :invalid
      assert check_version([1, 2, 3]) == :invalid
    end

    test "rejects wrong-sized binaries" do
      assert check_version(<<1>>) == :invalid
      assert check_version(<<1, 2>>) == :invalid
      assert check_version(<<1, 2, 3, 4>>) == :invalid
      assert check_version(<<1, 2, 3, 4, 5, 6, 7>>) == :invalid
      assert check_version(<<1, 2, 3, 4, 5, 6, 7, 8, 9>>) == :invalid
    end
  end
end
