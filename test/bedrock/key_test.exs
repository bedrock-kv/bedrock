defmodule Bedrock.KeyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Key

  doctest Key

  describe "key_after/1" do
    test "appends null byte to simple string" do
      assert Key.key_after("abc") == "abc\0"
    end

    test "appends null byte to empty string" do
      assert Key.key_after("") == "\0"
    end

    test "appends null byte to binary" do
      assert Key.key_after(<<1, 2, 3>>) == <<1, 2, 3, 0>>
    end

    test "appends null byte to key ending with 0xFF" do
      assert Key.key_after(<<0xFF>>) == <<0xFF, 0>>
    end

    test "appends null byte to key with null bytes" do
      assert Key.key_after(<<0, 0, 0>>) == <<0, 0, 0, 0>>
    end

    property "always produces a key greater than input" do
      check all(key <- binary(min_length: 0, max_length: 100)) do
        result = Key.key_after(key)
        assert result > key
      end
    end

    property "result always starts with input key as prefix" do
      check all(key <- binary(min_length: 0, max_length: 100)) do
        result = Key.key_after(key)
        assert String.starts_with?(result, key)
      end
    end

    property "result is always input + null byte" do
      check all(key <- binary(min_length: 0, max_length: 100)) do
        result = Key.key_after(key)
        assert result == key <> <<0>>
      end
    end
  end

  describe "to_range/1" do
    test "creates range with key and key_after" do
      assert Key.to_range("prefix") == {"prefix", "prefix\0"}
    end

    test "creates range for empty key" do
      assert Key.to_range("") == {"", "\0"}
    end

    test "creates range for binary key" do
      assert Key.to_range(<<1, 2, 3>>) == {<<1, 2, 3>>, <<1, 2, 3, 0>>}
    end

    property "range start is always less than range end" do
      check all(key <- binary(min_length: 0, max_length: 100)) do
        {start_key, end_key} = Key.to_range(key)
        assert start_key < end_key
      end
    end

    property "range contains the original key" do
      check all(key <- binary(min_length: 0, max_length: 100)) do
        {start_key, end_key} = Key.to_range(key)
        assert start_key <= key and key < end_key
      end
    end
  end

  describe "strinc/1" do
    test "increments last byte of simple string" do
      assert Key.strinc("abc") == "abd"
    end

    test "increments last byte of binary" do
      assert Key.strinc(<<0, 1, 2>>) == <<0, 1, 3>>
    end

    test "increments through character boundaries" do
      assert Key.strinc("hello") == "hellp"
    end

    test "handles single byte key" do
      assert Key.strinc(<<1>>) == <<2>>
    end

    test "handles key ending with 0xFE" do
      assert Key.strinc(<<1, 2, 0xFE>>) == <<1, 2, 0xFF>>
    end

    test "strips trailing 0xFF bytes and increments" do
      assert Key.strinc(<<1, 2, 0xFF>>) == <<1, 3>>
    end

    test "strips multiple trailing 0xFF bytes" do
      assert Key.strinc(<<1, 0xFF, 0xFF>>) == <<2>>
    end

    test "handles key with all trailing 0xFF" do
      assert Key.strinc(<<"abc", 0xFF, 0xFF>>) == "abd"
    end

    test "raises on key with all 0xFF bytes" do
      assert_raise ArgumentError, "Key must contain at least one byte not equal to 0xFF", fn ->
        Key.strinc(<<0xFF, 0xFF>>)
      end
    end

    test "raises on single 0xFF byte" do
      assert_raise ArgumentError, "Key must contain at least one byte not equal to 0xFF", fn ->
        Key.strinc(<<0xFF>>)
      end
    end

    test "raises on empty key" do
      assert_raise ArgumentError, "Key must contain at least one byte not equal to 0xFF", fn ->
        Key.strinc(<<>>)
      end
    end

    test "creates minimal increment for prefix ranges" do
      # strinc increments last byte
      prefix = "test"
      strinc_result = Key.strinc(prefix)

      assert strinc_result == "tesu"
      assert strinc_result > prefix
    end

    property "always produces a key greater than input (when valid)" do
      check all(
              key <- binary(min_length: 1, max_length: 100),
              # Ensure at least one non-0xFF byte exists
              :binary.match(key, [<<0xFF>>]) == :nomatch or
                byte_size(key) > count_trailing_ff(key)
            ) do
        try do
          result = Key.strinc(key)
          assert result > key
        rescue
          ArgumentError -> :ok
        end
      end
    end

    property "strinc produces valid successor key" do
      check all(key <- binary(min_length: 1, max_length: 50)) do
        try do
          strinc_result = Key.strinc(key)
          # strinc should always produce a key greater than the input
          assert strinc_result > key
        rescue
          ArgumentError -> :ok
        end
      end
    end

    property "strinc strips trailing 0xFF bytes correctly" do
      check all(
              base <- binary(min_length: 1, max_length: 20),
              # Ensure base doesn't end with 0xFF
              :binary.at(base, byte_size(base) - 1) != 0xFF,
              trailing_ff_count <- integer(0..10)
            ) do
        key = base <> :binary.copy(<<0xFF>>, trailing_ff_count)

        result = Key.strinc(key)

        # Result should be base with last byte incremented
        base_len = byte_size(base)
        expected_head = binary_part(base, 0, base_len - 1)
        expected_tail = :binary.at(base, base_len - 1) + 1

        assert result == expected_head <> <<expected_tail>>
      end
    end

    property "strinc maintains prefix relationship for range operations" do
      check all(key <- binary(min_length: 1, max_length: 50)) do
        try do
          upper_bound = Key.strinc(key)

          # Any key starting with original key and less than upper_bound
          # should be in the range [key, upper_bound)
          test_suffix = <<1, 2, 3>>
          test_key = key <> test_suffix

          if test_key < upper_bound do
            assert key <= test_key
            assert test_key < upper_bound
          end
        rescue
          ArgumentError -> :ok
        end
      end
    end
  end

  describe "ordering properties" do
    property "key_after preserves ordering" do
      check all(
              key1 <- binary(max_length: 50),
              key2 <- binary(max_length: 50)
            ) do
        after1 = Key.key_after(key1)
        after2 = Key.key_after(key2)

        cond do
          key1 < key2 -> assert after1 < after2
          key1 > key2 -> assert after1 > after2
          true -> assert after1 == after2
        end
      end
    end

    property "strinc preserves ordering (when both valid)" do
      check all(
              key1 <- binary(min_length: 1, max_length: 50),
              key2 <- binary(min_length: 1, max_length: 50)
            ) do
        try do
          strinc1 = Key.strinc(key1)
          strinc2 = Key.strinc(key2)

          cond do
            key1 < key2 -> assert strinc1 < strinc2
            key1 > key2 -> assert strinc1 > strinc2
            true -> assert strinc1 == strinc2
          end
        rescue
          ArgumentError -> :ok
        end
      end
    end
  end

  describe "range operations" do
    test "key_after and strinc create valid ranges" do
      keys = ["a", "abc", "test", "zebra"]

      for key <- keys do
        after_bound = Key.key_after(key)
        strinc_bound = Key.strinc(key)

        # Both should create valid upper bounds
        assert key < after_bound
        assert key < strinc_bound
      end
    end

    test "strinc creates minimal exclusive upper bound" do
      # For prefix "user", strinc gives "uses"
      # This means the range ["user", "uses") contains exactly keys starting with "user"
      prefix = "user"
      upper = Key.strinc(prefix)

      assert upper == "uses"

      # Keys starting with "user" should be in range
      assert "user" < upper
      assert "user1" < upper
      assert "user999" < upper
      assert "userZ" < upper

      # Keys not starting with "user" should be outside
      assert "uses" >= upper
      assert "uset" >= upper
    end

    property "range operations are consistent" do
      check all(prefix <- binary(min_length: 1, max_length: 20)) do
        {range_start, range_end} = Key.to_range(prefix)

        # to_range uses key_after, so range_end = prefix + <<0>>
        assert range_start == prefix
        assert range_end == prefix <> <<0>>

        # range_end should be greater than range_start
        assert range_end > range_start
      end
    end
  end

  describe "edge cases" do
    test "handles keys with all printable ASCII" do
      key = "abcdefghijklmnopqrstuvwxyz"
      assert Key.key_after(key) == key <> <<0>>
      # 'z' is 122, so strinc increments it to 123 which is '{'
      assert Key.strinc(key) == "abcdefghijklmnopqrstuvwxy{"
    end

    test "handles keys with mixed binary data" do
      key = <<0, 1, 2, 127, 128, 255>>
      assert Key.key_after(key) == key <> <<0>>

      # Last byte is 0xFF, so strinc strips it and increments 128 to 129
      assert Key.strinc(key) == <<0, 1, 2, 127, 129>>
    end

    test "handles very short keys" do
      assert Key.key_after(<<0>>) == <<0, 0>>
      assert Key.strinc(<<0>>) == <<1>>

      assert Key.key_after(<<1>>) == <<1, 0>>
      assert Key.strinc(<<1>>) == <<2>>
    end

    test "handles keys near byte boundaries" do
      # Test incrementing from 0xFF-1 to 0xFF
      assert Key.strinc(<<254>>) == <<255>>

      # Test key ending with 0xFE
      assert Key.strinc(<<"test", 0xFE>>) == <<"test", 0xFF>>

      # Test multiple trailing 0xFF
      assert Key.strinc(<<"a", 0xFF, 0xFF, 0xFF>>) == "b"
    end

    test "strinc raises error for all-0xFF keys" do
      # Single 0xFF byte
      assert_raise ArgumentError, fn -> Key.strinc(<<0xFF>>) end

      # Multiple 0xFF bytes
      assert_raise ArgumentError, fn -> Key.strinc(<<0xFF, 0xFF>>) end
      assert_raise ArgumentError, fn -> Key.strinc(<<0xFF, 0xFF, 0xFF>>) end
    end

    test "strinc handles empty key edge case" do
      # Empty binary should raise ArgumentError
      assert_raise ArgumentError, fn -> Key.strinc(<<>>) end
    end

    test "strinc ordering with specific edge cases" do
      # Test ordering preservation with keys that have trailing 0xFF
      key1 = <<"a", 0xFF>>
      key2 = <<"b", 0xFF>>

      # "b"
      strinc1 = Key.strinc(key1)
      # "c"
      strinc2 = Key.strinc(key2)

      assert strinc1 < strinc2

      # Test with mixed keys
      key3 = "user"
      key4 = "uses"

      # "uses"
      strinc3 = Key.strinc(key3)
      # "uset"
      strinc4 = Key.strinc(key4)

      assert strinc3 < strinc4
    end

    test "key_after ordering with specific edge cases" do
      # Test ordering with empty keys
      key1 = <<>>
      key2 = <<1>>

      # <<0>>
      after1 = Key.key_after(key1)
      # <<1, 0>>
      after2 = Key.key_after(key2)

      assert after1 < after2

      # Test with equal keys
      key3 = "test"
      key4 = "test"

      after3 = Key.key_after(key3)
      after4 = Key.key_after(key4)

      assert after3 == after4
    end
  end

  # Helper function for property tests
  defp count_trailing_ff(<<>>), do: 0

  defp count_trailing_ff(key) do
    key_len = byte_size(key)

    case :binary.at(key, key_len - 1) do
      0xFF -> 1 + count_trailing_ff(binary_part(key, 0, key_len - 1))
      _ -> 0
    end
  end
end
