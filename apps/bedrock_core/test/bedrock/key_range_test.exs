defmodule Bedrock.KeyRangeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.KeyRange

  describe "from_prefix/1" do
    test "creates range from simple prefix" do
      assert KeyRange.from_prefix("user") == {"user", "uses"}
    end

    test "creates range from prefix with slash" do
      assert KeyRange.from_prefix("prefix/") == {"prefix/", "prefix0"}
    end

    test "creates range from binary prefix" do
      assert KeyRange.from_prefix(<<1, 2, 3>>) == {<<1, 2, 3>>, <<1, 2, 4>>}
    end

    test "creates range from single character prefix" do
      assert KeyRange.from_prefix("a") == {"a", "b"}
    end

    test "handles prefix with trailing 0xFF" do
      assert KeyRange.from_prefix(<<1, 0xFF>>) == {<<1, 0xFF>>, <<2>>}
    end

    test "raises on prefix with all 0xFF bytes" do
      assert_raise ArgumentError, "Key must contain at least one byte not equal to 0xFF", fn ->
        KeyRange.from_prefix(<<0xFF, 0xFF>>)
      end
    end

    test "raises on empty prefix" do
      assert_raise ArgumentError, "Key must contain at least one byte not equal to 0xFF", fn ->
        KeyRange.from_prefix(<<>>)
      end
    end

    property "start key equals the prefix" do
      check all(prefix <- binary(min_length: 1, max_length: 50)) do
        try do
          {start_key, _end_key} = KeyRange.from_prefix(prefix)
          assert start_key == prefix
        rescue
          ArgumentError -> :ok
        end
      end
    end

    property "end key is always greater than start key" do
      check all(prefix <- binary(min_length: 1, max_length: 50)) do
        try do
          {start_key, end_key} = KeyRange.from_prefix(prefix)
          assert end_key > start_key
        rescue
          ArgumentError -> :ok
        end
      end
    end

    property "prefix is always within its own range" do
      check all(prefix <- binary(min_length: 1, max_length: 50)) do
        try do
          {start_key, end_key} = KeyRange.from_prefix(prefix)
          # prefix >= start_key and prefix < end_key
          assert prefix >= start_key and prefix < end_key
        rescue
          ArgumentError -> :ok
        end
      end
    end
  end

  describe "overlap?/2" do
    test "detects overlapping ranges" do
      assert KeyRange.overlap?({"a", "c"}, {"b", "d"}) == true
    end

    test "detects non-overlapping ranges" do
      assert KeyRange.overlap?({"a", "b"}, {"c", "d"}) == false
    end

    test "detects ranges that touch but don't overlap" do
      # {"a", "b"} and {"b", "c"} touch at "b" but don't overlap
      # because end is exclusive
      assert KeyRange.overlap?({"a", "b"}, {"b", "c"}) == false
    end

    test "detects range contained within another" do
      assert KeyRange.overlap?({"a", "z"}, {"m", "n"}) == true
    end

    test "detects range containing another" do
      assert KeyRange.overlap?({"m", "n"}, {"a", "z"}) == true
    end

    test "detects identical ranges" do
      assert KeyRange.overlap?({"a", "z"}, {"a", "z"}) == true
    end

    test "detects overlapping binary ranges" do
      assert KeyRange.overlap?({<<0, 0>>, <<0, 5>>}, {<<0, 3>>, <<0, 7>>}) == true
    end

    test "detects non-overlapping binary ranges" do
      assert KeyRange.overlap?({<<0, 0>>, <<0, 2>>}, {<<0, 3>>, <<0, 5>>}) == false
    end

    property "overlap is symmetric" do
      check all(
              range1 <- key_range_generator(),
              range2 <- key_range_generator()
            ) do
        assert KeyRange.overlap?(range1, range2) == KeyRange.overlap?(range2, range1)
      end
    end

    property "range always overlaps with itself" do
      check all(range <- key_range_generator()) do
        assert KeyRange.overlap?(range, range) == true
      end
    end

    test "non-overlapping ranges don't overlap" do
      # Manually test non-overlapping ranges
      refute KeyRange.overlap?({<<1>>, <<5>>}, {<<10>>, <<15>>})
      refute KeyRange.overlap?({<<1>>, <<5>>}, {<<5>>, <<10>>})
      refute KeyRange.overlap?({"a", "m"}, {"n", "z"})
    end
  end

  describe "contains?/2" do
    test "returns true for key within range" do
      assert KeyRange.contains?({"a", "c"}, "b") == true
    end

    test "returns true for key equal to start" do
      assert KeyRange.contains?({"a", "c"}, "a") == true
    end

    test "returns false for key equal to end (exclusive)" do
      assert KeyRange.contains?({"a", "c"}, "c") == false
    end

    test "returns false for key before range" do
      assert KeyRange.contains?({"b", "d"}, "a") == false
    end

    test "returns false for key after range" do
      assert KeyRange.contains?({"a", "c"}, "z") == false
    end

    test "handles binary keys" do
      range = {<<0, 0>>, <<0, 10>>}

      assert KeyRange.contains?(range, <<0, 0>>) == true
      assert KeyRange.contains?(range, <<0, 5>>) == true
      assert KeyRange.contains?(range, <<0, 9>>) == true
      assert KeyRange.contains?(range, <<0, 10>>) == false
      assert KeyRange.contains?(range, <<0, 11>>) == false
    end

    test "handles empty range" do
      # Range where start == end contains no keys
      assert KeyRange.contains?({"a", "a"}, "a") == false
    end

    property "start key is always contained" do
      check all({start_key, end_key} <- key_range_generator()) do
        if start_key < end_key do
          assert KeyRange.contains?({start_key, end_key}, start_key)
        end
      end
    end

    property "end key is never contained (exclusive)" do
      check all({start_key, end_key} <- key_range_generator()) do
        refute KeyRange.contains?({start_key, end_key}, end_key)
      end
    end

    test "keys before start are not contained" do
      range = {<<5>>, <<10>>}
      refute KeyRange.contains?(range, <<0>>)
      refute KeyRange.contains?(range, <<4>>)
    end

    test "keys at or after end are not contained" do
      range = {<<5>>, <<10>>}
      refute KeyRange.contains?(range, <<10>>)
      refute KeyRange.contains?(range, <<11>>)
      refute KeyRange.contains?(range, <<255>>)
    end
  end

  describe "integration with from_prefix" do
    test "range from prefix contains keys with that prefix" do
      range = KeyRange.from_prefix("user")

      assert KeyRange.contains?(range, "user")
      assert KeyRange.contains?(range, "user1")
      assert KeyRange.contains?(range, "user123")
      assert KeyRange.contains?(range, "userZZZ")

      refute KeyRange.contains?(range, "uses")
      refute KeyRange.contains?(range, "usdr")
      refute KeyRange.contains?(range, "use")
    end

    test "range from prefix works with binary prefixes" do
      range = KeyRange.from_prefix(<<1, 2>>)

      assert KeyRange.contains?(range, <<1, 2>>)
      assert KeyRange.contains?(range, <<1, 2, 0>>)
      assert KeyRange.contains?(range, <<1, 2, 255>>)

      refute KeyRange.contains?(range, <<1, 3>>)
      refute KeyRange.contains?(range, <<1, 1>>)
      refute KeyRange.contains?(range, <<1>>)
    end

    property "range from prefix contains all keys starting with prefix" do
      check all(
              prefix <- binary(min_length: 1, max_length: 20),
              suffix <- binary(min_length: 0, max_length: 20)
            ) do
        try do
          range = KeyRange.from_prefix(prefix)
          key = prefix <> suffix

          # Key starting with prefix should be in range (until we reach strinc)
          {_start, range_end} = range

          if key < range_end do
            assert KeyRange.contains?(range, key)
          end
        rescue
          ArgumentError -> :ok
        end
      end
    end
  end

  describe "range overlap patterns" do
    test "completely disjoint ranges don't overlap" do
      # [0..5) and [10..15)
      refute KeyRange.overlap?({<<0>>, <<5>>}, {<<10>>, <<15>>})
    end

    test "adjacent ranges don't overlap" do
      # [0..5) and [5..10)
      refute KeyRange.overlap?({<<0>>, <<5>>}, {<<5>>, <<10>>})
    end

    test "partially overlapping ranges overlap" do
      # [0..10) and [5..15)
      assert KeyRange.overlap?({<<0>>, <<10>>}, {<<5>>, <<15>>})
    end

    test "nested ranges overlap" do
      # [0..20) contains [5..15)
      assert KeyRange.overlap?({<<0>>, <<20>>}, {<<5>>, <<15>>})
    end

    test "single-point range overlaps containing range" do
      # [5..6) overlaps with [0..10)
      assert KeyRange.overlap?({<<5>>, <<6>>}, {<<0>>, <<10>>})
    end
  end

  describe "edge cases" do
    test "handles ranges with maximum byte values" do
      range = {<<0xFF>>, <<0xFF, 0>>}

      assert KeyRange.contains?(range, <<0xFF>>)
      refute KeyRange.contains?(range, <<0xFF, 0>>)
    end

    test "handles very large ranges" do
      range = {<<0>>, <<0xFF>>}

      assert KeyRange.contains?(range, <<0>>)
      assert KeyRange.contains?(range, <<127>>)
      assert KeyRange.contains?(range, <<254>>)
      refute KeyRange.contains?(range, <<0xFF>>)
    end

    test "handles single-key range" do
      # Range that contains exactly one key
      range = {<<5>>, <<6>>}

      assert KeyRange.contains?(range, <<5>>)
      refute KeyRange.contains?(range, <<4>>)
      refute KeyRange.contains?(range, <<6>>)
    end

    test "handles empty range behavior" do
      # When start == end, range is empty
      empty_range = {<<5>>, <<5>>}

      refute KeyRange.contains?(empty_range, <<5>>)
      refute KeyRange.contains?(empty_range, <<4>>)
      refute KeyRange.contains?(empty_range, <<6>>)

      # Empty range doesn't overlap with anything (even itself technically)
      refute KeyRange.overlap?(empty_range, empty_range)
    end
  end

  # Helper generators for property-based tests
  defp key_range_generator do
    gen all(
          start <- binary(min_length: 0, max_length: 20),
          end_suffix <- binary(min_length: 0, max_length: 10)
        ) do
      end_key = start <> end_suffix <> <<1>>
      {start, end_key}
    end
  end
end
