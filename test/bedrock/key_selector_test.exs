defmodule Bedrock.KeySelectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.KeySelector

  describe "construction functions" do
    test "first_greater_or_equal/1 creates correct KeySelector" do
      selector = KeySelector.first_greater_or_equal("user:")

      assert %KeySelector{key: "user:", or_equal: true, offset: 0} = selector
    end

    test "first_greater_than/1 creates correct KeySelector" do
      selector = KeySelector.first_greater_than("data")

      assert %KeySelector{key: "data", or_equal: true, offset: 1} = selector
    end

    test "last_less_or_equal/1 creates correct KeySelector" do
      selector = KeySelector.last_less_or_equal("items")

      assert %KeySelector{key: "items", or_equal: true, offset: -1} = selector
    end

    test "last_less_than/1 creates correct KeySelector" do
      selector = KeySelector.last_less_than("zone")

      assert %KeySelector{key: "zone", or_equal: false, offset: 0} = selector
    end

    test "construction functions only accept binaries" do
      assert_raise FunctionClauseError, fn ->
        KeySelector.first_greater_or_equal(:not_binary)
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.first_greater_than(123)
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.last_less_or_equal(nil)
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.last_less_than(%{})
      end
    end
  end

  describe "offset manipulation" do
    test "add/2 increases offset" do
      selector = KeySelector.first_greater_or_equal("base")

      result = KeySelector.add(selector, 5)
      assert %KeySelector{key: "base", or_equal: true, offset: 5} = result
    end

    test "add/2 with negative number decreases offset" do
      selector = KeySelector.first_greater_than("base")

      result = KeySelector.add(selector, -3)
      assert %KeySelector{key: "base", or_equal: true, offset: -2} = result
    end

    test "add/2 accumulates offsets" do
      selector =
        "base"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(3)
        |> KeySelector.add(2)
        |> KeySelector.add(-1)

      assert %KeySelector{key: "base", or_equal: true, offset: 4} = selector
    end

    test "subtract/2 decreases offset" do
      selector = KeySelector.first_greater_or_equal("base")

      result = KeySelector.subtract(selector, 3)
      assert %KeySelector{key: "base", or_equal: true, offset: -3} = result
    end

    test "subtract/2 with negative number increases offset" do
      selector = KeySelector.last_less_than("base")

      result = KeySelector.subtract(selector, -4)
      assert %KeySelector{key: "base", or_equal: false, offset: 4} = result
    end

    test "add/2 and subtract/2 only accept integers" do
      selector = KeySelector.first_greater_or_equal("base")

      assert_raise FunctionClauseError, fn ->
        KeySelector.add(selector, "not_integer")
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.subtract(selector, 3.14)
      end
    end
  end

  describe "offset predicates" do
    test "zero_offset?/1 correctly identifies zero offsets" do
      assert "a" |> KeySelector.first_greater_or_equal() |> KeySelector.zero_offset?()
      assert "z" |> KeySelector.last_less_than() |> KeySelector.zero_offset?()

      refute "b" |> KeySelector.first_greater_than() |> KeySelector.zero_offset?()
      refute "y" |> KeySelector.last_less_or_equal() |> KeySelector.zero_offset?()
      refute "c" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1) |> KeySelector.zero_offset?()
    end

    test "positive_offset?/1 correctly identifies positive offsets" do
      assert "a" |> KeySelector.first_greater_than() |> KeySelector.positive_offset?()
      assert "b" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1) |> KeySelector.positive_offset?()
      assert "c" |> KeySelector.last_less_than() |> KeySelector.add(5) |> KeySelector.positive_offset?()

      refute "d" |> KeySelector.first_greater_or_equal() |> KeySelector.positive_offset?()
      refute "e" |> KeySelector.last_less_than() |> KeySelector.positive_offset?()
      refute "f" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-1) |> KeySelector.positive_offset?()
    end

    test "negative_offset?/1 correctly identifies negative offsets" do
      assert "a" |> KeySelector.last_less_or_equal() |> KeySelector.negative_offset?()
      assert "b" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-1) |> KeySelector.negative_offset?()
      assert "c" |> KeySelector.first_greater_than() |> KeySelector.subtract(5) |> KeySelector.negative_offset?()

      refute "d" |> KeySelector.first_greater_or_equal() |> KeySelector.negative_offset?()
      refute "e" |> KeySelector.first_greater_than() |> KeySelector.negative_offset?()
      refute "f" |> KeySelector.last_less_than() |> KeySelector.negative_offset?()
    end
  end

  describe "string representation" do
    test "to_string/1 for basic selectors" do
      assert "user:" |> KeySelector.first_greater_or_equal() |> KeySelector.to_string() ==
               ~s{first_greater_or_equal("user:")}

      assert "data" |> KeySelector.first_greater_than() |> KeySelector.to_string() ==
               ~s{first_greater_than("data")}

      assert "items" |> KeySelector.last_less_or_equal() |> KeySelector.to_string() ==
               ~s{last_less_or_equal("items")}

      assert "zone" |> KeySelector.last_less_than() |> KeySelector.to_string() ==
               ~s{last_less_than("zone")}
    end

    test "to_string/1 for selectors with positive offsets" do
      selector = "base" |> KeySelector.first_greater_or_equal() |> KeySelector.add(3)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("base") + 3}

      selector = "data" |> KeySelector.first_greater_than() |> KeySelector.add(5)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("data") + 6}

      selector = "end" |> KeySelector.last_less_than() |> KeySelector.add(2)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("end") + 1}
    end

    test "to_string/1 for selectors with negative offsets" do
      selector = "base" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-2)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("base") - 2}

      selector = "data" |> KeySelector.first_greater_than() |> KeySelector.add(-3)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("data") - 2}

      selector = "end" |> KeySelector.last_less_than() |> KeySelector.add(-1)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("end") - 2}
    end

    test "to_string/1 handles complex offsets correctly" do
      # last_less_or_equal is internally first_greater_or_equal with offset -1
      selector = "test" |> KeySelector.last_less_or_equal() |> KeySelector.add(3)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("test") + 2}

      selector = "test" |> KeySelector.last_less_or_equal() |> KeySelector.subtract(2)
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("test") - 3}
    end

    test "String.Chars protocol implementation" do
      selector = KeySelector.first_greater_or_equal("test")
      assert to_string(selector) == ~s{first_greater_or_equal("test")}

      selector = "data" |> KeySelector.first_greater_than() |> KeySelector.add(2)
      assert to_string(selector) == ~s{first_greater_or_equal("data") + 3}
    end
  end

  describe "complex scenarios" do
    test "chaining operations produces expected results" do
      selector =
        "start"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(10)
        |> KeySelector.subtract(3)
        |> KeySelector.add(-2)

      assert %KeySelector{key: "start", or_equal: true, offset: 5} = selector
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("start") + 5}
    end

    test "edge case with empty key" do
      selector = KeySelector.first_greater_or_equal("")

      assert %KeySelector{key: "", or_equal: true, offset: 0} = selector
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("")}
    end

    test "edge case with binary containing special characters" do
      key = <<0xFF, 0x00, "special", 0x01>>
      selector = KeySelector.first_greater_than(key)

      assert %KeySelector{key: ^key, or_equal: true, offset: 1} = selector
      expected_string = ~s{first_greater_than(<<255, 0, 115, 112, 101, 99, 105, 97, 108, 1>>)}
      assert KeySelector.to_string(selector) == expected_string
    end

    test "large offset values" do
      selector = "base" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)

      assert %KeySelector{key: "base", or_equal: true, offset: 1000} = selector
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("base") + 1000}
    end

    test "very negative offset values" do
      selector = "base" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-500)

      assert %KeySelector{key: "base", or_equal: true, offset: -500} = selector
      assert KeySelector.to_string(selector) == ~s{first_greater_or_equal("base") - 500}
    end
  end

  describe "type checking and validation" do
    test "KeySelector struct has correct type specification" do
      selector = KeySelector.first_greater_or_equal("test")

      assert %KeySelector{} = selector
      assert is_binary(selector.key)
      assert is_boolean(selector.or_equal)
      assert is_integer(selector.offset)
    end

    test "all construction functions return KeySelector struct" do
      constructors = [
        &KeySelector.first_greater_or_equal/1,
        &KeySelector.first_greater_than/1,
        &KeySelector.last_less_or_equal/1,
        &KeySelector.last_less_than/1
      ]

      for constructor <- constructors do
        result = constructor.("test")
        assert %KeySelector{} = result
      end
    end
  end

  describe "equivalence and comparison scenarios" do
    test "different construction methods can produce equivalent KeySelectors" do
      # These should be functionally equivalent
      selector1 = KeySelector.first_greater_than("key")
      selector2 = "key" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1)

      # They have different internal representations but same logical meaning
      assert selector1.key == selector2.key
      assert selector1.offset == selector2.offset
      assert selector1.or_equal == selector2.or_equal
    end

    test "offset manipulation creates predictable patterns" do
      base = KeySelector.first_greater_or_equal("test")

      # Forward and back should cancel out
      result = base |> KeySelector.add(5) |> KeySelector.subtract(5)
      assert result == base

      # Multiple small additions should equal one large addition
      incremental = base |> KeySelector.add(2) |> KeySelector.add(3)
      bulk = KeySelector.add(base, 5)
      assert incremental.offset == bulk.offset
    end
  end
end
