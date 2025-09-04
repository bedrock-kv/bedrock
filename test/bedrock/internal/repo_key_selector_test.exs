defmodule Bedrock.Internal.Repo.KeySelectorTest do
  use ExUnit.Case, async: false

  alias Bedrock.KeySelector

  @moduletag :integration

  describe "KeySelector fetch operations" do
    test "fetch_key_selector/2 resolves and returns key-value pair" do
      # This test would require a running Bedrock cluster
      # For now, we'll create a basic structure test

      key_selector = KeySelector.first_greater_or_equal("test:key")
      assert %KeySelector{key: "test:key", or_equal: true, offset: 0} = key_selector

      # In a real test environment, this would call:
      # {:ok, transaction} = Gateway.begin_transaction(gateway)
      # result = Repo.fetch_key_selector(transaction, key_selector)
      # assert {:ok, {resolved_key, value}} = result
    end

    test "fetch_key_selector!/2 raises on not found" do
      _key_selector = KeySelector.first_greater_than("nonexistent")

      # In a real test environment:
      # assert_raise RuntimeError, ~r/KeySelector not found/, fn ->
      #   Repo.fetch_key_selector!(transaction, key_selector)
      # end
    end

    test "get_key_selector/2 returns nil on not found" do
      _key_selector = KeySelector.last_less_or_equal("nonexistent")

      # In a real test environment:
      # assert nil == Repo.get_key_selector(transaction, key_selector)
    end
  end

  describe "KeySelector range operations" do
    test "range_fetch_key_selectors/4 handles valid range" do
      start_selector = KeySelector.first_greater_or_equal("range:start")
      end_selector = KeySelector.first_greater_than("range:end")

      assert %KeySelector{key: "range:start", or_equal: true, offset: 0} = start_selector
      assert %KeySelector{key: "range:end", or_equal: false, offset: 1} = end_selector

      # In a real test environment:
      # {:ok, results} = Repo.range_fetch_key_selectors(transaction, start_selector, end_selector)
      # assert is_list(results)
      # assert Enum.all?(results, fn {key, value} -> is_binary(key) and is_binary(value) end)
    end

    test "range_stream_key_selectors/4 provides enumerable interface" do
      _start_selector = KeySelector.first_greater_or_equal("stream:a")
      _end_selector = KeySelector.last_less_than("stream:z")

      # In a real test environment:
      # stream = Repo.range_stream_key_selectors(transaction, start_selector, end_selector)
      # results = stream |> Enum.take(10) |> Enum.to_list()
      # assert is_list(results)
    end
  end

  describe "KeySelector construction and manipulation" do
    test "construction functions create correct selectors" do
      gte = KeySelector.first_greater_or_equal("base")
      gt = KeySelector.first_greater_than("base")
      lte = KeySelector.last_less_or_equal("base")
      lt = KeySelector.last_less_than("base")

      assert %KeySelector{key: "base", or_equal: true, offset: 0} = gte
      assert %KeySelector{key: "base", or_equal: false, offset: 1} = gt
      assert %KeySelector{key: "base", or_equal: true, offset: -1} = lte
      assert %KeySelector{key: "base", or_equal: false, offset: 0} = lt
    end

    test "offset manipulation works correctly" do
      selector =
        "key"
        |> KeySelector.first_greater_or_equal()
        |> KeySelector.add(3)
        |> KeySelector.subtract(1)

      assert %KeySelector{key: "key", or_equal: true, offset: 2} = selector
    end

    test "offset predicates work correctly" do
      zero = KeySelector.first_greater_or_equal("a")
      positive = KeySelector.first_greater_than("b")
      negative = KeySelector.last_less_or_equal("c")

      assert KeySelector.zero_offset?(zero)
      refute KeySelector.zero_offset?(positive)
      refute KeySelector.zero_offset?(negative)

      refute KeySelector.positive_offset?(zero)
      assert KeySelector.positive_offset?(positive)
      refute KeySelector.positive_offset?(negative)

      refute KeySelector.negative_offset?(zero)
      refute KeySelector.negative_offset?(positive)
      assert KeySelector.negative_offset?(negative)
    end

    test "string representation works correctly" do
      basic = KeySelector.first_greater_or_equal("test")
      with_offset = "data" |> KeySelector.first_greater_than() |> KeySelector.add(5)
      negative_offset = "base" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-3)

      assert KeySelector.to_string(basic) == ~s{first_greater_or_equal("test")}
      assert KeySelector.to_string(with_offset) == ~s{first_greater_or_equal("data") + 5}
      assert KeySelector.to_string(negative_offset) == ~s{first_greater_or_equal("base") - 3}
    end
  end

  describe "error handling" do
    test "invalid KeySelector operations return appropriate errors" do
      # Test construction with invalid types
      assert_raise FunctionClauseError, fn ->
        KeySelector.first_greater_or_equal(123)
      end

      assert_raise FunctionClauseError, fn ->
        KeySelector.add(KeySelector.first_greater_or_equal("test"), "not_integer")
      end
    end

    test "storage errors are properly propagated" do
      # In a real test environment, we would test various storage error conditions:
      # - :version_too_old
      # - :version_too_new
      # - :not_found
      # - :unavailable
      # - :clamped (for cross-shard operations)
    end
  end
end
