defmodule Bedrock.TypeCoercionTest do
  use ExUnit.Case, async: true

  alias Bedrock.Keyspace
  alias Bedrock.ToKeyRange
  alias Bedrock.ToKeyspace

  describe "ToKeyRange for Tuple" do
    test "converts valid binary tuple to key range" do
      start_key = <<"a">>
      end_key = <<"z">>

      assert ToKeyRange.to_key_range({start_key, end_key}) == {start_key, end_key}
    end

    test "raises for invalid tuple (start >= end)" do
      invalid_tuple = {<<"z">>, <<"a">>}

      assert_raise ArgumentError, ~r/ToKeyRange expects a 2-tuple/, fn ->
        ToKeyRange.to_key_range(invalid_tuple)
      end
    end

    test "raises for non-binary elements" do
      invalid_tuple = {"string", "other"}

      assert_raise ArgumentError, ~r/ToKeyRange expects a 2-tuple/, fn ->
        ToKeyRange.to_key_range(invalid_tuple)
      end
    end

    test "raises for single element tuple" do
      assert_raise ArgumentError, ~r/ToKeyRange expects a 2-tuple/, fn ->
        ToKeyRange.to_key_range({<<"key">>})
      end
    end

    test "raises for 3-tuple" do
      assert_raise ArgumentError, ~r/ToKeyRange expects a 2-tuple/, fn ->
        ToKeyRange.to_key_range({<<"a">>, <<"b">>, <<"c">>})
      end
    end
  end

  describe "ToKeyRange for BitString" do
    test "converts binary prefix to key range" do
      prefix = <<"test">>
      {start_key, end_key} = ToKeyRange.to_key_range(prefix)

      assert start_key == prefix
      # String increment
      assert end_key == <<"tesu">>
    end

    test "handles empty binary" do
      # Empty binary actually raises since strinc can't increment it
      # This is expected behavior - covered by Keyspace tests instead
      assert_raise ArgumentError, fn ->
        ToKeyRange.to_key_range(<<>>)
      end
    end

    test "handles various binary prefixes" do
      test_cases = [
        {<<"users">>, <<"users">>, <<"usert">>},
        {<<1, 2, 3>>, <<1, 2, 3>>, <<1, 2, 4>>},
        {<<"a">>, <<"a">>, <<"b">>}
      ]

      for {input, expected_start, expected_end} <- test_cases do
        {start_key, end_key} = ToKeyRange.to_key_range(input)
        assert start_key == expected_start
        assert end_key == expected_end
      end
    end
  end

  describe "ToKeyspace for Keyspace" do
    test "returns keyspace unchanged (identity)" do
      keyspace = Keyspace.new(<<"prefix">>)

      assert ToKeyspace.to_keyspace(keyspace) == keyspace
    end

    test "preserves all keyspace properties" do
      keyspace =
        Keyspace.new(<<"test">>,
          key_encoding: Bedrock.Encoding.Tuple,
          value_encoding: Bedrock.Encoding.Tuple
        )

      result = ToKeyspace.to_keyspace(keyspace)

      assert result == keyspace
      assert result.prefix == <<"test">>
      assert result.key_encoding == Bedrock.Encoding.Tuple
      assert result.value_encoding == Bedrock.Encoding.Tuple
    end
  end

  describe "ToKeyspace for BitString" do
    test "converts binary to keyspace" do
      prefix = <<"my_prefix">>

      result = ToKeyspace.to_keyspace(prefix)

      assert %Keyspace{} = result
      assert result.prefix == prefix
    end

    test "handles empty binary" do
      result = ToKeyspace.to_keyspace(<<>>)

      assert %Keyspace{} = result
      assert result.prefix == <<>>
    end

    test "creates keyspace with default options" do
      result = ToKeyspace.to_keyspace(<<"test">>)

      assert result.key_encoding == nil
      assert result.value_encoding == nil
    end
  end

  describe "protocol integration" do
    test "ToKeyRange works with Keyspace struct" do
      keyspace = Keyspace.new(<<"users">>)

      {start_key, end_key} = ToKeyRange.to_key_range(keyspace)

      assert start_key == <<"users">>
      assert end_key == <<"usert">>
    end

    test "can chain coercions" do
      # Convert binary -> keyspace -> range
      binary = <<"data">>
      keyspace = ToKeyspace.to_keyspace(binary)
      {start_key, end_key} = ToKeyRange.to_key_range(keyspace)

      assert start_key == <<"data">>
      assert end_key == <<"datb">>
    end

    test "tuple range works directly" do
      # Need start < end for tuple
      range = {<<"a">>, <<"z">>}

      # Tuple can be converted to range
      assert ToKeyRange.to_key_range(range) == range
    end
  end
end
