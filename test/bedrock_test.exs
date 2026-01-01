defmodule BedrockTest do
  use ExUnit.Case, async: true

  describe "key_range/2" do
    test "creates range with :end atom when min_key is less than end of keyspace" do
      assert Bedrock.key_range("a", :end) == {"a", <<0xFF, 0xFF>>}
      assert Bedrock.key_range(<<0>>, :end) == {<<0>>, <<0xFF, 0xFF>>}
      assert Bedrock.key_range(<<0xFF, 0xFE>>, :end) == {<<0xFF, 0xFE>>, <<0xFF, 0xFF>>}
    end

    test "creates range with explicit max_key when min_key is less than max_key" do
      assert Bedrock.key_range("a", "z") == {"a", "z"}
      assert Bedrock.key_range(<<0>>, <<10>>) == {<<0>>, <<10>>}
    end

    test "raises error when min_key equals max_key_exclusive" do
      assert_raise ArgumentError, "min_key must be less than max_key_exclusive", fn ->
        Bedrock.key_range("a", "a")
      end
    end

    test "raises error when min_key is greater than max_key_exclusive" do
      assert_raise ArgumentError, "min_key must be less than max_key_exclusive", fn ->
        Bedrock.key_range("z", "a")
      end
    end

    test "raises error when min_key equals or exceeds end of keyspace with :end" do
      assert_raise ArgumentError, "min_key must be less than max_key_exclusive", fn ->
        Bedrock.key_range(<<0xFF, 0xFF>>, :end)
      end
    end
  end
end
