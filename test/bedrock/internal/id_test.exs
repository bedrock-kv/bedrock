defmodule Bedrock.Internal.IdTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Id

  describe "random/0" do
    test "generates 8-character base32 lowercase string" do
      id = Id.random()

      assert is_binary(id)
      assert String.length(id) == 8
      assert id == String.downcase(id)
      assert String.match?(id, ~r/^[a-z2-7]+$/)
    end

    test "generates unique IDs" do
      ids = for _ <- 1..100, do: Id.random()
      assert length(Enum.uniq(ids)) == 100
    end
  end

  describe "random/1" do
    test "generates ID with specified entropy bytes" do
      # 4 bytes -> 7 characters (ceil(4 * 8 / 5))
      id = Id.random(4)
      assert String.length(id) == 7

      # 8 bytes -> 13 characters
      id = Id.random(8)
      assert String.length(id) == 13
    end

    test "raises for invalid byte count" do
      assert_raise FunctionClauseError, fn -> Id.random(0) end
      assert_raise FunctionClauseError, fn -> Id.random(-1) end
    end
  end
end
