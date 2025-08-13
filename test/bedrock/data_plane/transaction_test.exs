defmodule Bedrock.DataPlane.Log.TransactionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Transaction

  describe "new/1" do
    test "creates a new transaction" do
      assert {0, %{key: "value"}} = Transaction.new(0, %{key: "value"})
    end
  end

  describe "version/1" do
    test "returns the version of the transaction" do
      tx = Transaction.new(0, %{key: "value"})
      assert 0 == Transaction.version(tx)
    end
  end

  describe "key_values/1" do
    test "returns the key values of the transaction" do
      tx = Transaction.new(0, %{key: "value"})
      assert %{key: "value"} = Transaction.key_values(tx)
    end
  end
end
