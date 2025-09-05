defmodule Bedrock.DataPlane.TransactionTestSupportTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.TransactionTestSupport

  describe "new_log_transaction/2" do
    test "creates a new log transaction" do
      encoded = TransactionTestSupport.new_log_transaction(0, %{"key" => "value"})
      assert is_binary(encoded)
      assert TransactionTestSupport.extract_log_version(encoded) == <<0::64>>
      assert TransactionTestSupport.extract_log_writes(encoded) == %{"key" => "value"}
    end
  end

  describe "extract_log_version/1" do
    test "returns the version of the transaction" do
      encoded = TransactionTestSupport.new_log_transaction(42, %{"key" => "value"})
      assert <<42::64>> == TransactionTestSupport.extract_log_version(encoded)
    end
  end

  describe "extract_log_writes/1" do
    test "returns the key values of the transaction" do
      encoded = TransactionTestSupport.new_log_transaction(0, %{"key" => "value"})
      assert %{"key" => "value"} = TransactionTestSupport.extract_log_writes(encoded)
    end
  end
end
