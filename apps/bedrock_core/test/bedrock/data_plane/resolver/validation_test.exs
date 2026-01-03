defmodule Bedrock.DataPlane.Resolver.ValidationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver.Validation

  describe "check_transactions/1" do
    test "returns :ok for valid list of binary transactions" do
      transactions = [<<1, 2, 3>>, <<4, 5, 6>>]
      assert :ok = Validation.check_transactions(transactions)
    end

    test "returns :ok for empty list" do
      assert :ok = Validation.check_transactions([])
    end

    test "returns error when transactions is not a list" do
      assert {:error, msg} = Validation.check_transactions("not a list")
      assert msg =~ "invalid transactions: expected list of binary transactions"

      assert {:error, msg} = Validation.check_transactions(%{})
      assert msg =~ "invalid transactions: expected list of binary transactions"

      assert {:error, msg} = Validation.check_transactions(nil)
      assert msg =~ "invalid transactions: expected list of binary transactions"
    end

    test "returns error when list contains non-binary elements" do
      transactions = [<<1, 2, 3>>, 123, <<4, 5, 6>>]
      assert {:error, msg} = Validation.check_transactions(transactions)
      assert msg =~ "invalid transaction format: all transactions must be binary"

      transactions = [<<1, 2, 3>>, :atom, <<4, 5, 6>>]
      assert {:error, msg} = Validation.check_transactions(transactions)
      assert msg =~ "invalid transaction format: all transactions must be binary"
    end
  end
end
