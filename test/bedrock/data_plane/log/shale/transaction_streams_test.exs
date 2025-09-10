defmodule Bedrock.DataPlane.Log.Shale.TransactionStreamsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  # Helper functions for common test setup
  defp create_test_transaction(version, data) do
    TransactionTestSupport.new_log_transaction(Version.from_integer(version), data)
  end

  defp create_test_segment(path, min_version, transactions \\ nil) do
    %Segment{
      path: path,
      min_version: Version.from_integer(min_version),
      transactions: transactions
    }
  end

  describe "TransactionStreams.from_segments/2 with unloaded segments" do
    test "handles nil transactions gracefully without enumerable protocol errors" do
      # Create a segment with nil transactions (simulating unloaded state)
      segment = create_test_segment("nonexistent_path_for_test", 1)

      # Before the fix, this would crash with:
      # "protocol Enumerable not implemented for type Atom. Got value: nil"
      #
      # After the fix, it should handle nil gracefully by calling ensure_transactions_are_loaded
      # The file doesn't exist, so we expect a File.Error, not an Enumerable error

      assert_raise File.Error, fn ->
        TransactionStreams.from_segments([segment], Version.from_integer(1))
      end

      # The key point is that we get a File.Error (expected) rather than:
      # Protocol.UndefinedError with "protocol Enumerable not implemented for type Atom"
    end

    test "processes segments with pre-loaded transactions correctly" do
      # Create a segment with pre-loaded transactions (reversed order as stored)
      transaction_1 = create_test_transaction(1, %{"key1" => "value1"})
      transaction_2 = create_test_transaction(2, %{"key2" => "value2"})

      segment = create_test_segment("test_path", 1, [transaction_2, transaction_1])

      # This should work normally
      assert {:ok, stream} = TransactionStreams.from_segments([segment], Version.from_integer(1))

      # Convert stream to list to verify content - should get remaining transactions after target version
      # Since target_version=1 matches the first transaction, we get the rest
      assert [transaction] = Enum.to_list(stream)
      assert TransactionTestSupport.extract_log_version(transaction) == Version.from_integer(2)
    end
  end
end
