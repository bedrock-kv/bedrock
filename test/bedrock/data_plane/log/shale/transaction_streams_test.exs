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

    test "returns error when given empty segment list" do
      assert {:error, :not_found} = TransactionStreams.from_segments([], Version.from_integer(1))
    end

    test "returns error when no segments contain transactions after target version" do
      # We need segments that exist but have no transactions after the target
      transaction_1 = create_test_transaction(1, %{"key1" => "value1"})
      segment = create_test_segment("test_path", 1, [transaction_1])

      # Target version is higher than all transactions, so we get :not_found
      assert {:error, :not_found} = TransactionStreams.from_segments([segment], Version.from_integer(100))
    end

    test "returns error when all segments have only transactions at or before target version" do
      # Create segments where all transactions match the target version (not after)
      transaction_1 = create_test_transaction(1, %{"key1" => "value1"})
      transaction_2 = create_test_transaction(2, %{"key2" => "value2"})

      segment1 = create_test_segment("test_path1", 1, [transaction_1])
      segment2 = create_test_segment("test_path2", 2, [transaction_2])

      # Target version 2 means we want transactions > 2, but highest is 2
      assert {:error, :not_found} = TransactionStreams.from_segments([segment1, segment2], Version.from_integer(2))
    end
  end

  describe "TransactionStreams.from_list_of_transactions/1" do
    test "creates stream from transaction list function" do
      transactions = [
        create_test_transaction(1, %{"key1" => "value1"}),
        create_test_transaction(2, %{"key2" => "value2"})
      ]

      transactions_fn = fn -> transactions end
      stream = TransactionStreams.from_list_of_transactions(transactions_fn)

      assert Enum.count(stream) == 2
    end

    test "handles nil from transaction function" do
      transactions_fn = fn -> nil end
      stream = TransactionStreams.from_list_of_transactions(transactions_fn)

      # Stream should be empty when function returns nil
      assert Enum.to_list(stream) == []
    end
  end
end
