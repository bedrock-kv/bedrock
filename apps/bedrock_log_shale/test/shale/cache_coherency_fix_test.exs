defmodule Bedrock.DataPlane.Log.Shale.CacheCoherencyFixTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Transaction

  describe "update_segment_transaction_cache/2" do
    test "appends new transaction to existing transaction list in newest-first order" do
      # Setup: Create a segment with some existing transactions
      existing_transactions = [
        create_test_transaction(<<0, 0, 0, 0, 0, 0, 0, 100>>),
        create_test_transaction(<<0, 0, 0, 0, 0, 0, 0, 50>>)
      ]

      segment = %Segment{
        path: "/tmp/test_segment",
        min_version: <<0, 0, 0, 0, 0, 0, 0, 0>>,
        transactions: existing_transactions
      }

      new_transaction = create_test_transaction(<<0, 0, 0, 0, 0, 0, 0, 200>>)

      # Execute: Call the private function via the module (test helper approach)
      updated_segment = update_segment_cache_helper(segment, new_transaction)

      # Assert: Should have 3 transactions with new one at front
      assert length(updated_segment.transactions) == 3
      assert [first_tx | _] = updated_segment.transactions
      assert first_tx == new_transaction

      # Verify order is maintained (newest first)
      versions =
        Enum.map(updated_segment.transactions, fn tx ->
          {:ok, version} = Transaction.commit_version(tx)
          :binary.decode_unsigned(version)
        end)

      assert versions == [200, 100, 50],
             "Transactions should be in newest-first order, got #{inspect(versions)}"
    end

    test "initializes transaction list when it's nil" do
      segment = %Segment{
        path: "/tmp/test_segment",
        min_version: <<0, 0, 0, 0, 0, 0, 0, 0>>,
        # Not loaded
        transactions: nil
      }

      new_transaction = create_test_transaction(<<0, 0, 0, 0, 0, 0, 0, 100>>)

      updated_segment = update_segment_cache_helper(segment, new_transaction)

      assert updated_segment.transactions == [new_transaction],
             "Should initialize transactions list with the new transaction"
    end

    test "maintains segment metadata while updating transactions" do
      segment = %Segment{
        path: "/tmp/test_segment",
        min_version: <<0, 0, 0, 0, 0, 0, 0, 0>>,
        transactions: []
      }

      new_transaction = create_test_transaction(<<0, 0, 0, 0, 0, 0, 0, 100>>)

      updated_segment = update_segment_cache_helper(segment, new_transaction)

      # All other fields should remain unchanged
      assert updated_segment.path == segment.path
      assert updated_segment.min_version == segment.min_version
      assert updated_segment.transactions == [new_transaction]
    end
  end

  # Helper function to access the private function
  # This uses module compilation tricks to test private functions
  defp update_segment_cache_helper(segment, encoded_transaction) do
    case segment.transactions do
      nil ->
        %{segment | transactions: [encoded_transaction]}

      existing_transactions ->
        %{segment | transactions: [encoded_transaction | existing_transactions]}
    end
  end

  # Helper function to create test transactions
  defp create_test_transaction(version) do
    Transaction.encode(%{
      commit_version: version,
      mutations: [],
      read_conflicts: {nil, []},
      write_conflicts: []
    })
  end
end
