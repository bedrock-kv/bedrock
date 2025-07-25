defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogProtocolTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.Log

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :protocol_test)
    {:ok, log} = DiskRaftLog.open(log)

    on_exit(fn -> DiskRaftLog.close(log) end)

    %{log: log}
  end

  describe "Bedrock.Raft.Log protocol implementation" do
    test "implements new_id/3", %{log: log} do
      transaction_id = Log.new_id(log, 1, 5)
      assert transaction_id == {1, 5}
    end

    test "implements initial_transaction_id/1", %{log: log} do
      initial_id = Log.initial_transaction_id(log)
      assert initial_id == {0, 0}
    end

    test "implements append_transactions/3 for initial transactions", %{log: log} do
      initial_id = Log.initial_transaction_id(log)
      transactions = [{1, :first_transaction}, {1, :second_transaction}]

      {:ok, updated_log} = Log.append_transactions(log, initial_id, transactions)

      # Verify transactions were added
      assert Log.newest_transaction_id(updated_log) == {1, 2}
      assert Log.has_transaction_id?(updated_log, {1, 1})
      assert Log.has_transaction_id?(updated_log, {1, 2})
    end

    test "implements append_transactions/3 with previous transaction check", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add first batch
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :first}])

      # Add second batch with correct previous ID
      {:ok, log} = Log.append_transactions(log, {1, 1}, [{1, :second}])

      assert Log.newest_transaction_id(log) == {1, 2}

      # Try to add with wrong previous ID
      assert {:error, :prev_transaction_not_found} =
               Log.append_transactions(log, {2, 10}, [{1, :should_fail}])
    end

    test "implements commit_up_to/2", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :tx1}, {1, :tx2}, {1, :tx3}])

      # Initial commit state
      assert Log.newest_safe_transaction_id(log) == {0, 0}

      # Commit up to first transaction
      {:ok, log} = Log.commit_up_to(log, {1, 1})
      assert Log.newest_safe_transaction_id(log) == {1, 1}

      # Commit up to second transaction
      {:ok, log} = Log.commit_up_to(log, {1, 2})
      assert Log.newest_safe_transaction_id(log) == {1, 2}

      # Try to commit backwards (should be unchanged)
      assert :unchanged = Log.commit_up_to(log, {1, 1})
      assert Log.newest_safe_transaction_id(log) == {1, 2}
    end

    test "implements newest_transaction_id/1", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Empty log should return initial ID
      assert Log.newest_transaction_id(log) == {0, 0}

      # Add transactions
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :tx1}])
      assert Log.newest_transaction_id(log) == {1, 1}

      {:ok, log} = Log.append_transactions(log, {1, 1}, [{1, :tx2}, {2, :tx3}])
      assert Log.newest_transaction_id(log) == {2, 3}
    end

    test "implements has_transaction_id?/2", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Initial transaction should always exist
      assert Log.has_transaction_id?(log, {0, 0})

      # Non-existent transaction
      assert not Log.has_transaction_id?(log, {1, 1})

      # Add a transaction
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :tx1}])
      assert Log.has_transaction_id?(log, {1, 1})
      assert not Log.has_transaction_id?(log, {1, 2})
    end

    test "implements transactions_to/2", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} =
        Log.append_transactions(log, initial_id, [
          {1, :tx1},
          {1, :tx2},
          {2, :tx3}
        ])

      # Get all transactions to newest
      transactions = Log.transactions_to(log, :newest)
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}, {{2, 3}, :tx3}]

      # Get transactions to specific ID
      transactions = Log.transactions_to(log, {1, 2})
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}]

      # Get transactions to newest safe (initially nothing committed)
      transactions = Log.transactions_to(log, :newest_safe)
      assert transactions == []

      # Commit some and try again
      {:ok, log} = Log.commit_up_to(log, {1, 2})
      transactions = Log.transactions_to(log, :newest_safe)
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}]
    end

    test "implements transactions_from/3", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} =
        Log.append_transactions(log, initial_id, [
          {1, :tx1},
          {1, :tx2},
          {1, :tx3},
          {2, :tx4}
        ])

      # Get transactions from initial to newest
      transactions = Log.transactions_from(log, initial_id, Log.newest_transaction_id(log))
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}, {{1, 3}, :tx3}, {{2, 4}, :tx4}]

      # Get transactions from specific ID
      transactions = Log.transactions_from(log, {1, 1}, {1, 3})
      assert transactions == [{{1, 2}, :tx2}, {{1, 3}, :tx3}]

      # Get transactions from middle to end
      transactions = Log.transactions_from(log, {1, 2}, Log.newest_transaction_id(log))
      assert transactions == [{{1, 3}, :tx3}, {{2, 4}, :tx4}]
    end

    test "implements transactions_from/3 with :newest atom - CRITICAL BUG FIX", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} =
        Log.append_transactions(log, initial_id, [
          {1, :tx1},
          {1, :tx2},
          {2, :tx3}
        ])

      # Test the critical bug fix: transactions_from with :newest atom
      # This was causing the log replication stall in client logs
      transactions = Log.transactions_from(log, initial_id, :newest)
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}, {{2, 3}, :tx3}]

      # Test from middle to :newest  
      transactions = Log.transactions_from(log, {1, 1}, :newest)
      assert transactions == [{{1, 2}, :tx2}, {{2, 3}, :tx3}]

      # Test empty result case
      transactions = Log.transactions_from(log, {2, 3}, :newest)
      assert transactions == []
    end

    test "implements transactions_from/3 with :newest_safe atom - CRITICAL BUG FIX", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} =
        Log.append_transactions(log, initial_id, [
          {1, :tx1},
          {1, :tx2},
          {1, :tx3}
        ])

      # Initially no transactions are committed
      transactions = Log.transactions_from(log, initial_id, :newest_safe)
      assert transactions == []

      # Commit first two transactions
      {:ok, log} = Log.commit_up_to(log, {1, 2})

      # Now transactions_from with :newest_safe should work correctly
      transactions = Log.transactions_from(log, initial_id, :newest_safe)
      assert transactions == [{{1, 1}, :tx1}, {{1, 2}, :tx2}]

      # Test from middle to :newest_safe
      transactions = Log.transactions_from(log, {1, 1}, :newest_safe)
      assert transactions == [{{1, 2}, :tx2}]
    end

    test "implements purge_transactions_after/2", %{log: log} do
      initial_id = Log.initial_transaction_id(log)

      # Add some transactions
      {:ok, log} =
        Log.append_transactions(log, initial_id, [
          {1, :tx1},
          {1, :tx2},
          {1, :tx3},
          {1, :tx4}
        ])

      # Commit some transactions
      {:ok, log} = Log.commit_up_to(log, {1, 3})

      # Purge transactions after {1, 2}
      {:ok, log} = Log.purge_transactions_after(log, {1, 2})

      # Should only have transactions up to {1, 2}
      assert Log.newest_transaction_id(log) == {1, 2}
      assert Log.has_transaction_id?(log, {1, 1})
      assert Log.has_transaction_id?(log, {1, 2})
      assert not Log.has_transaction_id?(log, {1, 3})
      assert not Log.has_transaction_id?(log, {1, 4})

      # Commit point should be adjusted
      assert Log.newest_safe_transaction_id(log) == {1, 2}
    end
  end

  describe "persistence across restarts" do
    test "maintains raft log state across close/reopen", %{tmp_dir: tmp_dir} do
      log_name = :persistence_protocol_test

      # First session: create log and add transactions
      log1 = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)
      {:ok, log1} = DiskRaftLog.open(log1)

      initial_id = Log.initial_transaction_id(log1)

      {:ok, log1} =
        Log.append_transactions(log1, initial_id, [
          {1, :persistent_tx1},
          {1, :persistent_tx2},
          {2, :persistent_tx3}
        ])

      {:ok, log1} = Log.commit_up_to(log1, {1, 2})

      # Verify state before closing
      assert Log.newest_transaction_id(log1) == {2, 3}
      assert Log.newest_safe_transaction_id(log1) == {1, 2}
      assert Log.has_transaction_id?(log1, {1, 1})
      assert Log.has_transaction_id?(log1, {2, 3})

      DiskRaftLog.close(log1)

      # Second session: reopen and verify state is preserved
      log_struct = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)
      {:ok, log2} = DiskRaftLog.open(log_struct)

      assert Log.newest_transaction_id(log2) == {2, 3}
      assert Log.has_transaction_id?(log2, {1, 1})
      assert Log.has_transaction_id?(log2, {1, 2})
      assert Log.has_transaction_id?(log2, {2, 3})

      # Verify we can continue appending
      {:ok, log2} = Log.append_transactions(log2, {2, 3}, [{2, :new_tx_after_restart}])
      assert Log.newest_transaction_id(log2) == {2, 4}

      DiskRaftLog.close(log2)
    end
  end

  describe "error handling" do
    test "handles corrupted log gracefully", %{tmp_dir: tmp_dir} do
      log_name = :corruption_test

      # Create log and add some data
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)
      {:ok, log} = DiskRaftLog.open(log)

      initial_id = Log.initial_transaction_id(log)
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :tx1}])

      DiskRaftLog.close(log)

      # Corrupt the log file
      log_file = Path.join(tmp_dir, "raft_log.LOG")
      File.write!(log_file, "corrupted data")

      # Try to reopen - should handle gracefully
      log2 = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)

      # This might fail or recover gracefully depending on disk_log behavior
      case DiskRaftLog.open(log2) do
        {:ok, recovered_log} ->
          # If it opens, it should at least not crash
          assert %DiskRaftLog{} = recovered_log

        {:error, _reason} ->
          # Graceful failure is acceptable
          assert true
      end
    end
  end
end
