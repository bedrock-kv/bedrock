defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogProtocolTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.Log

  @moduletag :tmp_dir

  def with_log(%{tmp_dir: tmp_dir} = context) do
    log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :protocol_test)
    {:ok, log} = DiskRaftLog.open(log)

    on_exit(fn -> DiskRaftLog.close(log) end)

    {:ok, Map.put(context, :log, log)}
  end

  defp create_test_chain(log) do
    # Create test chain: {0,0} -> {1,1} -> {1,2} -> {2,3} -> {2,4}
    {:ok, _log} =
      Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}])

    {:ok, _log} =
      Log.append_transactions(log, {1, 2}, [{{2, 3}, {2, :data3}}, {{2, 4}, {2, :data4}}])

    log
  end

  describe "basic protocol methods" do
    setup :with_log

    test "new_id/3 creates transaction IDs", %{log: log} do
      assert {1, 5} = Log.new_id(log, 1, 5)
      assert {42, 99} = Log.new_id(log, 42, 99)
    end

    test "initial_transaction_id/1 returns {0, 0}", %{log: log} do
      assert {0, 0} = Log.initial_transaction_id(log)
    end

    test "has_transaction_id?/2 handles {0, 0} specially", %{log: log} do
      # {0, 0} always returns true without lookup
      assert Log.has_transaction_id?(log, {0, 0})

      # Other IDs require actual lookup
      refute Log.has_transaction_id?(log, {1, 1})
    end

    test "newest_transaction_id/1 returns {0, 0} for empty log", %{log: log} do
      assert {0, 0} = Log.newest_transaction_id(log)
    end

    test "newest_safe_transaction_id/1 returns {0, 0} for empty log", %{log: log} do
      assert {0, 0} = Log.newest_safe_transaction_id(log)
    end
  end

  describe "append_transactions/3" do
    setup :with_log

    test "can append first transactions from {0, 0}", %{log: log} do
      transactions = [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}]

      assert {:ok, _log} = Log.append_transactions(log, {0, 0}, transactions)

      # Verify transactions were stored and tail was updated
      assert Log.has_transaction_id?(log, {1, 1})
      assert Log.has_transaction_id?(log, {1, 2})
      assert {1, 2} = Log.newest_transaction_id(log)
    end

    test "can append transactions from existing transaction", %{log: log} do
      # First append
      {:ok, _log} = Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}])

      # Second append
      assert {:ok, _log} =
               Log.append_transactions(log, {1, 1}, [{{1, 2}, {1, :data2}}, {{1, 3}, {1, :data3}}])

      # Verify all transactions exist
      assert Log.has_transaction_id?(log, {1, 1})
      assert Log.has_transaction_id?(log, {1, 2})
      assert Log.has_transaction_id?(log, {1, 3})
      assert {1, 3} = Log.newest_transaction_id(log)
    end

    test "fails when prev_transaction_id doesn't exist", %{log: log} do
      assert {:error, :prev_transaction_not_found} =
               Log.append_transactions(log, {99, 99}, [{{1, 1}, {1, :data}}])
    end

    test "handles empty transaction list", %{log: log} do
      assert {:ok, _log} = Log.append_transactions(log, {0, 0}, [])

      # Tail should remain {0, 0} since no transactions were added
      assert {0, 0} = Log.newest_transaction_id(log)
    end

    test "creates proper chain structure", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}])

      # Verify chain links exist
      assert [{{:chain, {0, 0}}, {1, 1}}] = :dets.lookup(log.table_name, {:chain, {0, 0}})
      assert [{{:chain, {1, 1}}, {1, 2}}] = :dets.lookup(log.table_name, {:chain, {1, 1}})
      assert [{{:chain, {1, 2}}, nil}] = :dets.lookup(log.table_name, {:chain, {1, 2}})
    end
  end

  describe "transactions_to/2 and transactions_from/3" do
    setup :with_log

    setup %{log: log} do
      log = create_test_chain(log)
      {:ok, log: log}
    end

    test "transactions_to/2 with :newest returns all transactions", %{log: log} do
      result = Log.transactions_to(log, :newest)

      expected = [
        {{1, 1}, {1, :data1}},
        {{1, 2}, {1, :data2}},
        {{2, 3}, {2, :data3}},
        {{2, 4}, {2, :data4}}
      ]

      assert result == expected
    end

    test "transactions_to/2 with specific ID returns transactions up to ID", %{log: log} do
      result = Log.transactions_to(log, {1, 2})

      expected = [
        {{1, 1}, {1, :data1}},
        {{1, 2}, {1, :data2}}
      ]

      assert result == expected
    end

    test "transactions_from/3 from {0,0} includes all transactions (special case)", %{log: log} do
      result = Log.transactions_from(log, {0, 0}, {2, 3})

      expected = [
        {{1, 1}, {1, :data1}},
        {{1, 2}, {1, :data2}},
        {{2, 3}, {2, :data3}}
      ]

      assert result == expected
    end

    test "transactions_from/3 excludes the 'from' transaction", %{log: log} do
      result = Log.transactions_from(log, {1, 1}, {2, 3})

      # Should exclude {1, 1} but include {1, 2} and {2, 3}
      expected = [
        {{1, 2}, {1, :data2}},
        {{2, 3}, {2, :data3}}
      ]

      assert result == expected
    end

    test "transactions_from/3 with :newest symbol", %{log: log} do
      result = Log.transactions_from(log, {1, 2}, :newest)

      expected = [
        {{2, 3}, {2, :data3}},
        {{2, 4}, {2, :data4}}
      ]

      assert result == expected
    end
  end

  describe "commit_up_to/2" do
    setup :with_log

    test "commit_up_to/2 with {0, 0} returns :unchanged", %{log: log} do
      assert :unchanged = Log.commit_up_to(log, {0, 0})
    end

    test "can commit transaction and updates newest_safe_transaction_id", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}])

      # Initially no commits
      assert {0, 0} = Log.newest_safe_transaction_id(log)

      # Commit up to {1, 1}
      assert {:ok, _log} = Log.commit_up_to(log, {1, 1})
      assert {1, 1} = Log.newest_safe_transaction_id(log)

      # Commit further
      assert {:ok, _log} = Log.commit_up_to(log, {1, 2})
      assert {1, 2} = Log.newest_safe_transaction_id(log)
    end

    test "commit_up_to/2 with same commit level returns :unchanged", %{log: log} do
      {:ok, _log} = Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}])
      {:ok, _log} = Log.commit_up_to(log, {1, 1})

      # Trying to commit to same level should return :unchanged
      assert :unchanged = Log.commit_up_to(log, {1, 1})

      # Trying to commit to earlier level should return :unchanged
      assert :unchanged = Log.commit_up_to(log, {0, 0})
    end

    test "transactions_to/2 with :newest_safe respects commits", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}}
        ])

      {:ok, _log} = Log.commit_up_to(log, {1, 2})

      result = Log.transactions_to(log, :newest_safe)

      expected = [
        {{1, 1}, {1, :data1}},
        {{1, 2}, {1, :data2}}
      ]

      assert result == expected
    end
  end

  describe "purge_transactions_after/2" do
    setup :with_log

    test "truncates log after specified transaction", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}}
        ])

      # Verify all transactions exist
      assert Log.has_transaction_id?(log, {1, 1})
      assert Log.has_transaction_id?(log, {1, 2})
      assert Log.has_transaction_id?(log, {1, 3})
      assert {1, 3} = Log.newest_transaction_id(log)

      # Truncate after {1, 2}
      assert {:ok, _log} = Log.purge_transactions_after(log, {1, 2})

      # Verify truncation
      assert {1, 2} = Log.newest_transaction_id(log)

      # Verify {1, 3} is no longer reachable via chain traversal
      result = Log.transactions_to(log, :newest)
      expected = [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}]
      assert result == expected

      # The actual record may still exist in DETS but shouldn't be reachable
      # Physical record exists
      assert Log.has_transaction_id?(log, {1, 3})
    end

    test "adjusts commit level when purging committed transactions", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}}
        ])

      {:ok, _log} = Log.commit_up_to(log, {1, 3})

      assert {1, 3} = Log.newest_safe_transaction_id(log)

      # Purge after {1, 1} - should adjust commit level
      assert {:ok, _log} = Log.purge_transactions_after(log, {1, 1})

      # Commit level should be reduced to {1, 1}
      assert {1, 1} = Log.newest_safe_transaction_id(log)
    end

    test "leaves commit level unchanged when purging beyond commit", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}}
        ])

      {:ok, _log} = Log.commit_up_to(log, {1, 1})

      assert {1, 1} = Log.newest_safe_transaction_id(log)

      # Purge after {1, 2} - commit level should remain unchanged
      assert {:ok, _log} = Log.purge_transactions_after(log, {1, 2})

      assert {1, 1} = Log.newest_safe_transaction_id(log)
    end
  end

  describe "edge cases and error conditions" do
    setup :with_log

    test "empty log behavior", %{log: log} do
      # All transactions_* methods should return empty lists
      assert [] = Log.transactions_to(log, :newest)
      assert [] = Log.transactions_to(log, :newest_safe)
      assert [] = Log.transactions_to(log, {1, 1})
      assert [] = Log.transactions_from(log, {0, 0}, {1, 1})
      assert [] = Log.transactions_from(log, {1, 1}, {2, 2})

      # Newest IDs should be {0, 0}
      assert {0, 0} = Log.newest_transaction_id(log)
      assert {0, 0} = Log.newest_safe_transaction_id(log)
    end

    test "boundary and error conditions for range queries", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}])

      # from == to should return empty (since from is excluded)
      assert [] = Log.transactions_from(log, {1, 1}, {1, 1})

      # from > to should return empty
      assert [] = Log.transactions_from(log, {1, 2}, {1, 1})

      # transactions_to with exact boundary
      assert [{{1, 1}, {1, :data1}}] = Log.transactions_to(log, {1, 1})

      # from not found should return empty
      assert [] = Log.transactions_from(log, {99, 99}, {2, 4})
    end

    test "large transaction sequences", %{log: log} do
      # Create a longer chain to test performance
      large_transactions = for i <- 1..50, do: {{1, i}, {1, {:data, i}}}

      {:ok, _log} = Log.append_transactions(log, {0, 0}, large_transactions)

      # Verify all transactions exist
      assert {1, 50} = Log.newest_transaction_id(log)

      # Test range query performance
      result = Log.transactions_from(log, {1, 10}, {1, 20})
      # {1, 11} through {1, 20}
      assert length(result) == 10

      # Verify first and last in range
      assert {{1, 11}, {1, {:data, 11}}} = List.first(result)
      assert {{1, 20}, {1, {:data, 20}}} = List.last(result)
    end
  end

  describe "concurrent access patterns" do
    setup :with_log

    test "multiple append operations maintain consistency", %{log: log} do
      # Simulate multiple sequential appends as might happen in Raft
      {:ok, _log} = Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :term1_entry1}}])

      {:ok, _log} =
        Log.append_transactions(log, {1, 1}, [
          {{1, 2}, {1, :term1_entry2}},
          {{1, 3}, {1, :term1_entry3}}
        ])

      {:ok, _log} = Log.append_transactions(log, {1, 3}, [{{2, 4}, {2, :term2_entry1}}])

      # Verify chain integrity
      result = Log.transactions_to(log, :newest)

      expected = [
        {{1, 1}, {1, :term1_entry1}},
        {{1, 2}, {1, :term1_entry2}},
        {{1, 3}, {1, :term1_entry3}},
        {{2, 4}, {2, :term2_entry1}}
      ]

      assert result == expected

      # Verify chain links are correct
      assert [{{:chain, {0, 0}}, {1, 1}}] = :dets.lookup(log.table_name, {:chain, {0, 0}})
      assert [{{:chain, {1, 1}}, {1, 2}}] = :dets.lookup(log.table_name, {:chain, {1, 1}})
      assert [{{:chain, {1, 2}}, {1, 3}}] = :dets.lookup(log.table_name, {:chain, {1, 2}})
      assert [{{:chain, {1, 3}}, {2, 4}}] = :dets.lookup(log.table_name, {:chain, {1, 3}})
      assert [{{:chain, {2, 4}}, nil}] = :dets.lookup(log.table_name, {:chain, {2, 4}})
    end

    test "commit and purge operations work together", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}},
          {{1, 4}, {1, :data4}}
        ])

      # Commit some transactions
      {:ok, _log} = Log.commit_up_to(log, {1, 2})
      assert {1, 2} = Log.newest_safe_transaction_id(log)

      # Purge after committed transaction
      {:ok, _log} = Log.purge_transactions_after(log, {1, 3})

      # Verify state
      assert {1, 3} = Log.newest_transaction_id(log)
      # Should remain unchanged
      assert {1, 2} = Log.newest_safe_transaction_id(log)

      # Verify accessible transactions
      result = Log.transactions_to(log, :newest)
      expected = [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}, {{1, 3}, {1, :data3}}]
      assert result == expected

      result_safe = Log.transactions_to(log, :newest_safe)
      expected_safe = [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}]
      assert result_safe == expected_safe
    end
  end

  describe "persistence across restarts" do
    test "maintains raft log state across close/reopen", %{tmp_dir: tmp_dir} do
      table_name = :persistence_test2

      # First session: create log and add transactions
      log1 = DiskRaftLog.new(log_dir: tmp_dir, table_name: table_name)
      {:ok, log1} = DiskRaftLog.open(log1)

      {:ok, _log1} =
        Log.append_transactions(log1, {0, 0}, [
          {{1, 1}, {1, :persistent1}},
          {{1, 2}, {1, :persistent2}},
          {{2, 3}, {2, :persistent3}}
        ])

      {:ok, _log1} = Log.commit_up_to(log1, {1, 2})

      # Verify state before closing
      assert {2, 3} = Log.newest_transaction_id(log1)
      assert {1, 2} = Log.newest_safe_transaction_id(log1)
      assert Log.has_transaction_id?(log1, {1, 1})
      assert Log.has_transaction_id?(log1, {2, 3})

      DiskRaftLog.close(log1)

      # Second session: reopen and verify state is preserved
      log2 = DiskRaftLog.new(log_dir: tmp_dir, table_name: table_name)
      {:ok, log2} = DiskRaftLog.open(log2)

      assert {2, 3} = Log.newest_transaction_id(log2)
      assert {1, 2} = Log.newest_safe_transaction_id(log2)
      assert Log.has_transaction_id?(log2, {1, 1})
      assert Log.has_transaction_id?(log2, {1, 2})
      assert Log.has_transaction_id?(log2, {2, 3})

      # Verify we can continue appending
      {:ok, _log2} = Log.append_transactions(log2, {2, 3}, [{{2, 4}, {2, :new_after_restart}}])
      assert {2, 4} = Log.newest_transaction_id(log2)

      DiskRaftLog.close(log2)
    end
  end
end
