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

      # The purged record is deleted outright: the leader consults
      # has_transaction_id?/2 to reposition its send cursor on a follower's
      # hint, so a stale physical record would confirm an unreachable entry.
      refute Log.has_transaction_id?(log, {1, 3})
    end

    test "refuses to purge committed transactions", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [
          {{1, 1}, {1, :data1}},
          {{1, 2}, {1, :data2}},
          {{1, 3}, {1, :data3}}
        ])

      {:ok, _log} = Log.commit_up_to(log, {1, 3})

      assert {1, 3} = Log.newest_safe_transaction_id(log)

      # Raft's commit index must never decrease.
      assert {:error, :would_delete_committed_transactions} =
               Log.purge_transactions_after(log, {1, 1})

      # The log is untouched.
      assert {1, 3} = Log.newest_safe_transaction_id(log)
      assert {1, 3} = Log.newest_transaction_id(log)
      assert Log.has_transaction_id?(log, {1, 2})
      assert Log.has_transaction_id?(log, {1, 3})
    end

    test "purge at or beyond the newest transaction is a no-op", %{log: log} do
      {:ok, _log} =
        Log.append_transactions(log, {0, 0}, [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}])

      assert {:ok, _log} = Log.purge_transactions_after(log, {1, 2})
      assert {1, 2} = Log.newest_transaction_id(log)
      assert [_, _] = Log.transactions_to(log, :newest)

      assert {:ok, _log} = Log.purge_transactions_after(log, {7, 9})
      assert {1, 2} = Log.newest_transaction_id(log)
      assert [_, _] = Log.transactions_to(log, :newest)
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

  describe "transactions_from/4 (bounded reads)" do
    setup :with_log

    test "limit 0 returns no transactions", %{log: log} do
      create_test_chain(log)

      assert [] = Log.transactions_from(log, {0, 0}, :newest, 0)
    end

    test "limit bounds the batch, preserving order from the front", %{log: log} do
      create_test_chain(log)

      assert [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}] =
               Log.transactions_from(log, {0, 0}, :newest, 2)

      assert [{{2, 3}, {2, :data3}}] = Log.transactions_from(log, {1, 2}, :newest, 1)
    end

    test ":infinity behaves exactly like transactions_from/3", %{log: log} do
      create_test_chain(log)

      assert Log.transactions_from(log, {1, 1}, :newest, :infinity) ==
               Log.transactions_from(log, {1, 1}, :newest)
    end

    test "limit larger than the available range returns everything", %{log: log} do
      create_test_chain(log)

      assert [_, _, _, _] = Log.transactions_from(log, {0, 0}, :newest, 100)
    end

    test "respects the to bound together with the limit", %{log: log} do
      create_test_chain(log)

      assert [{{1, 1}, {1, :data1}}] = Log.transactions_from(log, {0, 0}, {1, 2}, 1)
      assert [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}] = Log.transactions_from(log, {0, 0}, {1, 2}, 100)
    end

    test ":newest_safe bound honors the commit point", %{log: log} do
      create_test_chain(log)
      {:ok, _log} = Log.commit_up_to(log, {1, 2})

      assert [{{1, 1}, {1, :data1}}, {{1, 2}, {1, :data2}}] =
               Log.transactions_from(log, {0, 0}, :newest_safe, :infinity)
    end
  end

  describe "previous_transaction_id/2" do
    setup :with_log

    test "returns the predecessor of a middle entry", %{log: log} do
      create_test_chain(log)

      assert {1, 2} = Log.previous_transaction_id(log, {2, 3})
      assert {2, 3} = Log.previous_transaction_id(log, {2, 4})
    end

    test "returns the initial id for the first entry", %{log: log} do
      create_test_chain(log)

      assert {0, 0} = Log.previous_transaction_id(log, {1, 1})
    end

    test "returns the initial id for the initial id itself", %{log: log} do
      create_test_chain(log)

      assert {0, 0} = Log.previous_transaction_id(log, {0, 0})
    end

    test "returns the newest older entry for an absent id", %{log: log} do
      create_test_chain(log)

      # {1, 7} sorts between {1, 2} and {2, 3}
      assert {1, 2} = Log.previous_transaction_id(log, {1, 7})
      # Beyond the newest entry, the newest entry is the predecessor.
      assert {2, 4} = Log.previous_transaction_id(log, {9, 9})
    end

    test "returns the initial id on an empty log", %{log: log} do
      assert {0, 0} = Log.previous_transaction_id(log, {5, 5})
    end

    test "does not resurrect purged entries", %{log: log} do
      create_test_chain(log)
      {:ok, _log} = Log.purge_transactions_after(log, {1, 1})
      {:ok, _log} = Log.append_transactions(log, {1, 1}, [{{3, 2}, {3, :after_purge}}])

      # {2, 3} and {2, 4} were purged; the predecessor of {3, 2} is {1, 1}.
      assert {1, 1} = Log.previous_transaction_id(log, {3, 2})
    end
  end

  describe "election state" do
    setup :with_log

    test "a fresh log has term 0 and no vote", %{log: log} do
      assert 0 = Log.current_term(log)
      assert nil == Log.voted_for(log)
    end

    test "save_election_state/3 advancing the term records term and vote", %{log: log} do
      assert {:ok, _log} = Log.save_election_state(log, 3, :node_a)
      assert 3 = Log.current_term(log)
      assert :node_a = Log.voted_for(log)
    end

    test "advancing the term with nil clears the earlier vote", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:ok, _log} = Log.save_election_state(log, 4, nil)
      assert 4 = Log.current_term(log)
      assert nil == Log.voted_for(log)
    end

    test "an equal-term write may set a vote when none exists", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, nil)

      assert {:ok, _log} = Log.save_election_state(log, 3, :node_b)
      assert :node_b = Log.voted_for(log)
    end

    test "an equal-term write may repeat the existing vote", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:ok, _log} = Log.save_election_state(log, 3, :node_a)
      assert :node_a = Log.voted_for(log)
    end

    test "an equal-term write must not change or clear an existing vote", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:error, :already_voted} = Log.save_election_state(log, 3, :node_b)
      assert {:error, :already_voted} = Log.save_election_state(log, 3, nil)
      assert :node_a = Log.voted_for(log)
    end

    test "a lower-term write is rejected as stale", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:error, :stale_term} = Log.save_election_state(log, 2, :node_b)
      assert 3 = Log.current_term(log)
      assert :node_a = Log.voted_for(log)
    end

    test "save_current_term/2 clears an earlier vote when advancing", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:ok, _log} = Log.save_current_term(log, 5)
      assert 5 = Log.current_term(log)
      assert nil == Log.voted_for(log)
    end

    test "save_current_term/2 at or below the current term keeps term and vote", %{log: log} do
      {:ok, _log} = Log.save_election_state(log, 3, :node_a)

      assert {:ok, _log} = Log.save_current_term(log, 3)
      assert {:ok, _log} = Log.save_current_term(log, 2)
      assert 3 = Log.current_term(log)
      assert :node_a = Log.voted_for(log)
    end

    test "election state survives close and reopen", %{tmp_dir: tmp_dir} do
      # A separate directory, so this open doesn't collide with the table the
      # with_log setup already holds on this test's raft_log.dets.
      tmp_dir = Path.join(tmp_dir, "reopen")
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :election_reopen_test)
      {:ok, log} = DiskRaftLog.open(log)
      {:ok, _log} = Log.save_election_state(log, 7, :node_c)
      DiskRaftLog.close(log)

      log2 = DiskRaftLog.new(log_dir: tmp_dir, table_name: :election_reopen_test)
      {:ok, log2} = DiskRaftLog.open(log2)
      on_exit(fn -> DiskRaftLog.close(log2) end)

      assert 7 = Log.current_term(log2)
      assert :node_c = Log.voted_for(log2)
    end

    test "a legacy current_term record is still read, with no vote", %{log: log} do
      # Logs written before 0.10 persisted the term under :current_term.
      :ok = :dets.insert(log.table_name, {:current_term, 4})

      assert 4 = Log.current_term(log)
      assert nil == Log.voted_for(log)

      # An equal-term vote grant works against the legacy record.
      assert {:ok, _log} = Log.save_election_state(log, 4, :node_a)
      assert 4 = Log.current_term(log)
      assert :node_a = Log.voted_for(log)

      # And a stale write is still rejected against it.
      assert {:error, :stale_term} = Log.save_election_state(log, 3, :node_b)
    end
  end
end
