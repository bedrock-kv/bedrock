defmodule Bedrock.ControlPlane.Coordinator.DiskRaftPersistenceTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.Log

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    log_dir = Path.join(tmp_dir, "persistence_test")
    %{log_dir: log_dir}
  end

  # Helper functions for common test patterns
  defp create_and_populate_log(log_dir, log_name) do
    log = DiskRaftLog.new(log_dir: log_dir, log_name: log_name)
    {:ok, log} = DiskRaftLog.open(log)

    initial_id = Log.initial_transaction_id(log)

    {:ok, log} =
      Log.append_transactions(log, initial_id, [
        {{1, 1}, {1, {:coordinator_config, %{epoch: 1, coordinators: [:node1, :node2]}}}},
        {{1, 2}, {1, {:director_assignment, :node1}}},
        {{2, 3}, {2, {:coordinator_config, %{epoch: 2, coordinators: [:node1, :node2, :node3]}}}}
      ])

    {:ok, log} = Log.commit_up_to(log, {2, 3})
    log
  end

  defp verify_transaction_state(log, expected_newest_id) do
    # Use pattern matching to verify both IDs at once
    newest_id = Log.newest_transaction_id(log)
    newest_safe_id = Log.newest_safe_transaction_id(log)
    assert {^expected_newest_id, ^expected_newest_id} = {newest_id, newest_safe_id}
  end

  defp verify_transaction_existence(log, transaction_ids) when is_list(transaction_ids) do
    # Batch verify all transaction IDs exist
    for id <- transaction_ids do
      assert Log.has_transaction_id?(log, id)
    end
  end

  describe "coordinator raft persistence verification" do
    test "disk raft log persists data across restarts", %{log_dir: log_dir} do
      # First session: create and populate log
      log1 = create_and_populate_log(log_dir, :persistence_test)

      # Verify state before restart using consolidated assertion
      verify_transaction_state(log1, {2, 3})

      # Verify transaction count
      assert [_, _, _] = Log.transactions_to(log1, :newest)

      DiskRaftLog.close(log1)

      # Second session: reopen and verify persistence
      log2 = DiskRaftLog.new(log_dir: log_dir, log_name: :persistence_test)
      {:ok, log2} = DiskRaftLog.open(log2)

      # Verify all state is preserved using helpers
      verify_transaction_state(log2, {2, 3})
      verify_transaction_existence(log2, [{1, 1}, {1, 2}, {2, 3}])

      # Verify transaction content with consolidated pattern matching
      expected_config1 = %{epoch: 1, coordinators: [:node1, :node2]}
      expected_config2 = %{epoch: 2, coordinators: [:node1, :node2, :node3]}

      assert [
               {{1, 1}, {1, {:coordinator_config, ^expected_config1}}},
               {{1, 2}, {1, {:director_assignment, :node1}}},
               {{2, 3}, {2, {:coordinator_config, ^expected_config2}}}
             ] = Log.transactions_to(log2, :newest)

      # Verify we can continue adding after restart
      {:ok, log2} =
        Log.append_transactions(log2, {2, 3}, [
          {{2, 4}, {2, {:post_restart_transaction, :success}}}
        ])

      assert Log.newest_transaction_id(log2) == {2, 4}
      DiskRaftLog.close(log2)
    end

    test "verifies coordinator follows standard working directory pattern", %{log_dir: log_dir} do
      # This test verifies coordinator uses same pattern as logs/storage
      working_directory = Path.join(log_dir, "raft")
      raft_log_file = Path.join(working_directory, "raft_log.dets")

      log = DiskRaftLog.new(log_dir: working_directory, log_name: :coordinator_raft)
      {:ok, log} = DiskRaftLog.open(log)

      # Verify standard directory structure and file creation with pattern matching
      assert {true, true} = {File.exists?(working_directory), File.exists?(raft_log_file)}

      # Add and verify a transaction
      initial_id = Log.initial_transaction_id(log)
      {:ok, log} = Log.append_transactions(log, initial_id, [{{1, 1}, {1, :coordinator_startup}}])

      assert Log.newest_transaction_id(log) == {1, 1}
      DiskRaftLog.close(log)

      # Verify file survives close
      assert File.exists?(raft_log_file)
    end
  end
end
