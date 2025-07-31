defmodule Bedrock.ControlPlane.Coordinator.DiskRaftPersistenceTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.Log

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    log_dir = Path.join(tmp_dir, "persistence_test")
    %{log_dir: log_dir}
  end

  describe "coordinator raft persistence verification" do
    test "disk raft log persists data across restarts", %{log_dir: log_dir} do
      # First session: create and populate log
      log1 = DiskRaftLog.new(log_dir: log_dir, log_name: :persistence_test)
      {:ok, log1} = DiskRaftLog.open(log1)

      # Add some coordinator-like transactions
      initial_id = Log.initial_transaction_id(log1)

      {:ok, log1} =
        Log.append_transactions(log1, initial_id, [
          {1, {:coordinator_config, %{epoch: 1, coordinators: [:node1, :node2]}}},
          {1, {:director_assignment, :node1}},
          {2, {:coordinator_config, %{epoch: 2, coordinators: [:node1, :node2, :node3]}}}
        ])

      # Commit the transactions
      {:ok, log1} = Log.commit_up_to(log1, {2, 3})

      # Verify state before restart
      assert Log.newest_transaction_id(log1) == {2, 3}
      assert Log.newest_safe_transaction_id(log1) == {2, 3}

      # Get all transactions to verify content
      transactions = Log.transactions_to(log1, :newest)
      assert length(transactions) == 3

      DiskRaftLog.close(log1)

      # Second session: reopen and verify persistence
      log2 = DiskRaftLog.new(log_dir: log_dir, log_name: :persistence_test)
      {:ok, log2} = DiskRaftLog.open(log2)

      # Verify all state is preserved
      assert Log.newest_transaction_id(log2) == {2, 3}
      assert Log.has_transaction_id?(log2, {1, 1})
      assert Log.has_transaction_id?(log2, {1, 2})
      assert Log.has_transaction_id?(log2, {2, 3})

      # Verify transaction content is preserved
      restored_transactions = Log.transactions_to(log2, :newest)
      assert length(restored_transactions) == 3

      # Check specific transaction content
      [
        {{1, 1}, {1, {:coordinator_config, config1}}},
        {{1, 2}, {1, {:director_assignment, director}}},
        {{2, 3}, {2, {:coordinator_config, config2}}}
      ] = restored_transactions

      assert config1 == %{epoch: 1, coordinators: [:node1, :node2]}
      assert director == :node1
      assert config2 == %{epoch: 2, coordinators: [:node1, :node2, :node3]}

      # Verify we can continue adding after restart
      {:ok, log2} =
        Log.append_transactions(log2, {2, 3}, [{2, {:post_restart_transaction, :success}}])

      assert Log.newest_transaction_id(log2) == {2, 4}

      DiskRaftLog.close(log2)
    end

    test "verifies coordinator follows standard working directory pattern", %{log_dir: log_dir} do
      # This test verifies coordinator uses same pattern as logs/storage
      # Base path like /data/coordinator
      base_path = log_dir
      # Working dir like workers
      working_directory = Path.join(base_path, "raft")

      log = DiskRaftLog.new(log_dir: working_directory, log_name: :coordinator_raft)
      {:ok, log} = DiskRaftLog.open(log)

      # Verify standard directory structure is created
      assert File.exists?(working_directory)
      raft_log_file = Path.join(working_directory, "raft_log.dets")
      assert File.exists?(raft_log_file)

      # Add and verify a transaction
      initial_id = Log.initial_transaction_id(log)
      {:ok, log} = Log.append_transactions(log, initial_id, [{1, :coordinator_startup}])

      assert Log.newest_transaction_id(log) == {1, 1}

      DiskRaftLog.close(log)

      # Verify file survives close
      assert File.exists?(raft_log_file)
    end
  end
end
