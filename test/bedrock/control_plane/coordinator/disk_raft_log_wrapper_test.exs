defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogWrapperTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog

  @moduletag :tmp_dir

  describe "DiskRaftLog creation and basic operations" do
    test "can create a new disk raft log", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir)

      assert %DiskRaftLog{} = log
      assert log.log_name == :raft_log
      assert is_list(log.log_file)
      assert log.index == %{}
    end

    test "can open and close a disk raft log", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :test_log)

      assert {:ok, opened_log} = DiskRaftLog.open(log)
      assert %DiskRaftLog{} = opened_log

      assert :ok = DiskRaftLog.close(opened_log)
    end

    test "can append and read entries", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :append_test)
      {:ok, log} = DiskRaftLog.open(log)

      # Append some entries
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 1}, 1, :first_entry)
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 2}, 1, :second_entry)
      {:ok, log} = DiskRaftLog.append_entry(log, {2, 3}, 2, :third_entry)

      # Sync to ensure durability
      assert :ok = DiskRaftLog.sync(log)

      # Read entries back
      {:ok, entries} = DiskRaftLog.read_all_entries(log)

      expected_entries = [
        {{1, 1}, 1, :first_entry},
        {{1, 2}, 1, :second_entry},
        {{2, 3}, 2, :third_entry}
      ]

      assert entries == expected_entries

      DiskRaftLog.close(log)
    end

    test "builds index correctly from entries", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :index_test)
      {:ok, log} = DiskRaftLog.open(log)

      # Append entries
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 1}, 1, :entry1)
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 2}, 1, :entry2)
      {:ok, log} = DiskRaftLog.append_entry(log, {2, 3}, 2, :entry3)

      # Check index was built
      assert DiskRaftLog.has_transaction_id?(log, {1, 1})
      assert DiskRaftLog.has_transaction_id?(log, {1, 2})
      assert DiskRaftLog.has_transaction_id?(log, {2, 3})
      assert not DiskRaftLog.has_transaction_id?(log, {1, 4})

      DiskRaftLog.close(log)
    end

    test "preserves data across close/reopen", %{tmp_dir: tmp_dir} do
      log_name = :persistence_test
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)

      # First session: write data
      {:ok, log} = DiskRaftLog.open(log)
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 1}, 1, :persistent_entry)
      {:ok, _log} = DiskRaftLog.append_entry(log, {1, 2}, 1, :another_entry)
      DiskRaftLog.sync(log)
      DiskRaftLog.close(log)

      # Second session: reopen and verify data
      log2 = DiskRaftLog.new(log_dir: tmp_dir, log_name: log_name)
      {:ok, log2} = DiskRaftLog.open(log2)

      {:ok, entries} = DiskRaftLog.read_all_entries(log2)
      assert length(entries) == 2
      assert {{1, 1}, 1, :persistent_entry} in entries
      assert {{1, 2}, 1, :another_entry} in entries

      # Index should be rebuilt
      assert DiskRaftLog.has_transaction_id?(log2, {1, 1})
      assert DiskRaftLog.has_transaction_id?(log2, {1, 2})

      DiskRaftLog.close(log2)
    end
  end

  describe "error handling" do
    test "handles non-existent directory gracefully" do
      # This should succeed because new/1 creates the directory
      log = DiskRaftLog.new(log_dir: "/tmp/non_existent_test_dir_#{:rand.uniform(1000)}")
      assert %DiskRaftLog{} = log
    end

    test "handles invalid log directory" do
      # Try to create log in invalid location
      assert_raise File.Error, fn ->
        DiskRaftLog.new(log_dir: "/dev/null/cannot_create_dir")
      end
    end

    test "handles operations on closed log", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :closed_test)

      # Try to append without opening
      assert {:error, _reason} = DiskRaftLog.append_entry(log, {1, 1}, 1, :test)
    end
  end

  describe "raft-specific patterns" do
    test "handles raft transaction ID patterns correctly", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, log_name: :raft_patterns)
      {:ok, log} = DiskRaftLog.open(log)

      # Test initial/genesis entry
      {:ok, log} = DiskRaftLog.append_entry(log, {0, 0}, 0, :genesis)

      # Test normal entries
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 1}, 1, :noop)
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 2}, 1, {:config_change, :add_node})
      {:ok, log} = DiskRaftLog.append_entry(log, {1, 3}, 1, {:user_command, "test"})

      # Test term change
      {:ok, log} = DiskRaftLog.append_entry(log, {2, 4}, 2, :term_change)
      {:ok, log} = DiskRaftLog.append_entry(log, {2, 5}, 2, {:user_command, "after_term_change"})

      # Verify all entries
      {:ok, entries} = DiskRaftLog.read_all_entries(log)
      assert length(entries) == 6

      # Verify transaction IDs are properly indexed
      assert DiskRaftLog.has_transaction_id?(log, {0, 0})
      assert DiskRaftLog.has_transaction_id?(log, {1, 1})
      assert DiskRaftLog.has_transaction_id?(log, {1, 2})
      assert DiskRaftLog.has_transaction_id?(log, {1, 3})
      assert DiskRaftLog.has_transaction_id?(log, {2, 4})
      assert DiskRaftLog.has_transaction_id?(log, {2, 5})

      # Verify ordering is preserved
      transaction_ids = Enum.map(entries, fn {id, _term, _data} -> id end)
      expected_ids = [{0, 0}, {1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 5}]
      assert transaction_ids == expected_ids

      DiskRaftLog.close(log)
    end
  end
end
