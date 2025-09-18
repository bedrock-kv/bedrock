defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog

  @moduletag :tmp_dir

  # Helper function to create and open a log for testing
  defp create_opened_log(tmp_dir, table_name) do
    log = DiskRaftLog.new(log_dir: tmp_dir, table_name: table_name)
    {:ok, opened_log} = DiskRaftLog.open(log)
    opened_log
  end

  describe "module initialization" do
    test "creates new log with proper structure", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :test_log)

      assert %DiskRaftLog{
               table_name: :test_log,
               is_open: false
             } = log

      assert String.contains?(log.table_file, "raft_log.dets")
    end

    test "creates log directory if it doesn't exist" do
      non_existent_dir = "/tmp/bedrock_test_#{:rand.uniform(1_000_000)}"

      on_exit(fn ->
        if File.exists?(non_existent_dir), do: File.rm_rf!(non_existent_dir)
      end)

      refute File.exists?(non_existent_dir)

      _log = DiskRaftLog.new(log_dir: non_existent_dir)

      assert File.exists?(non_existent_dir)
      assert File.dir?(non_existent_dir)
    end
  end

  describe "DETS table lifecycle" do
    test "can open and close DETS table", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :lifecycle_test)

      assert {:ok, %DiskRaftLog{is_open: true} = opened_log} = DiskRaftLog.open(log)
      assert :ok = DiskRaftLog.close(opened_log)
    end

    test "handles DETS errors gracefully" do
      log = %DiskRaftLog{
        table_name: :bad_table,
        table_file: "/non/existent/path/test.dets",
        is_open: false
      }

      assert {:error, _reason} = DiskRaftLog.open(log)
    end

    test "survives table recreation", %{tmp_dir: tmp_dir} do
      # Open, write some test data, and close
      log = create_opened_log(tmp_dir, :recreation_test)
      :dets.insert(log.table_name, {{1, 1}, :test_data})
      :dets.close(log.table_name)

      # Reopen and verify data persisted
      log = create_opened_log(tmp_dir, :recreation_test)
      assert [{_id, :test_data}] = :dets.lookup(log.table_name, {1, 1})

      DiskRaftLog.close(log)
    end
  end

  describe "helper functions" do
    test "build_chain_links/2 creates proper forward pointers" do
      transactions = [
        {{1, 1}, :data1},
        {{1, 2}, :data2},
        {{1, 3}, :data3}
      ]

      prev_id = {0, 0}

      assert [
               # prev -> first
               {{:chain, {0, 0}}, {1, 1}},
               # first -> second
               {{:chain, {1, 1}}, {1, 2}},
               # second -> third
               {{:chain, {1, 2}}, {1, 3}},
               # third -> nil (end)
               {{:chain, {1, 3}}, nil}
             ] = DiskRaftLog.build_chain_links(prev_id, transactions)
    end

    test "build_chain_links/2 handles empty transaction list" do
      assert [] = DiskRaftLog.build_chain_links({0, 0}, [])
    end

    test "build_chain_links/2 handles single transaction" do
      transactions = [{{1, 1}, :data1}]
      prev_id = {0, 0}

      assert [
               # prev -> first
               {{:chain, {0, 0}}, {1, 1}},
               # first -> nil (end)
               {{:chain, {1, 1}}, nil}
             ] = DiskRaftLog.build_chain_links(prev_id, transactions)
    end

    test "walk_chain_inclusive/3 follows chain correctly", %{tmp_dir: tmp_dir} do
      log = create_opened_log(tmp_dir, :chain_test)

      # Set up test chain: {1,1} -> {1,2} -> {1,3}
      test_data = [
        {{1, 1}, :data1},
        {{1, 2}, :data2},
        {{1, 3}, :data3},
        {{:chain, {1, 1}}, {1, 2}},
        {{:chain, {1, 2}}, {1, 3}},
        {{:chain, {1, 3}}, nil}
      ]

      :dets.insert(log.table_name, test_data)

      # Walk full chain
      assert [{{1, 1}, :data1}, {{1, 2}, :data2}, {{1, 3}, :data3}] =
               DiskRaftLog.walk_chain_inclusive(log, {1, 1}, {1, 3})

      # Walk partial chain
      assert [{{1, 2}, :data2}, {{1, 3}, :data3}] =
               DiskRaftLog.walk_chain_inclusive(log, {1, 2}, {1, 3})

      # Walk single element
      assert [{{1, 2}, :data2}] =
               DiskRaftLog.walk_chain_inclusive(log, {1, 2}, {1, 2})

      # Walk beyond range (should stop at boundary)
      assert [{{1, 1}, :data1}, {{1, 2}, :data2}] =
               DiskRaftLog.walk_chain_inclusive(log, {1, 1}, {1, 2})

      DiskRaftLog.close(log)
    end
  end

  describe "DETS sync functionality" do
    test "sync returns :ok when table is open", %{tmp_dir: tmp_dir} do
      log = create_opened_log(tmp_dir, :sync_test)

      assert :ok = DiskRaftLog.sync(log)

      DiskRaftLog.close(log)
    end

    test "sync handles closed table gracefully", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :closed_sync_test)

      # Don't open the table, just try to sync
      assert {:error, _reason} = DiskRaftLog.sync(log)
    end
  end
end
