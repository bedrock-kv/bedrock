defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog

  @moduletag :tmp_dir

  describe "module initialization" do
    test "creates new log with proper structure", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :test_log)

      assert %DiskRaftLog{} = log
      assert log.table_name == :test_log
      assert String.contains?(log.table_file, "raft_log.dets")
      assert log.is_open == false
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

      assert {:ok, opened_log} = DiskRaftLog.open(log)
      assert opened_log.is_open == true

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
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :recreation_test)

      # Open, write some test data, and close
      {:ok, log} = DiskRaftLog.open(log)
      :dets.insert(log.table_name, {{1, 1}, :test_data})
      :dets.close(log.table_name)

      # Reopen and verify data persisted
      {:ok, log} = DiskRaftLog.open(log)
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

      chain_links = DiskRaftLog.build_chain_links(prev_id, transactions)

      expected = [
        # prev -> first
        {{:chain, {0, 0}}, {1, 1}},
        # first -> second
        {{:chain, {1, 1}}, {1, 2}},
        # second -> third
        {{:chain, {1, 2}}, {1, 3}},
        # third -> nil (end)
        {{:chain, {1, 3}}, nil}
      ]

      assert chain_links == expected
    end

    test "build_chain_links/2 handles empty transaction list" do
      assert [] = DiskRaftLog.build_chain_links({0, 0}, [])
    end

    test "build_chain_links/2 handles single transaction" do
      transactions = [{{1, 1}, :data1}]
      prev_id = {0, 0}

      chain_links = DiskRaftLog.build_chain_links(prev_id, transactions)

      expected = [
        # prev -> first
        {{:chain, {0, 0}}, {1, 1}},
        # first -> nil (end)
        {{:chain, {1, 1}}, nil}
      ]

      assert chain_links == expected
    end

    test "walk_chain_inclusive/3 follows chain correctly", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :chain_test)
      {:ok, log} = DiskRaftLog.open(log)

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
      result = DiskRaftLog.walk_chain_inclusive(log, {1, 1}, {1, 3})
      expected = [{{1, 1}, :data1}, {{1, 2}, :data2}, {{1, 3}, :data3}]
      assert result == expected

      # Walk partial chain
      result = DiskRaftLog.walk_chain_inclusive(log, {1, 2}, {1, 3})
      expected = [{{1, 2}, :data2}, {{1, 3}, :data3}]
      assert result == expected

      # Walk single element
      result = DiskRaftLog.walk_chain_inclusive(log, {1, 2}, {1, 2})
      expected = [{{1, 2}, :data2}]
      assert result == expected

      # Walk beyond range (should stop at boundary)
      result = DiskRaftLog.walk_chain_inclusive(log, {1, 1}, {1, 2})
      expected = [{{1, 1}, :data1}, {{1, 2}, :data2}]
      assert result == expected

      DiskRaftLog.close(log)
    end
  end

  describe "DETS sync functionality" do
    test "sync returns :ok when table is open", %{tmp_dir: tmp_dir} do
      log = DiskRaftLog.new(log_dir: tmp_dir, table_name: :sync_test)
      {:ok, log} = DiskRaftLog.open(log)

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
