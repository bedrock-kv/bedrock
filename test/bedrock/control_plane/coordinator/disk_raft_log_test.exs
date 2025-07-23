defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLogTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  describe "basic disk_log functionality" do
    test "can create and open a disk log", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "test.log") |> String.to_charlist()

      # Test that we can open a new disk_log
      assert {:ok, :test_log} =
               :disk_log.open([
                 {:name, :test_log},
                 {:file, log_file},
                 {:type, :halt},
                 {:format, :internal},
                 {:quiet, true}
               ])

      # Test that we can write to it
      assert :ok = :disk_log.log(:test_log, {{1, 1}, 1, :test_data})
      assert :ok = :disk_log.sync(:test_log)

      # Test that we can read from it
      assert {_, [{{1, 1}, 1, :test_data}]} = :disk_log.chunk(:test_log, :start)

      assert :ok = :disk_log.close(:test_log)
    end

    test "survives crash and recovery", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "recovery.log") |> String.to_charlist()

      # Create log and write data
      {:ok, :recovery_test} =
        :disk_log.open([
          {:name, :recovery_test},
          {:file, log_file},
          {:type, :halt},
          {:format, :internal}
        ])

      :disk_log.log(:recovery_test, {{1, 1}, 1, :before_crash})
      :disk_log.log(:recovery_test, {{1, 2}, 1, :also_before_crash})
      :disk_log.sync(:recovery_test)

      # Simulate crash (close without proper shutdown)
      :disk_log.close(:recovery_test)

      # Reopen and verify data survived
      {:ok, :recovery_test} =
        :disk_log.open([
          {:name, :recovery_test},
          {:file, log_file}
        ])

      # Read all entries
      entries = read_all_entries(:recovery_test)
      assert length(entries) == 2
      assert {{1, 1}, 1, :before_crash} in entries
      assert {{1, 2}, 1, :also_before_crash} in entries

      :disk_log.close(:recovery_test)
    end

    test "handles sequential access patterns", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "sequential.log") |> String.to_charlist()

      {:ok, :seq_test} =
        :disk_log.open([
          {:name, :seq_test},
          {:file, log_file},
          {:type, :halt},
          {:format, :internal}
        ])

      # Write a sequence of entries
      entries =
        for i <- 1..10 do
          entry = {{1, i}, 1, {:entry, i}}
          :disk_log.log(:seq_test, entry)
          entry
        end

      :disk_log.sync(:seq_test)

      # Read them back sequentially
      read_entries = read_all_entries(:seq_test)
      assert read_entries == entries

      :disk_log.close(:seq_test)
    end
  end

  describe "raft-specific transaction ID patterns" do
    test "can store and retrieve raft-style transaction IDs", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "raft_ids.log") |> String.to_charlist()

      {:ok, :raft_test} =
        :disk_log.open([
          {:name, :raft_test},
          {:file, log_file},
          {:type, :halt},
          {:format, :internal}
        ])

      # Test various raft transaction patterns
      raft_entries = [
        # Initial/genesis entry
        {{0, 0}, 0, :initial},
        # No-op entry
        {{1, 1}, 1, :noop},
        # Config change
        {{1, 2}, 1, {:config, :add_node}},
        # User data
        {{1, 3}, 1, {:user_data, "test"}},
        # New term
        {{2, 4}, 2, {:term_change}},
        # More user data
        {{2, 5}, 2, {:user_data, "more"}}
      ]

      # Write all entries
      Enum.each(raft_entries, fn entry ->
        assert :ok = :disk_log.log(:raft_test, entry)
      end)

      :disk_log.sync(:raft_test)

      # Read back and verify
      read_entries = read_all_entries(:raft_test)
      assert read_entries == raft_entries

      :disk_log.close(:raft_test)
    end

    test "supports transaction ID ordering", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "ordering.log") |> String.to_charlist()

      {:ok, :order_test} =
        :disk_log.open([
          {:name, :order_test},
          {:file, log_file},
          {:type, :halt},
          {:format, :internal}
        ])

      # Write entries with different terms
      entries = [
        {{1, 1}, 1, :term1_entry1},
        {{1, 2}, 1, :term1_entry2},
        # Term change
        {{2, 3}, 2, :term2_entry1},
        {{2, 4}, 2, :term2_entry2},
        {{2, 5}, 2, :term2_entry3}
      ]

      Enum.each(entries, &:disk_log.log(:order_test, &1))
      :disk_log.sync(:order_test)

      # Verify ordering is preserved
      read_entries = read_all_entries(:order_test)
      assert read_entries == entries

      # Verify we can extract transaction IDs
      transaction_ids = Enum.map(read_entries, fn {id, _term, _data} -> id end)
      expected_ids = [{1, 1}, {1, 2}, {2, 3}, {2, 4}, {2, 5}]
      assert transaction_ids == expected_ids

      :disk_log.close(:order_test)
    end
  end

  describe "error handling and edge cases" do
    test "handles empty logs gracefully", %{tmp_dir: tmp_dir} do
      log_file = Path.join(tmp_dir, "empty.log") |> String.to_charlist()

      {:ok, :empty_test} =
        :disk_log.open([
          {:name, :empty_test},
          {:file, log_file},
          {:type, :halt},
          {:format, :internal}
        ])

      # Try to read from empty log
      assert :eof = :disk_log.chunk(:empty_test, :start)

      :disk_log.close(:empty_test)
    end

    test "handles file permissions and disk errors" do
      # Test opening a log in a non-existent directory
      bad_log_file = ~c"/non/existent/path/test.log"

      assert {:error, _reason} =
               :disk_log.open([
                 {:name, :bad_test},
                 {:file, bad_log_file},
                 {:type, :halt},
                 {:format, :internal}
               ])
    end
  end

  # Helper function to read all entries from a disk_log
  defp read_all_entries(log_name) do
    read_entries_recursive(log_name, :start, [])
  end

  defp read_entries_recursive(log_name, continuation, acc) do
    case :disk_log.chunk(log_name, continuation) do
      :eof ->
        Enum.reverse(acc)

      {:error, reason} ->
        flunk("Error reading from disk_log: #{inspect(reason)}")

      {next_continuation, terms} ->
        read_entries_recursive(log_name, next_continuation, Enum.reverse(terms) ++ acc)
    end
  end
end
