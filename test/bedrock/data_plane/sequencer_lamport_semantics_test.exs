defmodule Bedrock.DataPlane.SequencerLamportSemanticsTest do
  @moduledoc """
  Tests focused on Lamport clock semantics and version chain properties.

  These tests complement the existing server tests by focusing specifically on
  the correctness of Lamport clock behavior and edge cases that could cause
  causality violations in distributed MVCC.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Version

  describe "Lamport clock chain semantics" do
    test "consecutive version assignments form proper chains" do
      commit0 = Version.from_integer(100)

      {:ok, sequencer} = start_sequencer(commit0)

      # Get three consecutive version assignments
      {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer)
      {:ok, ^commit1, commit2} = Sequencer.next_commit_version(sequencer)
      {:ok, ^commit2, commit3} = Sequencer.next_commit_version(sequencer)

      # Lamport clock property: each assignment advances the logical clock
      assert commit0 < commit1
      assert commit1 < commit2
      assert commit2 < commit3
    end

    test "version gaps don't break causality chains" do
      commit0 = Version.from_integer(200)
      {:ok, sequencer} = start_sequencer(commit0)

      # Assign versions 201, 202, 203 forming proper chains
      {:ok, ^commit0, v1} = Sequencer.next_commit_version(sequencer)
      {:ok, ^v1, v2} = Sequencer.next_commit_version(sequencer)
      {:ok, ^v2, v3} = Sequencer.next_commit_version(sequencer)

      # Simulate partial failures - only report v1 and v3 (gap at v2)
      :ok = Sequencer.report_successful_commit(sequencer, v1)
      :ok = Sequencer.report_successful_commit(sequencer, v3)

      # Readable horizon should advance to highest committed version
      {:ok, read_version} = Sequencer.next_read_version(sequencer)
      assert read_version == v3

      # New assignment should still maintain proper chain from last assigned
      {:ok, ^v3, next_commit} = Sequencer.next_commit_version(sequencer)
      # next in sequence should be > v3
      assert Version.to_integer(next_commit) > Version.to_integer(v3)

      # Late-arriving commit for gap shouldn't affect read version (monotonic)
      :ok = Sequencer.report_successful_commit(sequencer, v2)
      {:ok, final_read} = Sequencer.next_read_version(sequencer)
      # unchanged due to monotonic property (v3 is still highest)
      assert final_read == v3
    end

    test "read version isolation from assignment counters" do
      commit0 = Version.from_integer(300)
      {:ok, sequencer} = start_sequencer(commit0)

      # Initial read version matches initialization
      {:ok, ^commit0} = Sequencer.next_read_version(sequencer)

      # Assign many versions but don't report any commits
      versions =
        for _i <- 1..10 do
          {:ok, _last, commit} = Sequencer.next_commit_version(sequencer)
          commit
        end

      # All versions should be unique and > commit0
      version_ints = Enum.map(versions, &Version.to_integer/1)
      assert length(Enum.uniq(version_ints)) == length(version_ints)
      assert Enum.all?(version_ints, &(&1 > Version.to_integer(commit0)))

      # Should be in increasing order
      assert version_ints == Enum.sort(version_ints)

      # Read version should be unchanged (no commits reported)
      {:ok, ^commit0} = Sequencer.next_read_version(sequencer)

      # Report only some of the actually assigned commits
      # 5th version assigned
      reported_version1 = Enum.at(versions, 4)
      # 7th version assigned
      reported_version2 = Enum.at(versions, 6)
      :ok = Sequencer.report_successful_commit(sequencer, reported_version1)
      :ok = Sequencer.report_successful_commit(sequencer, reported_version2)

      # Read version advances to highest reported
      {:ok, ^reported_version2} = Sequencer.next_read_version(sequencer)

      # But assignment counter continues from where it left off
      {:ok, read_before_next, next_commit} = Sequencer.next_commit_version(sequencer)
      # Should reflect the latest reported version (or higher due to microsecond progression)
      assert Version.to_integer(read_before_next) >= Version.to_integer(reported_version2)
      # Next version should be > any previously assigned versions
      last_version_int = Enum.max(Enum.map(versions, &Version.to_integer/1))
      assert Version.to_integer(next_commit) > last_version_int
    end

    test "concurrent assignment preserves causality ordering" do
      commit0 = Version.from_integer(400)
      {:ok, sequencer} = start_sequencer(commit0)

      # Simulate high concurrency - many tasks getting versions simultaneously
      num_tasks = 50

      tasks =
        for i <- 1..num_tasks do
          Task.async(fn ->
            {:ok, last_commit, commit_version} = Sequencer.next_commit_version(sequencer)
            # Each task should see a causally consistent view
            {i, last_commit, commit_version}
          end)
        end

      results = Task.await_many(tasks, 5000)

      # Extract version pairs
      version_pairs = Enum.map(results, fn {_task, last, commit} -> {last, commit} end)
      commit_versions = Enum.map(version_pairs, &elem(&1, 1))

      # All commit versions should be unique and > commit0
      commit_ints = Enum.map(commit_versions, &Version.to_integer/1)
      assert length(commit_versions) == num_tasks
      assert length(Enum.uniq(commit_ints)) == num_tasks
      assert Enum.all?(commit_ints, &(&1 > Version.to_integer(commit0)))

      # Verify causality chain properties
      sorted_pairs = Enum.sort_by(version_pairs, &elem(&1, 1))

      for {{last1, commit1}, {last2, commit2}} <- Enum.zip(sorted_pairs, tl(sorted_pairs)) do
        # Each pair should form a valid Lamport clock
        assert last1 < commit1
        assert last2 < commit2
        # Later assignments should have higher last_commit values
        assert last1 <= last2
        # Commit versions should be strictly increasing
        assert commit1 < commit2
      end
    end
  end

  describe "edge cases and error conditions" do
    test "massive version numbers don't break invariants" do
      # Test with very large version numbers (near integer limits)
      commit0 = Version.from_integer(999_999_999_999)
      {:ok, sequencer} = start_sequencer(commit0)

      {:ok, ^commit0} = Sequencer.next_read_version(sequencer)

      {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer)
      assert Version.to_integer(commit1) > Version.to_integer(commit0)

      # Report commit and verify monotonic advancement
      :ok = Sequencer.report_successful_commit(sequencer, commit1)
      {:ok, ^commit1} = Sequencer.next_read_version(sequencer)
    end

    test "duplicate commit reports are idempotent" do
      commit0 = Version.from_integer(500)
      {:ok, sequencer} = start_sequencer(commit0)

      {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer)

      # Report the same commit multiple times
      :ok = Sequencer.report_successful_commit(sequencer, commit1)
      :ok = Sequencer.report_successful_commit(sequencer, commit1)
      :ok = Sequencer.report_successful_commit(sequencer, commit1)

      # Should have no ill effects
      {:ok, ^commit1} = Sequencer.next_read_version(sequencer)

      # Next assignment should be unaffected
      {:ok, ^commit1, commit2} = Sequencer.next_commit_version(sequencer)
      assert Version.to_integer(commit2) > Version.to_integer(commit1)
    end
  end

  # Helper function to start a sequencer with given initial version
  defp start_sequencer(initial_version) do
    GenServer.start_link(
      Bedrock.DataPlane.Sequencer.Server,
      {self(), 1, initial_version}
    )
  end
end
