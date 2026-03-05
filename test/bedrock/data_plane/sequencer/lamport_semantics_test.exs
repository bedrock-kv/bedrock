defmodule Bedrock.DataPlane.Sequencer.LamportSemanticsTest do
  @moduledoc """
  Tests focused on Lamport clock semantics and version chain properties.

  These tests complement the existing server tests by focusing specifically on
  the correctness of Lamport clock behavior and edge cases that could cause
  causality violations in distributed MVCC.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
  end

  @epoch 1

  # Helper to start a sequencer with given initial version
  defp start_sequencer(initial_version) do
    otp_name = :"test_sequencer_#{:erlang.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {Bedrock.DataPlane.Sequencer.Server,
         [
           cluster: TestCluster,
           director: self(),
           epoch: @epoch,
           last_committed_version: initial_version,
           otp_name: otp_name
         ]}
      )

    {:ok, pid}
  end

  # Helper to assign multiple versions without reporting commits
  defp assign_versions(sequencer, count) do
    for _i <- 1..count do
      {:ok, _last, commit} = Sequencer.next_commit_version(sequencer, @epoch)
      commit
    end
  end

  describe "Lamport clock chain semantics" do
    test "consecutive version assignments form proper chains" do
      commit0 = Version.from_integer(100)
      {:ok, sequencer} = start_sequencer(commit0)

      # Get three consecutive version assignments with chained pattern matching
      assert {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer, @epoch)
      assert {:ok, ^commit1, commit2} = Sequencer.next_commit_version(sequencer, @epoch)
      assert {:ok, ^commit2, commit3} = Sequencer.next_commit_version(sequencer, @epoch)

      # Verify Lamport clock property: each assignment advances the logical clock
      assert commit0 < commit1 < commit2 < commit3
    end

    test "version gaps don't break causality chains" do
      commit0 = Version.from_integer(200)
      {:ok, sequencer} = start_sequencer(commit0)

      # Assign versions 201, 202, 203 forming proper chains
      assert {:ok, ^commit0, v1} = Sequencer.next_commit_version(sequencer, @epoch)
      assert {:ok, ^v1, v2} = Sequencer.next_commit_version(sequencer, @epoch)
      assert {:ok, ^v2, v3} = Sequencer.next_commit_version(sequencer, @epoch)

      # Simulate partial failures - only report v1 and v3 (gap at v2)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, v1)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, v3)

      # Readable horizon should advance to highest committed version
      assert {:ok, ^v3} = Sequencer.next_read_version(sequencer, @epoch)

      # New assignment should maintain proper chain from last assigned
      assert {:ok, ^v3, next_commit} = Sequencer.next_commit_version(sequencer, @epoch)
      assert Version.to_integer(next_commit) > Version.to_integer(v3)

      # Late-arriving commit for gap shouldn't affect read version (monotonic)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, v2)
      assert {:ok, ^v3} = Sequencer.next_read_version(sequencer, @epoch)
    end

    test "read version isolation from assignment counters" do
      commit0 = Version.from_integer(300)
      {:ok, sequencer} = start_sequencer(commit0)

      # Initial read version matches initialization
      assert {:ok, ^commit0} = Sequencer.next_read_version(sequencer, @epoch)

      # Assign many versions but don't report any commits
      versions = assign_versions(sequencer, 10)
      version_ints = Enum.map(versions, &Version.to_integer/1)
      commit0_int = Version.to_integer(commit0)

      # All versions should be unique, > commit0, and in increasing order
      assert length(Enum.uniq(version_ints)) == length(version_ints)
      assert Enum.all?(version_ints, &(&1 > commit0_int))
      assert version_ints == Enum.sort(version_ints)

      # Read version should be unchanged (no commits reported)
      assert {:ok, ^commit0} = Sequencer.next_read_version(sequencer, @epoch)

      # Report only some of the assigned commits (5th and 7th)
      reported_version1 = Enum.at(versions, 4)
      reported_version2 = Enum.at(versions, 6)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, reported_version1)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, reported_version2)

      # Read version advances to highest reported
      assert {:ok, ^reported_version2} = Sequencer.next_read_version(sequencer, @epoch)

      # Assignment counter continues from where it left off
      assert {:ok, read_before_next, next_commit} = Sequencer.next_commit_version(sequencer, @epoch)
      assert Version.to_integer(read_before_next) >= Version.to_integer(reported_version2)

      last_version_int = Enum.max(version_ints)
      assert Version.to_integer(next_commit) > last_version_int
    end

    test "concurrent assignment preserves causality ordering" do
      commit0 = Version.from_integer(400)
      {:ok, sequencer} = start_sequencer(commit0)
      num_tasks = 50

      # Simulate high concurrency - many tasks getting versions simultaneously
      tasks =
        for i <- 1..num_tasks do
          Task.async(fn ->
            assert {:ok, last_commit, commit_version} = Sequencer.next_commit_version(sequencer, @epoch)
            {i, last_commit, commit_version}
          end)
        end

      results = Task.await_many(tasks, 5000)
      version_pairs = Enum.map(results, fn {_task, last, commit} -> {last, commit} end)
      commit_versions = Enum.map(version_pairs, &elem(&1, 1))
      commit_ints = Enum.map(commit_versions, &Version.to_integer/1)
      commit0_int = Version.to_integer(commit0)

      # All commit versions should be unique and > commit0
      assert length(commit_versions) == num_tasks
      assert length(Enum.uniq(commit_ints)) == num_tasks
      assert Enum.all?(commit_ints, &(&1 > commit0_int))

      # Verify causality chain properties for adjacent pairs
      sorted_pairs = Enum.sort_by(version_pairs, &elem(&1, 1))
      adjacent_pairs = Enum.zip(sorted_pairs, tl(sorted_pairs))

      for {{last1, commit1}, {last2, commit2}} <- adjacent_pairs do
        # Each pair should form a valid Lamport clock and maintain ordering
        assert last1 < commit1 and last2 < commit2
        assert last1 <= last2 and commit1 < commit2
      end
    end
  end

  describe "edge cases and error conditions" do
    test "massive version numbers don't break invariants" do
      # Test with very large version numbers (near integer limits)
      commit0 = Version.from_integer(999_999_999_999)
      {:ok, sequencer} = start_sequencer(commit0)

      assert {:ok, ^commit0} = Sequencer.next_read_version(sequencer, @epoch)
      assert {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer, @epoch)
      assert Version.to_integer(commit1) > Version.to_integer(commit0)

      # Report commit and verify monotonic advancement
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, commit1)
      assert {:ok, ^commit1} = Sequencer.next_read_version(sequencer, @epoch)
    end

    test "duplicate commit reports are idempotent" do
      commit0 = Version.from_integer(500)
      {:ok, sequencer} = start_sequencer(commit0)

      assert {:ok, ^commit0, commit1} = Sequencer.next_commit_version(sequencer, @epoch)

      # Report the same commit multiple times
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, commit1)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, commit1)
      assert :ok = Sequencer.report_successful_commit(sequencer, @epoch, commit1)

      # Should have no ill effects on read version or next assignment
      assert {:ok, ^commit1} = Sequencer.next_read_version(sequencer, @epoch)
      assert {:ok, ^commit1, commit2} = Sequencer.next_commit_version(sequencer, @epoch)
      assert Version.to_integer(commit2) > Version.to_integer(commit1)
    end
  end
end
