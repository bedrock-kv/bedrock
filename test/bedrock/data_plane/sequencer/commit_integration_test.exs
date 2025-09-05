defmodule Bedrock.DataPlane.Sequencer.CommitIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
  end

  describe "sequencer and commit proxy integration" do
    test "complete flow: version assignment -> commit notification -> read version update" do
      # Start a sequencer process
      initial_version = Version.from_integer(100)

      sequencer_pid =
        start_supervised!(
          {Server,
           [
             cluster: TestCluster,
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer_1
           ]}
        )

      # 1. Get initial read version
      {:ok, initial_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert initial_read_version == initial_version

      # 2. Assign some commit versions (simulate commit proxy getting versions)
      {:ok, read_v1, commit_v1} = Sequencer.next_commit_version(sequencer_pid)
      # read version hasn't changed
      assert read_v1 == initial_version
      # first assigned version should be > initial version
      assert Version.to_integer(commit_v1) > Version.to_integer(initial_version)

      {:ok, read_v2, commit_v2} = Sequencer.next_commit_version(sequencer_pid)
      # last commit version updated from previous assignment
      assert read_v2 == commit_v1
      # second assigned version should be > first
      assert Version.to_integer(commit_v2) > Version.to_integer(commit_v1)

      # 3. Verify read version is still old
      {:ok, current_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert current_read_version == initial_version

      # 4. Simulate commit proxy notifying sequencer of successful commit
      :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v1)

      # 5. Verify read version updated
      {:ok, updated_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert updated_read_version == commit_v1

      # 6. Report second commit
      :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v2)

      # 7. Verify read version updated again
      {:ok, final_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert final_read_version == commit_v2

      # 8. Get another commit version to verify assignment counter advanced
      {:ok, current_read, next_commit} = Sequencer.next_commit_version(sequencer_pid)
      # reflects latest committed
      assert current_read == commit_v2
      # next available version should be > current
      assert Version.to_integer(next_commit) > Version.to_integer(commit_v2)

      # Process will be automatically stopped by start_supervised!
    end

    test "out-of-order commit notifications handled correctly" do
      initial_version = Version.from_integer(200)

      sequencer_pid =
        start_supervised!(
          {Server,
           [
             cluster: TestCluster,
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer_2
           ]}
        )

      # Assign versions 201, 202, 203
      {:ok, _, v1} = Sequencer.next_commit_version(sequencer_pid)
      {:ok, _, v2} = Sequencer.next_commit_version(sequencer_pid)
      {:ok, _, v3} = Sequencer.next_commit_version(sequencer_pid)

      # Versions should be monotonically increasing
      v1_int = Version.to_integer(v1)
      v2_int = Version.to_integer(v2)
      v3_int = Version.to_integer(v3)
      assert v1_int > Version.to_integer(initial_version)
      assert v2_int > v1_int
      assert v3_int > v2_int

      # Report commits out of order: v2, v3, v1
      :ok = Sequencer.report_successful_commit(sequencer_pid, v2)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      assert read_version == v2

      :ok = Sequencer.report_successful_commit(sequencer_pid, v3)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      assert read_version == v3

      # Reporting older version shouldn't decrease read_version
      :ok = Sequencer.report_successful_commit(sequencer_pid, v1)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      # unchanged due to monotonic property
      assert read_version == v3

      # Process will be automatically stopped by start_supervised!
    end

    test "version invariants maintained under concurrent operations" do
      initial_version = Version.from_integer(300)

      sequencer_pid =
        start_supervised!(
          {Server,
           [
             cluster: TestCluster,
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer_3
           ]}
        )

      # Simulate multiple commit proxies getting versions concurrently
      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            {:ok, read_v, commit_v} = Sequencer.next_commit_version(sequencer_pid)
            # Verify invariant: read_version <= commit_version
            assert read_v <= commit_v
            {read_v, commit_v}
          end)
        end

      results = Task.await_many(tasks)

      # All assigned versions should be unique and increasing
      commit_versions = Enum.map(results, &elem(&1, 1))
      commit_ints = Enum.map(commit_versions, &Version.to_integer/1)

      # Should be unique
      assert length(Enum.uniq(commit_ints)) == length(commit_ints)

      # Should all be > initial version
      assert Enum.all?(commit_ints, &(&1 > Version.to_integer(initial_version)))

      # Report all commits
      for {_, commit_v} <- results do
        :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v)
      end

      # Final read version should be the highest commit version
      {:ok, final_read_version} = Sequencer.next_read_version(sequencer_pid)
      max_commit_version = Enum.max_by(commit_versions, &Version.to_integer/1)
      assert final_read_version == max_commit_version

      # Process will be automatically stopped by start_supervised!
    end
  end
end
