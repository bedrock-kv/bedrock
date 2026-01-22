defmodule Bedrock.DataPlane.Sequencer.CommitIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
  end

  @epoch 1

  # Helper to start a sequencer with common configuration
  defp start_test_sequencer(initial_version, otp_name) do
    start_supervised!(
      {Server,
       [
         cluster: TestCluster,
         director: self(),
         epoch: @epoch,
         last_committed_version: initial_version,
         otp_name: otp_name
       ]}
    )
  end

  describe "sequencer and commit proxy integration" do
    test "complete flow: version assignment -> commit notification -> read version update" do
      initial_version = Version.from_integer(100)
      sequencer_pid = start_test_sequencer(initial_version, :test_sequencer_1)

      # 1. Get initial read version
      assert {:ok, ^initial_version} = Sequencer.next_read_version(sequencer_pid, @epoch)

      # 2. Assign commit versions (simulate commit proxy getting versions)
      assert {:ok, ^initial_version, commit_v1} = Sequencer.next_commit_version(sequencer_pid, @epoch)
      assert commit_v1 > initial_version

      assert {:ok, ^commit_v1, commit_v2} = Sequencer.next_commit_version(sequencer_pid, @epoch)
      assert commit_v2 > commit_v1

      # 3. Verify read version is still old
      assert {:ok, ^initial_version} = Sequencer.next_read_version(sequencer_pid, @epoch)

      # 4. Simulate commit proxy notifying sequencer of successful commit
      assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, commit_v1)

      # 5. Verify read version updated
      assert {:ok, ^commit_v1} = Sequencer.next_read_version(sequencer_pid, @epoch)

      # 6. Report second commit
      assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, commit_v2)

      # 7. Verify read version updated again
      assert {:ok, ^commit_v2} = Sequencer.next_read_version(sequencer_pid, @epoch)

      # 8. Get another commit version to verify assignment counter advanced
      assert {:ok, ^commit_v2, next_commit} = Sequencer.next_commit_version(sequencer_pid, @epoch)
      assert next_commit > commit_v2
    end

    test "out-of-order commit notifications handled correctly" do
      initial_version = Version.from_integer(200)
      sequencer_pid = start_test_sequencer(initial_version, :test_sequencer_2)

      # Assign three versions
      assert {:ok, _, v1} = Sequencer.next_commit_version(sequencer_pid, @epoch)
      assert {:ok, _, v2} = Sequencer.next_commit_version(sequencer_pid, @epoch)
      assert {:ok, _, v3} = Sequencer.next_commit_version(sequencer_pid, @epoch)

      # Versions should be monotonically increasing
      assert v1 > initial_version
      assert v2 > v1
      assert v3 > v2

      # Report commits out of order: v2, v3, v1
      assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, v2)
      assert {:ok, ^v2} = Sequencer.next_read_version(sequencer_pid, @epoch)

      assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, v3)
      assert {:ok, ^v3} = Sequencer.next_read_version(sequencer_pid, @epoch)

      # Reporting older version shouldn't decrease read_version
      assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, v1)
      assert {:ok, ^v3} = Sequencer.next_read_version(sequencer_pid, @epoch)
    end

    test "version invariants maintained under concurrent operations" do
      initial_version = Version.from_integer(300)
      sequencer_pid = start_test_sequencer(initial_version, :test_sequencer_3)

      # Simulate multiple commit proxies getting versions concurrently
      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            assert {:ok, read_v, commit_v} = Sequencer.next_commit_version(sequencer_pid, @epoch)
            # Verify invariant: read_version <= commit_version
            assert read_v <= commit_v
            {read_v, commit_v}
          end)
        end

      results = Task.await_many(tasks)
      commit_versions = Enum.map(results, &elem(&1, 1))

      # All assigned versions should be unique and > initial version
      assert length(Enum.uniq(commit_versions)) == length(commit_versions)
      assert Enum.all?(commit_versions, &(&1 > initial_version))

      # Report all commits
      for {_, commit_v} <- results do
        assert :ok = Sequencer.report_successful_commit(sequencer_pid, @epoch, commit_v)
      end

      # Final read version should be the highest commit version
      max_commit_version = Enum.max(commit_versions)
      assert {:ok, ^max_commit_version} = Sequencer.next_read_version(sequencer_pid, @epoch)
    end
  end
end
