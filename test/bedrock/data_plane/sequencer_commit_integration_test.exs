defmodule Bedrock.DataPlane.SequencerCommitIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer

  describe "sequencer and commit proxy integration" do
    test "complete flow: version assignment -> commit notification -> read version update" do
      # Start a sequencer process
      {:ok, sequencer_pid} =
        GenServer.start_link(
          Bedrock.DataPlane.Sequencer.Server,
          {self(), 1, 100}
        )

      # 1. Get initial read version
      {:ok, initial_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert initial_read_version == 100

      # 2. Assign some commit versions (simulate commit proxy getting versions)
      {:ok, read_v1, commit_v1} = Sequencer.next_commit_version(sequencer_pid)
      # read version hasn't changed
      assert read_v1 == 100
      # first assigned version
      assert commit_v1 == 101

      {:ok, read_v2, commit_v2} = Sequencer.next_commit_version(sequencer_pid)
      # last commit version updated from previous assignment
      assert read_v2 == 101
      # second assigned version
      assert commit_v2 == 102

      # 3. Verify read version is still old
      {:ok, current_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert current_read_version == 100

      # 4. Simulate commit proxy notifying sequencer of successful commit
      :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v1)

      # 5. Verify read version updated
      {:ok, updated_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert updated_read_version == 101

      # 6. Report second commit
      :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v2)

      # 7. Verify read version updated again
      {:ok, final_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert final_read_version == 102

      # 8. Get another commit version to verify assignment counter advanced
      {:ok, current_read, next_commit} = Sequencer.next_commit_version(sequencer_pid)
      # reflects latest committed
      assert current_read == 102
      # next available version
      assert next_commit == 103

      # Cleanup
      GenServer.stop(sequencer_pid)
    end

    test "out-of-order commit notifications handled correctly" do
      {:ok, sequencer_pid} =
        GenServer.start_link(
          Bedrock.DataPlane.Sequencer.Server,
          {self(), 1, 200}
        )

      # Assign versions 201, 202, 203
      {:ok, _, v1} = Sequencer.next_commit_version(sequencer_pid)
      {:ok, _, v2} = Sequencer.next_commit_version(sequencer_pid)
      {:ok, _, v3} = Sequencer.next_commit_version(sequencer_pid)

      assert v1 == 201
      assert v2 == 202
      assert v3 == 203

      # Report commits out of order: 202, 203, 201
      :ok = Sequencer.report_successful_commit(sequencer_pid, 202)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      assert read_version == 202

      :ok = Sequencer.report_successful_commit(sequencer_pid, 203)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      assert read_version == 203

      # Reporting older version shouldn't decrease read_version
      :ok = Sequencer.report_successful_commit(sequencer_pid, 201)
      {:ok, read_version} = Sequencer.next_read_version(sequencer_pid)
      # unchanged due to monotonic property
      assert read_version == 203

      GenServer.stop(sequencer_pid)
    end

    test "version invariants maintained under concurrent operations" do
      {:ok, sequencer_pid} =
        GenServer.start_link(
          Bedrock.DataPlane.Sequencer.Server,
          {self(), 1, 300}
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

      # All assigned versions should be unique and sequential
      commit_versions = Enum.map(results, &elem(&1, 1))
      assert Enum.sort(commit_versions) == Enum.to_list(301..310)

      # Report all commits
      for {_, commit_v} <- results do
        :ok = Sequencer.report_successful_commit(sequencer_pid, commit_v)
      end

      # Final read version should be the highest commit version
      {:ok, final_read_version} = Sequencer.next_read_version(sequencer_pid)
      assert final_read_version == 310

      GenServer.stop(sequencer_pid)
    end
  end
end
