defmodule Bedrock.DataPlane.SequencerMicrosecondVersioningTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Version

  describe "microsecond-based versioning" do
    test "versions progress based on monotonic time" do
      # Start with a known baseline
      initial_version = Version.from_integer(1000)

      pid =
        start_supervised!(
          {Server,
           [
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer
           ]}
        )

      # Get first version
      {:ok, _read1, commit1} = GenServer.call(pid, :next_commit_version)
      commit1_int = Version.to_integer(commit1)

      # Version should be >= baseline + 1 (time has passed since init)
      assert commit1_int >= 1001

      # Wait a bit to ensure time advances
      Process.sleep(1)

      # Get second version
      {:ok, _read2, commit2} = GenServer.call(pid, :next_commit_version)
      commit2_int = Version.to_integer(commit2)

      # Second version should be larger (time advanced)
      assert commit2_int > commit1_int

      GenServer.stop(pid)
    end

    test "versions always progress monotonically even with rapid requests" do
      initial_version = Version.from_integer(2000)

      pid =
        start_supervised!(
          {Server,
           [
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer
           ]}
        )

      # Make rapid requests
      versions =
        Enum.map(1..10, fn _ ->
          {:ok, _read, commit} = GenServer.call(pid, :next_commit_version)
          Version.to_integer(commit)
        end)

      # All versions should be unique and increasing
      assert versions == Enum.sort(versions)
      assert Enum.uniq(versions) == versions

      # Each version should advance by at least 1
      version_pairs = Enum.zip(versions, Enum.drop(versions, 1))

      Enum.each(version_pairs, fn {v1, v2} ->
        assert v2 > v1
      end)

      GenServer.stop(pid)
    end

    test "initialization preserves baseline from previous epoch" do
      # Simulate recovery with large version from previous epoch
      previous_epoch_version = Version.from_integer(1_000_000)

      pid =
        start_supervised!(
          {Server,
           [
             director: self(),
             epoch: 2,
             last_committed_version: previous_epoch_version,
             otp_name: :test_sequencer_epoch2
           ]}
        )

      # First version should be >= previous epoch version
      {:ok, _read, commit} = GenServer.call(pid, :next_commit_version)
      commit_int = Version.to_integer(commit)

      assert commit_int >= 1_000_001

      GenServer.stop(pid)
    end

    test "read versions track committed versions correctly" do
      initial_version = Version.from_integer(5000)

      pid =
        start_supervised!(
          {Server,
           [
             director: self(),
             epoch: 1,
             last_committed_version: initial_version,
             otp_name: :test_sequencer
           ]}
        )

      # Initial read version should be the baseline
      {:ok, read_v1} = GenServer.call(pid, :next_read_version)
      assert Version.to_integer(read_v1) == 5000

      # Get a commit version
      {:ok, _last, commit_v1} = GenServer.call(pid, :next_commit_version)

      # Read version should still be baseline (commit not reported yet)
      {:ok, read_v2} = GenServer.call(pid, :next_read_version)
      assert Version.to_integer(read_v2) == 5000

      # Report the commit
      GenServer.cast(pid, {:report_successful_commit, commit_v1})

      # Now read version should advance
      {:ok, read_v3} = GenServer.call(pid, :next_read_version)
      assert Version.to_integer(read_v3) == Version.to_integer(commit_v1)

      GenServer.stop(pid)
    end
  end
end
