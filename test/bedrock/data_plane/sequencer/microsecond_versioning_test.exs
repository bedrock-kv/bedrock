defmodule Bedrock.DataPlane.Sequencer.MicrosecondVersioningTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
  end

  # Helper function to start sequencer server with common configuration
  defp start_sequencer(initial_version, epoch \\ 1, otp_name \\ :test_sequencer) do
    pid =
      start_supervised!(
        {Server,
         [
           cluster: TestCluster,
           director: self(),
           epoch: epoch,
           last_committed_version: initial_version,
           otp_name: otp_name
         ]}
      )

    {pid, epoch}
  end

  # Helper to get next commit version as integer
  defp next_commit_version_int({pid, epoch}) do
    {:ok, _read, commit} = GenServer.call(pid, {:next_commit_version, epoch})
    Version.to_integer(commit)
  end

  # Helper to get next read version as integer
  defp next_read_version_int({pid, epoch}) do
    {:ok, read} = GenServer.call(pid, {:next_read_version, epoch})
    Version.to_integer(read)
  end

  describe "microsecond-based versioning" do
    test "versions progress based on monotonic time" do
      # Start with a known baseline
      initial_version = Version.from_integer(1000)
      sequencer = start_sequencer(initial_version)

      # Get first version - should be >= baseline + 1 (time has passed since init)
      commit1_int = next_commit_version_int(sequencer)
      assert commit1_int >= 1001

      # Get second version - sequencer guarantees monotonically increasing versions
      commit2_int = next_commit_version_int(sequencer)
      assert commit2_int > commit1_int
    end

    test "versions always progress monotonically even with rapid requests" do
      initial_version = Version.from_integer(2000)
      sequencer = start_sequencer(initial_version)

      # Make rapid requests
      versions = Enum.map(1..10, fn _ -> next_commit_version_int(sequencer) end)

      # All versions should be unique and increasing
      assert versions == Enum.sort(versions)
      assert Enum.uniq(versions) == versions

      # Each version should advance by at least 1
      version_pairs = Enum.zip(versions, Enum.drop(versions, 1))
      Enum.each(version_pairs, fn {v1, v2} -> assert v2 > v1 end)
    end

    test "initialization preserves baseline from previous epoch" do
      # Simulate recovery with large version from previous epoch
      previous_epoch_version = Version.from_integer(1_000_000)
      sequencer = start_sequencer(previous_epoch_version, 2, :test_sequencer_epoch2)

      # First version should be >= previous epoch version + 1
      commit_int = next_commit_version_int(sequencer)
      assert commit_int >= 1_000_001
    end

    test "read versions track committed versions correctly" do
      initial_version = Version.from_integer(5000)
      {pid, epoch} = start_sequencer(initial_version)

      # Initial read version should be the baseline
      assert next_read_version_int({pid, epoch}) == 5000

      # Get a commit version
      assert {:ok, _last, commit_v1} = GenServer.call(pid, {:next_commit_version, epoch})

      # Read version should still be baseline (commit not reported yet)
      assert next_read_version_int({pid, epoch}) == 5000

      # Report the commit
      assert :ok = GenServer.call(pid, {:report_successful_commit, epoch, commit_v1})

      # Now read version should advance to match the committed version
      assert next_read_version_int({pid, epoch}) == Version.to_integer(commit_v1)
    end
  end
end
