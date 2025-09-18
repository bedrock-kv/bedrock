defmodule Bedrock.DataPlane.Sequencer.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Sequencer.State
  alias Bedrock.DataPlane.Version

  # Helper function to create a test state with customizable fields
  defp create_state(overrides \\ []) do
    defaults = [
      director: self(),
      epoch: 1,
      next_commit_version_int: 101,
      last_commit_version_int: 100,
      known_committed_version_int: 100,
      epoch_baseline_version_int: 100,
      epoch_start_monotonic_us: System.monotonic_time(:microsecond)
    ]

    struct(State, Keyword.merge(defaults, overrides))
  end

  describe "sequencer version tracking" do
    test "initialization sets all three version counters correctly" do
      initial_version = Version.from_integer(100)
      {:ok, state} = Server.init({self(), 1, initial_version})

      assert %State{
               next_commit_version_int: 101,
               last_commit_version_int: 100,
               known_committed_version_int: 100,
               epoch_baseline_version_int: 100,
               epoch: 1,
               director: director,
               epoch_start_monotonic_us: epoch_start
             } = state

      assert director == self()
      assert is_integer(epoch_start)
    end

    test "next_commit_version forms proper Lamport clock chain" do
      initial_state =
        create_state(
          known_committed_version_int: 95,
          epoch_baseline_version_int: 90
        )

      {:reply, {:ok, last_commit, commit_version}, new_state} =
        Server.handle_call(:next_commit_version, self(), initial_state)

      # Lamport clock chain: returns previous last_commit_version and new version
      assert last_commit == Version.from_integer(100)
      # With microsecond-based versioning, commit_version should be >= 101
      assert Version.to_integer(commit_version) >= 101

      # Verify state changes with pattern matching
      assert %State{
               next_commit_version_int: next_commit,
               last_commit_version_int: last_commit_int,
               known_committed_version_int: 95
             } = new_state

      # assignment counter incremented (by at least 1)
      assert next_commit >= 102
      # last_commit_version updated to what we just assigned
      assert last_commit_int >= 101
    end

    test "next_read_version returns current known committed version" do
      state =
        create_state(
          next_commit_version_int: 105,
          last_commit_version_int: 104,
          known_committed_version_int: 103
        )

      {:reply, {:ok, version}, ^state} =
        Server.handle_call(:next_read_version, self(), state)

      # returns known_committed_version, not assigned or next versions
      assert version == Version.from_integer(103)
    end

    test "report_successful_commit updates known committed version monotonically" do
      initial_state =
        create_state(
          next_commit_version_int: 105,
          last_commit_version_int: 104,
          epoch_baseline_version_int: 90
        )

      # Report commit version 103
      {:reply, :ok, state1} =
        Server.handle_call({:report_successful_commit, Version.from_integer(103)}, self(), initial_state)

      assert %State{
               known_committed_version_int: 103,
               next_commit_version_int: 105,
               last_commit_version_int: 104
             } = state1

      # Report older commit version 102 - should not decrease known_committed_version
      {:reply, :ok, state2} =
        Server.handle_call({:report_successful_commit, Version.from_integer(102)}, self(), state1)

      # unchanged (monotonic)
      assert %State{known_committed_version_int: 103} = state2

      # Report newer commit version 104
      {:reply, :ok, state3} =
        Server.handle_call({:report_successful_commit, Version.from_integer(104)}, self(), state2)

      # updated
      assert %State{known_committed_version_int: 104} = state3
    end

    test "version invariants maintained" do
      # Start with properly initialized state
      state = create_state()

      # Assign several versions
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)

      assert %State{
               next_commit_version_int: next_commit,
               last_commit_version_int: last_commit,
               known_committed_version_int: 100
             } = state

      assert next_commit >= 104
      assert last_commit >= 103
      # invariants
      assert 100 <= last_commit
      assert last_commit < next_commit

      # Report some commits (out of order)
      {:reply, :ok, state} =
        Server.handle_call({:report_successful_commit, Version.from_integer(102)}, self(), state)

      {:reply, :ok, state} =
        Server.handle_call({:report_successful_commit, Version.from_integer(101)}, self(), state)

      assert %State{
               next_commit_version_int: next_commit,
               last_commit_version_int: last_commit,
               known_committed_version_int: 102
             } = state

      assert next_commit >= 104
      assert last_commit >= 103
      # invariants maintained
      assert 102 <= last_commit
      assert last_commit < next_commit
    end
  end
end
