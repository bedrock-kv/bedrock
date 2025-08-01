defmodule Bedrock.DataPlane.Sequencer.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Sequencer.State

  describe "sequencer version tracking" do
    test "initialization sets all three version counters correctly" do
      {:ok, state} = Server.init({self(), 1, 100})

      assert state.next_commit_version == 101
      assert state.last_commit_version == 100
      assert state.known_committed_version == 100
      assert state.epoch == 1
      assert state.director == self()
    end

    test "next_commit_version forms proper Lamport clock chain" do
      initial_state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 101,
        last_commit_version: 100,
        known_committed_version: 95
      }

      {:reply, {:ok, last_commit, commit_version}, new_state} =
        Server.handle_call(:next_commit_version, self(), initial_state)

      # Lamport clock chain: returns previous last_commit_version and new version
      assert last_commit == 100
      assert commit_version == 101
      # assignment counter incremented
      assert new_state.next_commit_version == 102
      # last_commit_version updated to what we just assigned
      assert new_state.last_commit_version == 101
      # known_committed_version unchanged (Commit Proxy updates this)
      assert new_state.known_committed_version == 95
    end

    test "next_read_version returns current known committed version" do
      state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 105,
        last_commit_version: 104,
        known_committed_version: 103
      }

      {:reply, {:ok, version}, ^state} =
        Server.handle_call(:next_read_version, self(), state)

      # returns known_committed_version, not assigned or next versions
      assert version == 103
    end

    test "report_successful_commit updates known committed version monotonically" do
      initial_state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 105,
        last_commit_version: 104,
        known_committed_version: 100
      }

      # Report commit version 103
      {:noreply, state1} =
        Server.handle_cast({:report_successful_commit, 103}, initial_state)

      assert state1.known_committed_version == 103
      # unchanged
      assert state1.next_commit_version == 105
      assert state1.last_commit_version == 104

      # Report older commit version 102 - should not decrease known_committed_version
      {:noreply, state2} =
        Server.handle_cast({:report_successful_commit, 102}, state1)

      # unchanged (monotonic)
      assert state2.known_committed_version == 103

      # Report newer commit version 104
      {:noreply, state3} =
        Server.handle_cast({:report_successful_commit, 104}, state2)

      # updated
      assert state3.known_committed_version == 104
    end

    test "version invariants maintained" do
      # Start with properly initialized state
      state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 101,
        last_commit_version: 100,
        known_committed_version: 100
      }

      # Assign several versions
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)

      assert state.next_commit_version == 104
      assert state.last_commit_version == 103
      assert state.known_committed_version == 100
      # invariants
      assert state.known_committed_version <= state.last_commit_version
      assert state.last_commit_version < state.next_commit_version

      # Report some commits (out of order)
      {:noreply, state} = Server.handle_cast({:report_successful_commit, 102}, state)
      {:noreply, state} = Server.handle_cast({:report_successful_commit, 101}, state)

      assert state.next_commit_version == 104
      assert state.last_commit_version == 103
      assert state.known_committed_version == 102
      # invariants maintained
      assert state.known_committed_version <= state.last_commit_version
      assert state.last_commit_version < state.next_commit_version
    end
  end
end
