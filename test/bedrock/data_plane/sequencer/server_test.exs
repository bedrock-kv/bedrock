defmodule Bedrock.DataPlane.Sequencer.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Sequencer.State

  describe "sequencer version tracking" do
    test "initialization sets both counters to same value" do
      {:ok, state} = Server.init({self(), 1, 100})

      assert state.next_commit_version == 100
      assert state.read_version == 100
      assert state.epoch == 1
      assert state.director == self()
    end

    test "next_commit_version increments assignment counter" do
      initial_state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 100,
        read_version: 100
      }

      {:reply, {:ok, read_version, commit_version}, new_state} =
        Server.handle_call(:next_commit_version, self(), initial_state)

      # read_version unchanged
      assert read_version == 100
      # next assigned version  
      assert commit_version == 101
      # assignment counter incremented
      assert new_state.next_commit_version == 101
      # read version unchanged
      assert new_state.read_version == 100
    end

    test "next_read_version returns current read version" do
      state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 105,
        read_version: 103
      }

      {:reply, {:ok, version}, ^state} =
        Server.handle_call(:next_read_version, self(), state)

      # returns read_version, not next_commit_version
      assert version == 103
    end

    test "report_successful_commit updates read version monotonically" do
      initial_state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 105,
        read_version: 100
      }

      # Report commit version 103
      {:noreply, state1} =
        Server.handle_cast({:report_successful_commit, 103}, initial_state)

      assert state1.read_version == 103
      # unchanged
      assert state1.next_commit_version == 105

      # Report older commit version 102 - should not decrease read_version
      {:noreply, state2} =
        Server.handle_cast({:report_successful_commit, 102}, state1)

      # unchanged (monotonic)
      assert state2.read_version == 103

      # Report newer commit version 104
      {:noreply, state3} =
        Server.handle_cast({:report_successful_commit, 104}, state2)

      # updated
      assert state3.read_version == 104
    end

    test "version invariants maintained" do
      # Start with both counters equal
      state = %State{
        director: self(),
        epoch: 1,
        next_commit_version: 100,
        read_version: 100
      }

      # Assign several versions
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)
      {:reply, _, state} = Server.handle_call(:next_commit_version, self(), state)

      assert state.next_commit_version == 103
      assert state.read_version == 100
      # invariant
      assert state.read_version <= state.next_commit_version

      # Report some commits
      {:noreply, state} = Server.handle_cast({:report_successful_commit, 101}, state)
      {:noreply, state} = Server.handle_cast({:report_successful_commit, 102}, state)

      assert state.next_commit_version == 103
      assert state.read_version == 102
      # invariant maintained
      assert state.read_version <= state.next_commit_version
    end
  end
end
