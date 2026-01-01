defmodule Bedrock.DataPlane.Storage.Basalt.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Basalt.State

  describe "State struct" do
    test "creates default state with locked mode" do
      state = %State{}

      assert state.otp_name == nil
      assert state.path == nil
      assert state.foreman == nil
      assert state.id == nil
      assert state.database == nil
      assert state.pull_task == nil
      assert state.epoch == nil
      assert state.director == nil
      assert state.mode == :locked
    end

    test "creates state with custom fields" do
      state = %State{
        otp_name: :test_storage,
        path: "/tmp/test",
        id: "storage_1",
        mode: :running
      }

      assert state.otp_name == :test_storage
      assert state.path == "/tmp/test"
      assert state.id == "storage_1"
      assert state.mode == :running
    end
  end

  describe "update_mode/2" do
    test "updates mode from locked to running" do
      state = %State{mode: :locked}

      updated_state = State.update_mode(state, :running)

      assert updated_state.mode == :running
    end

    test "updates mode from running to locked" do
      state = %State{mode: :running}

      updated_state = State.update_mode(state, :locked)

      assert updated_state.mode == :locked
    end

    test "preserves other fields when updating mode" do
      state = %State{
        otp_name: :test,
        path: "/tmp",
        mode: :locked
      }

      updated_state = State.update_mode(state, :running)

      assert updated_state.otp_name == :test
      assert updated_state.path == "/tmp"
      assert updated_state.mode == :running
    end
  end

  describe "update_director_and_epoch/3" do
    test "updates director and epoch" do
      state = %State{}
      director = self()
      epoch = 123

      updated_state = State.update_director_and_epoch(state, director, epoch)

      assert updated_state.director == director
      assert updated_state.epoch == 123
    end

    test "can set director and epoch to nil" do
      state = %State{director: self(), epoch: 456}

      updated_state = State.update_director_and_epoch(state, nil, nil)

      assert updated_state.director == nil
      assert updated_state.epoch == nil
    end

    test "preserves other fields when updating director and epoch" do
      state = %State{
        otp_name: :test,
        mode: :running,
        director: nil,
        epoch: nil
      }

      updated_state = State.update_director_and_epoch(state, self(), 789)

      assert updated_state.otp_name == :test
      assert updated_state.mode == :running
      assert updated_state.director == self()
      assert updated_state.epoch == 789
    end
  end

  describe "reset_puller/1" do
    test "sets pull_task to nil" do
      mock_task = Task.async(fn -> :ok end)
      state = %State{pull_task: mock_task}

      updated_state = State.reset_puller(state)

      assert updated_state.pull_task == nil

      # Clean up
      Task.shutdown(mock_task)
    end

    test "preserves other fields when resetting puller" do
      mock_task = Task.async(fn -> :ok end)

      state = %State{
        otp_name: :test,
        mode: :running,
        pull_task: mock_task
      }

      updated_state = State.reset_puller(state)

      assert updated_state.otp_name == :test
      assert updated_state.mode == :running
      assert updated_state.pull_task == nil

      # Clean up
      Task.shutdown(mock_task)
    end
  end

  describe "put_puller/2" do
    test "sets pull_task to given task" do
      state = %State{pull_task: nil}
      mock_task = Task.async(fn -> :ok end)

      updated_state = State.put_puller(state, mock_task)

      assert updated_state.pull_task == mock_task

      # Clean up
      Task.shutdown(mock_task)
    end

    test "replaces existing pull_task" do
      old_task = Task.async(fn -> :ok end)
      new_task = Task.async(fn -> :ok end)
      state = %State{pull_task: old_task}

      updated_state = State.put_puller(state, new_task)

      assert updated_state.pull_task == new_task
      refute updated_state.pull_task == old_task

      # Clean up
      Task.shutdown(old_task)
      Task.shutdown(new_task)
    end

    test "preserves other fields when setting puller" do
      state = %State{
        otp_name: :test,
        mode: :running,
        pull_task: nil
      }

      mock_task = Task.async(fn -> :ok end)

      updated_state = State.put_puller(state, mock_task)

      assert updated_state.otp_name == :test
      assert updated_state.mode == :running
      assert updated_state.pull_task == mock_task

      # Clean up
      Task.shutdown(mock_task)
    end
  end
end
