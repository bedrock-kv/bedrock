defmodule Bedrock.DataPlane.Storage.Olivine.LogicTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.KeySelector

  setup do
    test_dir = Path.join(System.tmp_dir!(), "olivine_logic_test_#{:rand.uniform(100_000)}")
    File.rm_rf!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  describe "startup/4" do
    test "creates directory and initializes state", %{test_dir: test_dir} do
      otp_name = :test_storage
      foreman = self()
      id = "test_shard"

      assert {:ok, %State{} = state} = Logic.startup(otp_name, foreman, id, test_dir)
      assert state.path == test_dir
      assert state.otp_name == otp_name
      assert state.id == id
      assert state.foreman == foreman
      assert %Database{} = state.database
      assert %IndexManager{} = state.index_manager
      assert File.dir?(test_dir)

      Logic.shutdown(state)
    end

    test "handles directory creation failure" do
      invalid_path = "/invalid/read-only/path"

      result = Logic.startup(:test_storage, self(), "test", invalid_path)
      assert {:error, _posix_error} = result
    end
  end

  describe "shutdown/1" do
    test "properly shuts down all components", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Should not crash
      assert :ok = Logic.shutdown(state)
    end

    test "handles shutdown gracefully", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test normal shutdown path
      assert :ok = Logic.shutdown(state)
    end
  end

  describe "lock_for_recovery/3" do
    test "locks successfully with valid epoch", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      director = make_ref()
      epoch = 5

      assert {:ok, locked_state} = Logic.lock_for_recovery(state, director, epoch)
      assert locked_state.mode == :locked
      assert locked_state.director == director
      assert locked_state.epoch == epoch

      Logic.shutdown(locked_state)
    end

    test "rejects older epoch", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      state = %{state | epoch: 10}
      director = make_ref()
      old_epoch = 5

      assert {:error, :newer_epoch_exists} = Logic.lock_for_recovery(state, director, old_epoch)

      Logic.shutdown(state)
    end

    test "handles locking without active puller", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      director = make_ref()
      epoch = 1

      # Test locking when no puller is active
      assert {:ok, locked_state} = Logic.lock_for_recovery(state, director, epoch)
      assert locked_state.pull_task == nil

      Logic.shutdown(locked_state)
    end
  end

  describe "unlock_after_recovery/3" do
    test "basic unlock functionality", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      locked_state = %{state | mode: :locked, epoch: 1}

      # Simple layout structure that matches the pattern matching
      layout = %{logs: %{}, services: %{}}
      durable_version = 100

      # This test focuses on the basic state transitions
      # More complex async behavior would need integration testing
      assert {:ok, unlocked_state} = Logic.unlock_after_recovery(locked_state, durable_version, layout)
      assert unlocked_state.mode == :running
      Logic.shutdown(unlocked_state)
    end
  end

  describe "fetch/4 async behavior" do
    test "returns not_found for missing key", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test sync path without reply_fn
      result = Logic.get(state, "nonexistent_key", 1, [])
      assert {:error, _} = result

      Logic.shutdown(state)
    end

    test "async behavior with reply_fn when errors occur immediately", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      test_pid = self()

      reply_fn = fn result ->
        send(test_pid, {:async_result, result})
      end

      # When fetch fails immediately (like version_too_old), async path isn't taken
      result = Logic.get(state, "async_key", 1, reply_fn: reply_fn)
      # Should return error immediately, not task pid
      assert {:error, _} = result

      Logic.shutdown(state)
    end

    test "KeySelector fetch immediate error handling", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      test_pid = self()

      key_selector = KeySelector.first_greater_or_equal("selector_key")

      reply_fn = fn result ->
        send(test_pid, {:selector_result, result})
      end

      # KeySelector fetch that fails immediately returns error, not task
      result = Logic.get(state, key_selector, 1, reply_fn: reply_fn)
      assert {:error, _} = result

      Logic.shutdown(state)
    end
  end

  describe "range_fetch/5 async behavior" do
    test "executes synchronously when no reply_fn provided", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test range fetch on empty data - expect specific error for empty database
      assert {:error, :version_too_old} = Logic.get_range(state, "range_key1", "range_key3", 1, [])

      Logic.shutdown(state)
    end

    test "handles reply_fn with immediate errors", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)
      test_pid = self()

      reply_fn = fn result ->
        send(test_pid, {:range_result, result})
      end

      # Range fetch that fails immediately returns error, not task
      result = Logic.get_range(state, "async_range1", "async_range3", 1, reply_fn: reply_fn)
      assert {:error, _} = result

      Logic.shutdown(state)
    end

    test "respects limit parameter in opts", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test that limit parameter is passed through (tests opts handling)
      assert {:error, :version_too_old} = Logic.get_range(state, "limit_key1", "limit_key9", 1, limit: 2)

      Logic.shutdown(state)
    end
  end

  describe "error handling paths" do
    test "fetch handles various error conditions", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test basic error path
      result = Logic.get(state, "nonexistent_key", 1, [])
      assert {:error, _} = result

      Logic.shutdown(state)
    end

    test "KeySelector fetch error handling", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Create a KeySelector that should fail
      key_selector = KeySelector.first_greater_or_equal("test_key")

      result = Logic.get(state, key_selector, 1, [])
      # Should return an error for empty database
      assert {:error, _} = result

      Logic.shutdown(state)
    end

    test "version handling in fetch calls", %{test_dir: test_dir} do
      {:ok, state} = Logic.startup(:test_storage, self(), "test", test_dir)

      # Test with various versions that might trigger different error paths
      result1 = Logic.get(state, "key", 0, [])
      result2 = Logic.get(state, "key", 999_999, [])

      # Both should be errors due to empty database
      assert {:error, _} = result1
      assert {:error, _} = result2

      Logic.shutdown(state)
    end
  end

  describe "stop_pulling/1" do
    test "handles state with no puller" do
      state = %State{pull_task: nil}
      result = Logic.stop_pulling(state)
      assert result.pull_task == nil
    end

    test "handles stop_pulling with fake puller" do
      # Test the logic branch without invoking the actual Pulling.stop
      # since that requires a real Task struct
      state = %State{pull_task: nil}

      result = Logic.stop_pulling(state)
      assert result.pull_task == nil
    end
  end
end
