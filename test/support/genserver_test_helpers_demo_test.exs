defmodule Bedrock.Test.GenServerTestHelpersDemoTest do
  @moduledoc """
  Demonstration tests showing how to use the GenServer test helper macros.
  These tests show the proper patterns for testing GenServer calls and casts.
  """

  use ExUnit.Case, async: true

  import Bedrock.Test.GenServerTestHelpers

  describe "GenServer test helper macros" do
    test "assert_cast_received demonstrates cast message testing" do
      # Simulate a GenServer that sends a cast to our test process
      test_pid = self()

      # Spawn a process that will send a cast message
      spawn(fn ->
        GenServer.cast(test_pid, {:worker_health, "worker_1", {:ok, self()}})
      end)

      # Use our helper macro to assert on the exact cast message
      assert_cast_received({:worker_health, worker_id, health_status}) do
        assert worker_id == "worker_1"
        assert {:ok, pid} = health_status
        assert is_pid(pid)
      end
    end

    test "assert_call_received with timeout demonstrates call message testing" do
      # Simulate a process sending us a call message
      test_pid = self()

      spawn(fn ->
        # Send a call message to our test process
        send(test_pid, {:"$gen_call", {self(), make_ref()}, {:recover_from, :log_1, 100, 200}})
      end)

      # Use our helper macro to assert on the exact call message with timeout
      assert_call_received({:recover_from, source, first, last}, 200) do
        assert source == :log_1
        assert first == 100
        assert last == 200
      end
    end

    test "simple assert_cast_received without assertions" do
      test_pid = self()

      spawn(fn ->
        GenServer.cast(test_pid, {:notification, "test_message"})
      end)

      # Simple usage without additional assertions
      message = assert_cast_received({:notification, _content})
      assert message == {:notification, "test_message"}
    end

    test "simple assert_call_received without assertions" do
      test_pid = self()

      spawn(fn ->
        send(test_pid, {:"$gen_call", {self(), make_ref()}, {:fetch_data, :user_id_123}})
      end)

      # Simple usage without additional assertions
      message = assert_call_received({:fetch_data, _user_id})
      assert message == {:fetch_data, :user_id_123}
    end
  end
end
