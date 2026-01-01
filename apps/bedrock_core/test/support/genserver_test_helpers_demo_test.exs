defmodule Bedrock.Test.GenServerTestHelpersDemoTest do
  @moduledoc """
  Demonstration tests showing how to use the GenServer test helper macros.
  These tests show the proper patterns for testing GenServer calls and casts
  with pattern matching and timeout handling.
  """

  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  # Helpers to simulate message sending in separate processes
  defp send_cast_message(test_pid, message) do
    spawn(fn -> GenServer.cast(test_pid, message) end)
  end

  defp send_call_message(test_pid, message) do
    spawn(fn -> send(test_pid, {:"$gen_call", {self(), make_ref()}, message}) end)
  end

  describe "GenServer test helper macros" do
    setup do
      [test_pid: self()]
    end

    test "assert_cast_received demonstrates cast message testing", %{test_pid: test_pid} do
      send_cast_message(test_pid, {:worker_health, "worker_1", {:ok, self()}})

      # Use pattern matching to assert on exact message structure
      assert_cast_received({:worker_health, "worker_1", {:ok, pid}}) do
        assert is_pid(pid)
      end
    end

    test "assert_call_received with timeout demonstrates call message testing", %{test_pid: test_pid} do
      send_call_message(test_pid, {:recover_from, :log_1, 100, 200})

      # Use pattern matching to assert on exact message structure with timeout
      assert_call_received({:recover_from, :log_1, 100, 200}, 200)
    end

    test "demonstrates simple message pattern matching", %{test_pid: test_pid} do
      # Test both cast and call message patterns
      send_cast_message(test_pid, {:notification, "test_message"})
      assert_cast_received({:notification, "test_message"})

      send_call_message(test_pid, {:fetch_data, :user_id_123})
      assert_call_received({:fetch_data, :user_id_123})
    end
  end
end
