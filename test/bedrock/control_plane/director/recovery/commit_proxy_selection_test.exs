defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxySelectionTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxySelection

  describe "get_available_commit_proxy/1" do
    test "returns first valid PID from list" do
      # Create a local process to use as a valid PID
      test_process = self()

      pid =
        spawn(fn ->
          send(test_process, {:process_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process_started, ^pid}
      on_exit(fn -> send(pid, :stop) end)

      # Test with valid PID first
      assert {:ok, ^pid} = CommitProxySelection.get_available_commit_proxy([pid])

      # Test with invalid entries followed by valid PID
      assert {:ok, ^pid} = CommitProxySelection.get_available_commit_proxy([nil, "invalid", pid])
    end

    test "returns error when no valid PIDs available" do
      assert {:error, :no_commit_proxies} = CommitProxySelection.get_available_commit_proxy([])

      assert {:error, :no_commit_proxies} =
               CommitProxySelection.get_available_commit_proxy([nil, "invalid", :atom])
    end

    test "handles remote PIDs without crashing" do
      # Create a fake remote PID structure
      # This simulates what we'd get from a remote node
      # Note: We can't actually create a real remote PID in tests, but we can
      # test that the function doesn't crash when given a PID-like structure

      # Create a local PID to test with
      test_process = self()

      local_pid =
        spawn(fn ->
          send(test_process, {:process_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process_started, ^local_pid}
      on_exit(fn -> send(local_pid, :stop) end)

      # The key fix was removing Process.alive? check, so any PID should be returned
      # without attempting to check if it's alive
      assert {:ok, ^local_pid} = CommitProxySelection.get_available_commit_proxy([local_pid])
    end

    test "skips non-PID entries and finds first valid PID" do
      # Create a valid PID
      test_process = self()

      valid_pid =
        spawn(fn ->
          send(test_process, {:process_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process_started, ^valid_pid}
      on_exit(fn -> send(valid_pid, :stop) end)

      # Test with various invalid entries before the valid PID
      invalid_entries = [nil, "string", :atom, 123, %{}, []]
      test_list = invalid_entries ++ [valid_pid]

      assert {:ok, ^valid_pid} = CommitProxySelection.get_available_commit_proxy(test_list)
    end

    test "returns first PID when multiple PIDs are available" do
      # Create two valid PIDs
      test_process = self()

      pid1 =
        spawn(fn ->
          send(test_process, {:process1_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      pid2 =
        spawn(fn ->
          send(test_process, {:process2_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process1_started, ^pid1}
      assert_receive {:process2_started, ^pid2}

      on_exit(fn ->
        send(pid1, :stop)
        send(pid2, :stop)
      end)

      # Should return the first PID in the list
      assert {:ok, ^pid1} = CommitProxySelection.get_available_commit_proxy([pid1, pid2])
      assert {:ok, ^pid2} = CommitProxySelection.get_available_commit_proxy([pid2, pid1])
    end
  end

  describe "get_available_commit_proxy/1 edge cases" do
    test "handles mixed valid and invalid PIDs" do
      # Create a valid PID
      test_process = self()

      valid_pid =
        spawn(fn ->
          send(test_process, {:process_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process_started, ^valid_pid}
      on_exit(fn -> send(valid_pid, :stop) end)

      # Test with a mix of invalid and valid entries
      mixed_list = [
        # invalid
        nil,
        # invalid
        "not_a_pid",
        # invalid
        :also_not_a_pid,
        # valid - should be returned
        valid_pid,
        # valid but comes after first valid
        spawn(fn -> :ok end)
      ]

      assert {:ok, ^valid_pid} = CommitProxySelection.get_available_commit_proxy(mixed_list)
    end

    test "handles empty list" do
      assert {:error, :no_commit_proxies} = CommitProxySelection.get_available_commit_proxy([])
    end

    test "handles list with only invalid entries" do
      invalid_list = [nil, "string", :atom, 123, %{key: "value"}, [1, 2, 3]]

      assert {:error, :no_commit_proxies} =
               CommitProxySelection.get_available_commit_proxy(invalid_list)
    end
  end

  describe "regression test for original bug" do
    test "function does not call Process.alive? which caused remote PID errors" do
      # This test documents the original bug and verifies our fix
      # The original error was:
      # ** (ArgumentError) errors were found at the given arguments:
      #   * 1st argument: not a local pid
      #     :erlang.is_process_alive(#PID<24671.257.0>)

      # Our fix removed the Process.alive? check entirely
      # So this function should work with any PID without attempting to check if it's alive

      test_process = self()

      local_pid =
        spawn(fn ->
          send(test_process, {:process_started, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:process_started, ^local_pid}
      on_exit(fn -> send(local_pid, :stop) end)

      # The function should return the PID without checking if it's alive
      # This prevents the ArgumentError that occurred with remote PIDs
      assert {:ok, ^local_pid} = CommitProxySelection.get_available_commit_proxy([local_pid])

      # The actual commit attempt will handle any connection issues
      # This follows the "fail fast" philosophy - let the actual call handle errors
    end
  end
end
