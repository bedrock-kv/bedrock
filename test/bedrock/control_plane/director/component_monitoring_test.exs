defmodule Bedrock.ControlPlane.Director.ComponentMonitoringTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Server

  import ExUnit.CaptureLog

  describe "component failure handling" do
    test "director exits immediately on component failure with proper logging" do
      # Spawn a test director process
      test_process = self()

      director_pid =
        spawn(fn ->
          # Simulate director receiving :DOWN message
          send(test_process, {:director_started, self()})

          receive do
            {:simulate_component_failure, failed_pid, reason} ->
              # This should cause the director to exit
              send(self(), {:DOWN, make_ref(), :process, failed_pid, reason})

              # Use the actual handle_info logic
              try do
                Server.handle_info({:DOWN, make_ref(), :process, failed_pid, reason}, %{})
              catch
                :exit, exit_reason ->
                  send(test_process, {:director_exited, exit_reason})
                  exit(exit_reason)
              end
          end
        end)

      # Wait for director to start
      assert_receive {:director_started, ^director_pid}

      # Monitor the director
      monitor_ref = Process.monitor(director_pid)

      # Simulate component failure and capture log output
      failed_component = spawn(fn -> :ok end)

      log_output =
        capture_log(fn ->
          send(director_pid, {:simulate_component_failure, failed_component, :test_failure})

          # Director should exit immediately
          assert_receive {:director_exited,
                          {:component_failure, ^failed_component, :test_failure}}

          assert_receive {:DOWN, ^monitor_ref, :process, ^director_pid,
                          {:component_failure, ^failed_component, :test_failure}}
        end)

      # Verify both log messages are present
      assert log_output =~
               "Transaction component #{inspect(failed_component)} failed with reason: :test_failure"

      assert log_output =~ "Director exiting immediately due to component failure"
    end
  end
end
