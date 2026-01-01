defmodule Bedrock.ControlPlane.Director.ComponentMonitoringTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Server

  describe "component failure handling" do
    test "terminates with shutdown reason when component fails" do
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
              case Server.handle_info({:DOWN, make_ref(), :process, failed_pid, reason}, %{}) do
                {:stop, exit_reason, _state} ->
                  send(test_process, {:director_exited, exit_reason})
                  exit(exit_reason)

                other ->
                  send(test_process, {:unexpected_result, other})
                  exit(:unexpected_result)
              end
          end
        end)

      # Wait for director to start
      assert_receive {:director_started, ^director_pid}

      # Monitor the director
      monitor_ref = Process.monitor(director_pid)

      # Simulate component failure
      failed_component_pid = spawn(fn -> :ok end)
      failure_reason = :test_failure
      expected_shutdown_reason = {:shutdown, {:component_failure, failed_component_pid, failure_reason}}

      send(director_pid, {:simulate_component_failure, failed_component_pid, failure_reason})

      # Director should exit immediately with proper shutdown reason
      assert_receive {:director_exited, ^expected_shutdown_reason}
      assert_receive {:DOWN, ^monitor_ref, :process, ^director_pid, ^expected_shutdown_reason}
    end
  end
end
