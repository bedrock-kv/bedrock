defmodule Bedrock.ControlPlane.Coordinator.DirectorMonitoringTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.State

  import ExUnit.CaptureLog

  describe "director management" do
    test "handle_director_failure processes failure for current director" do
      director_pid = spawn(fn -> :timer.sleep(100) end)

      state = %State{
        director: director_pid,
        leader_node: Node.self(),
        my_node: Node.self()
      }

      # Capture log output to verify warning message
      log_output =
        capture_log(fn ->
          result = DirectorManagement.handle_director_failure(state, director_pid, :test_reason)

          assert result.director == :unavailable
        end)

      assert log_output =~ "Director #{inspect(director_pid)} failed with reason: :test_reason"
    end

    test "handle_director_failure ignores failure from different director" do
      current_director = spawn(fn -> :timer.sleep(100) end)
      different_director = spawn(fn -> :timer.sleep(100) end)

      state = %State{
        director: current_director,
        leader_node: Node.self(),
        my_node: Node.self()
      }

      result = DirectorManagement.handle_director_failure(state, different_director, :test_reason)

      assert result == state
    end

    test "handle_director_failure does nothing when not leader" do
      director_pid = spawn(fn -> :timer.sleep(100) end)

      state = %State{
        director: director_pid,
        leader_node: :other_node,
        my_node: Node.self()
      }

      result = DirectorManagement.handle_director_failure(state, director_pid, :test_reason)

      assert result == state
    end
  end

  describe "director shutdown" do
    test "shutdown_director_if_running does nothing when not leader" do
      director_pid = spawn(fn -> :timer.sleep(100) end)

      state = %State{
        director: director_pid,
        leader_node: :other_node,
        my_node: Node.self()
      }

      result = DirectorManagement.shutdown_director_if_running(state)

      assert result == state
      assert result.director == director_pid
    end

    test "shutdown_director_if_running does nothing when no director running" do
      state = %State{
        director: :unavailable,
        leader_node: Node.self(),
        my_node: Node.self()
      }

      result = DirectorManagement.shutdown_director_if_running(state)

      assert result == state
      assert result.director == :unavailable
    end

    test "shutdown_director_if_running sets director to unavailable when leader with running director" do
      director_pid = spawn(fn -> :timer.sleep(100) end)

      # Start a test supervisor to handle the terminate_child call
      {:ok, test_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

      state = %State{
        cluster: :test_cluster,
        director: director_pid,
        leader_node: Node.self(),
        my_node: Node.self(),
        supervisor_otp_name: test_supervisor
      }

      # The function will try to terminate via supervisor,
      # expect :not_found since director wasn't started by this supervisor
      result = DirectorManagement.shutdown_director_if_running(state)

      assert result.director == :unavailable

      # Clean up
      DynamicSupervisor.stop(test_supervisor)
    end
  end
end
