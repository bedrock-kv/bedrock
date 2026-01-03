defmodule Bedrock.ControlPlane.Coordinator.DirectorMonitoringTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.State

  # Common test helpers
  defp create_director_pid, do: spawn(fn -> :timer.sleep(100) end)

  defp leader_state(director \\ :unavailable) do
    %State{
      director: director,
      leader_node: Node.self(),
      my_node: Node.self()
    }
  end

  defp non_leader_state(director) do
    %State{
      director: director,
      leader_node: :other_node,
      my_node: Node.self()
    }
  end

  describe "director management" do
    test "handle_director_failure processes failure for current director" do
      director_pid = create_director_pid()
      state = leader_state(director_pid)

      # Capture log output to verify warning message
      log_output =
        capture_log(fn ->
          assert %{director: :unavailable} =
                   DirectorManagement.handle_director_failure(state, director_pid, :test_reason)
        end)

      assert log_output =~ "Director #{inspect(director_pid)} failed with reason: :test_reason"
    end

    test "handle_director_failure ignores failure from different director" do
      current_director = create_director_pid()
      different_director = create_director_pid()
      state = leader_state(current_director)

      assert ^state =
               DirectorManagement.handle_director_failure(state, different_director, :test_reason)
    end

    test "handle_director_failure does nothing when not leader" do
      director_pid = create_director_pid()
      state = non_leader_state(director_pid)

      assert ^state =
               DirectorManagement.handle_director_failure(state, director_pid, :test_reason)
    end
  end

  describe "director shutdown" do
    test "shutdown_director_if_running does nothing when not leader" do
      director_pid = create_director_pid()
      state = non_leader_state(director_pid)

      assert %{director: ^director_pid} =
               ^state =
               DirectorManagement.shutdown_director_if_running(state)
    end

    test "shutdown_director_if_running does nothing when no director running" do
      state = leader_state()

      assert %{director: :unavailable} =
               ^state =
               DirectorManagement.shutdown_director_if_running(state)
    end

    test "shutdown_director_if_running sets director to unavailable when leader with running director" do
      director_pid = create_director_pid()

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
      assert %{director: :unavailable} =
               DirectorManagement.shutdown_director_if_running(state)

      # Clean up
      DynamicSupervisor.stop(test_supervisor)
    end
  end
end
