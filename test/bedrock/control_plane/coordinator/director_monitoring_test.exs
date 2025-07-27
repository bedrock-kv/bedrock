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
end
