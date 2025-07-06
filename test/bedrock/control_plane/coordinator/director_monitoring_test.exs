defmodule Bedrock.ControlPlane.Coordinator.DirectorMonitoringTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes
  import ExUnit.CaptureLog

  describe "director monitoring and restart logic" do
    test "start_director_with_monitoring returns director and monitor reference" do
      # This test would require a more complex setup with actual supervisor
      # For now, we'll test the logic components
    end

    test "calculate_backoff_delay implements exponential backoff" do
      # Test the backoff calculation directly
      assert DirectorManagement.calculate_backoff_delay(0) == 1_000
      assert DirectorManagement.calculate_backoff_delay(1) == 2_000
      assert DirectorManagement.calculate_backoff_delay(2) == 4_000
      assert DirectorManagement.calculate_backoff_delay(3) == 8_000
      assert DirectorManagement.calculate_backoff_delay(4) == 16_000

      # Should cap at max delay
      assert DirectorManagement.calculate_backoff_delay(10) == 30_000
    end

    test "handle_director_failure only processes current director's monitor" do
      state = %State{
        director: :some_director,
        director_monitor: make_ref(),
        leader_node: Node.self(),
        my_node: Node.self()
      }

      # Different monitor ref should not trigger failure handling
      different_ref = make_ref()
      result = DirectorManagement.handle_director_failure(state, different_ref, :test_reason)

      assert result == state
    end

    test "handle_director_failure processes failure for current director" do
      monitor_ref = make_ref()

      state = %State{
        director: :some_director,
        director_monitor: monitor_ref,
        leader_node: Node.self(),
        my_node: Node.self(),
        director_retry_state: nil
      }

      # Capture log output to verify warning message
      log_output =
        capture_log(fn ->
          result = DirectorManagement.handle_director_failure(state, monitor_ref, :test_reason)

          assert result.director == :unavailable
          assert result.director_monitor == nil
          assert result.director_retry_state.consecutive_failures == 1
          assert result.director_retry_state.last_failure_reason == :test_reason
        end)

      assert log_output =~ "Director :some_director failed with reason: :test_reason"
    end

    test "schedule_director_restart increments failure count with simple backoff" do
      state = %State{
        director_retry_state: %{
          consecutive_failures: 2,
          last_failure_time: nil
        }
      }

      result = DirectorManagement.schedule_director_restart(state, :persistent_failure)

      assert result.director_retry_state.consecutive_failures == 3
      assert result.director_retry_state.last_failure_reason == :persistent_failure
    end

    test "reset_director_retry_state clears all retry state" do
      state = %State{
        director_retry_state: %{
          consecutive_failures: 3,
          last_failure_time: 12345,
          last_failure_reason: :some_error
        }
      }

      result = DirectorManagement.reset_director_retry_state(state)

      assert result.director_retry_state.consecutive_failures == 0
      assert result.director_retry_state.last_failure_time == nil
      assert result.director_retry_state.last_failure_reason == nil
    end

    test "handle_director_restart_timeout does nothing when not leader" do
      state = %State{
        leader_node: :other_node,
        my_node: Node.self(),
        director: :unavailable
      }

      result = DirectorManagement.handle_director_restart_timeout(state)

      assert result == state
    end

    test "handle_director_restart_timeout does nothing when director already running" do
      state = %State{
        leader_node: Node.self(),
        my_node: Node.self(),
        director: :some_running_director
      }

      result = DirectorManagement.handle_director_restart_timeout(state)

      assert result == state
    end
  end

  describe "state changes" do
    test "put_director_monitor updates monitor reference" do
      state = %State{director_monitor: nil}
      monitor_ref = make_ref()

      result = put_director_monitor(state, monitor_ref)

      assert result.director_monitor == monitor_ref
    end

    test "put_director_retry_state updates retry state" do
      state = %State{director_retry_state: nil}
      retry_state = %{consecutive_failures: 1}

      result = put_director_retry_state(state, retry_state)

      assert result.director_retry_state == retry_state
    end
  end

  describe "timeout configuration" do
    test "timeout values are reasonable" do
      assert DirectorManagement.timeout_in_ms(:director_restart_delay) == 1_000
      assert DirectorManagement.timeout_in_ms(:director_restart_backoff_max) == 30_000
    end
  end
end
