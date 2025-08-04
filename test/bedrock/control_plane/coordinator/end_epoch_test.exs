defmodule Bedrock.ControlPlane.Coordinator.EndEpochTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.State

  describe "end_epoch command processing" do
    test "end_epoch command shuts down director when leader with running director" do
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

      # Process the end_epoch command
      result = Durability.process_command(state, {:end_epoch, 42})

      # Should set director to :unavailable
      assert result.director == :unavailable

      # Clean up
      DynamicSupervisor.stop(test_supervisor)
    end

    test "end_epoch command does nothing when not leader" do
      director_pid = spawn(fn -> :timer.sleep(100) end)

      state = %State{
        cluster: :test_cluster,
        director: director_pid,
        # Not the leader
        leader_node: :other_node,
        my_node: Node.self(),
        supervisor_otp_name: nil
      }

      # Process the end_epoch command
      result = Durability.process_command(state, {:end_epoch, 42})

      # Should leave director unchanged
      assert result.director == director_pid
      assert result == state
    end

    test "end_epoch command does nothing when no director running" do
      state = %State{
        cluster: :test_cluster,
        director: :unavailable,
        leader_node: Node.self(),
        my_node: Node.self(),
        supervisor_otp_name: nil
      }

      # Process the end_epoch command
      result = Durability.process_command(state, {:end_epoch, 42})

      # Should leave state unchanged
      assert result.director == :unavailable
      assert result == state
    end
  end
end
