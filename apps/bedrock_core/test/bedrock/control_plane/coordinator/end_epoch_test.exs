defmodule Bedrock.ControlPlane.Coordinator.EndEpochTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.State

  # Helper function to create base state with overrides
  defp create_state(overrides) do
    defaults = [
      cluster: :test_cluster,
      director: :unavailable,
      leader_node: Node.self(),
      my_node: Node.self(),
      supervisor_otp_name: nil
    ]

    struct!(State, Keyword.merge(defaults, overrides))
  end

  describe "end_epoch command processing" do
    test "end_epoch command shuts down director when leader with running director" do
      director_pid = spawn(fn -> :timer.sleep(100) end)
      {:ok, test_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

      state = create_state(director: director_pid, supervisor_otp_name: test_supervisor)

      # Should set director to :unavailable when processing end_epoch
      assert %State{director: :unavailable} =
               Durability.process_command(state, {:end_epoch, 42})

      DynamicSupervisor.stop(test_supervisor)
    end

    test "end_epoch command does nothing when not leader" do
      director_pid = spawn(fn -> :timer.sleep(100) end)
      state = create_state(director: director_pid, leader_node: :other_node)

      # Should leave state unchanged when not leader
      assert ^state = Durability.process_command(state, {:end_epoch, 42})
    end

    test "end_epoch command does nothing when no director running" do
      state = create_state(director: :unavailable)

      # Should leave state unchanged when no director is running
      assert ^state = Durability.process_command(state, {:end_epoch, 42})
    end
  end
end
