defmodule Bedrock.ControlPlane.Director.Recovery.CoordinatorConfigPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.CoordinatorConfigPhase

  # Mock cluster that returns unavailable coordinator
  defmodule MockUnavailableCluster do
    def fetch_coordinator(), do: {:error, :unavailable}
    def otp_name(:coordinator), do: :nonexistent_coordinator
  end

  describe "execute/2" do
    test "transitions to monitor_components when coordinator call fails" do
      recovery_attempt = %RecoveryAttempt{
        state: :persist_coordinator_config,
        cluster: MockUnavailableCluster
      }

      # Create a dead process to simulate coordinator failure
      dead_pid = spawn(fn -> :ok end)
      Process.exit(dead_pid, :kill)
      # Ensure it's dead
      :timer.sleep(10)

      context = %{
        cluster_config: %{test: :config},
        node_tracking: nil,
        lock_token: "test_token",
        available_services: %{},
        coordinator: dead_pid
      }

      result = CoordinatorConfigPhase.execute(recovery_attempt, context)

      # Should transition to monitoring even when coordinator fails
      assert result.state == :monitor_components
    end
  end
end
