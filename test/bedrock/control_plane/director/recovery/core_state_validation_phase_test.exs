defmodule Bedrock.ControlPlane.Director.Recovery.CoreStateValidationPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.CoreStateValidationPhase
  alias Bedrock.ControlPlane.Director.Recovery.InitializationPhase

  describe "execute/2" do
    test "transitions to LockingPhase when core state validation succeeds" do
      recovery_attempt = %RecoveryAttempt{}

      # Valid TSL with correct types
      valid_tsl = %{
        logs: %{},
        resolvers: []
      }

      context = %{prior_core_state: valid_tsl}

      {result_attempt, next_phase} = CoreStateValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.LockingPhase
    end

    test "stalls recovery when core state validation fails with corrupted data" do
      recovery_attempt = %RecoveryAttempt{}

      # Invalid TSL with binary versions in logs (should be integers)
      invalid_tsl = %{
        logs: %{
          # Binary versions instead of integers
          "log_1" => [<<1, 2, 3>>, <<4, 5, 6>>]
        },
        resolvers: []
      }

      context = %{prior_core_state: invalid_tsl}

      {result_attempt, next_phase} = CoreStateValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert {:stalled, {:corrupted_core_state, _validation_error}} = next_phase
    end

    test "transitions to InitializationPhase when context has no prior_core_state" do
      recovery_attempt = %RecoveryAttempt{}

      # Context without prior_core_state
      context = %{}

      {result_attempt, next_phase} = CoreStateValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end

    test "transitions to InitializationPhase when prior_core_state is nil" do
      recovery_attempt = %RecoveryAttempt{}

      # Context with nil prior_core_state
      context = %{prior_core_state: nil}

      {result_attempt, next_phase} = CoreStateValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end
  end
end
