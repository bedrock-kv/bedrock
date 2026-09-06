defmodule Bedrock.ControlPlane.Director.Recovery.TSLValidationPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.InitializationPhase
  alias Bedrock.ControlPlane.Director.Recovery.TSLValidationPhase

  describe "execute/2" do
    test "transitions to LockingPhase when TSL validation succeeds" do
      recovery_attempt = %RecoveryAttempt{}

      # Valid TSL with correct types
      valid_tsl = %{
        logs: %{"retained-log" => [0, 1]},
        resolvers: []
      }

      context = %{prior_core_state: valid_tsl}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.LockingPhase
    end

    test "stalls recovery when TSL validation fails with corrupted data" do
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

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert {:stalled, {:corrupted_tsl, _validation_error}} = next_phase
    end

    test "transitions to InitializationPhase when context has no prior_core_state" do
      recovery_attempt = %RecoveryAttempt{}

      # Context without prior_core_state
      context = %{}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end

    test "transitions to InitializationPhase when prior_core_state is nil" do
      recovery_attempt = %RecoveryAttempt{}

      # Context with nil prior_core_state
      context = %{prior_core_state: nil}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end
  end
end
