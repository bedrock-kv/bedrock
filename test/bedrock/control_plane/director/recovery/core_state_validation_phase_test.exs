defmodule Bedrock.ControlPlane.Director.Recovery.CoreStateValidationPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.CoreStateValidationPhase
  alias Bedrock.ControlPlane.Director.Recovery.InitializationPhase

  describe "execute/2" do
    test "transitions to InitializationPhase when prior core state names no logs (fresh cluster)" do
      recovery_attempt = %RecoveryAttempt{}

      # Valid types, but no prior logs to recover from -- the durable
      # bootstrap of a cluster that has never completed a recovery.
      fresh_core_state = %{
        logs: %{},
        resolvers: []
      }

      context = %{prior_core_state: fresh_core_state}

      {result_attempt, next_phase} = CoreStateValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end

    test "transitions to LockingPhase when prior core state names prior logs (existing cluster)" do
      recovery_attempt = %RecoveryAttempt{}

      # Valid types, and prior logs to lock and recover from.
      existing_core_state = %{
        logs: %{"log_1" => [1, 2]},
        resolvers: []
      }

      context = %{prior_core_state: existing_core_state}

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
