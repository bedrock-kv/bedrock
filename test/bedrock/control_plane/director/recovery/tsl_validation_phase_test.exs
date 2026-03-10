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
        logs: %{},
        resolvers: []
      }

      context = %{old_transaction_system_layout: valid_tsl}

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

      context = %{old_transaction_system_layout: invalid_tsl}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert {:stalled, {:corrupted_tsl, _validation_error}} = next_phase
    end

    test "transitions to InitializationPhase when context has no old_transaction_system_layout" do
      recovery_attempt = %RecoveryAttempt{}

      # Context without old_transaction_system_layout
      context = %{}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end

    test "transitions to InitializationPhase when old_transaction_system_layout is nil" do
      recovery_attempt = %RecoveryAttempt{}

      # Context with nil old_transaction_system_layout
      context = %{old_transaction_system_layout: nil}

      {result_attempt, next_phase} = TSLValidationPhase.execute(recovery_attempt, context)

      assert result_attempt == recovery_attempt
      assert next_phase == InitializationPhase
    end
  end
end
