defmodule Bedrock.ControlPlane.Director.Recovery.ValidationPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.ValidationPhase
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  import RecoveryTestSupport

  describe "execute/2" do
    test "validates sequencer presence" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_sequencer(nil)

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled, {:recovery_system_failed, {:invalid_recovery_state, :no_sequencer}}}
    end

    test "validates commit proxies presence" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_proxies([])

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed, {:invalid_recovery_state, :no_commit_proxies}}}
    end

    test "validates resolvers presence" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_resolvers([])

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled, {:recovery_system_failed, {:invalid_recovery_state, :no_resolvers}}}
    end

    test "validates logs have corresponding services" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{})

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed,
                 {:invalid_recovery_state, {:missing_log_services, ["log_1"]}}}}
    end

    test "fails with invalid sequencer type" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_sequencer(:invalid_sequencer)

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed, {:invalid_recovery_state, :invalid_sequencer}}}
    end

    test "fails with invalid commit proxies type" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_proxies(:invalid_proxies)

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed, {:invalid_recovery_state, :invalid_commit_proxies}}}
    end

    test "validates resolver formats" do
      recovery_attempt =
        validation_recovery_attempt()
        |> with_resolvers([{<<0>>, self()}, :invalid_resolver])

      {_result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed, {:invalid_recovery_state, :invalid_resolvers}}}
    end

    test "succeeds with valid components" do
      recovery_attempt = validation_recovery_attempt()

      {result, next_phase_or_stall} =
        ValidationPhase.execute(recovery_attempt, validation_context())

      assert result == recovery_attempt
      assert next_phase_or_stall == Bedrock.ControlPlane.Director.Recovery.PersistencePhase
    end
  end

  # Helper functions to create test data
  defp validation_recovery_attempt do
    %RecoveryAttempt{
      cluster: RecoveryTestSupport.TestCluster,
      epoch: 1,
      sequencer: self(),
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]},
      storage_teams: [],
      transaction_services: %{
        "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
      }
    }
  end

  defp validation_context do
    %{}
  end
end
