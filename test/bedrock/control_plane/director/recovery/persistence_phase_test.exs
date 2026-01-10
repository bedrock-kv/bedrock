defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase

  # Shared test data setup
  defp mock_transaction_system_layout do
    %{
      id: "test_layout_id",
      epoch: 1,
      director: self(),
      sequencer: self(),
      rate_keeper: nil,
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]},
      storage_teams: [],
      services: %{
        "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
      }
    }
  end

  defp base_recovery_attempt do
    layout = mock_transaction_system_layout()

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
    })
    |> Map.put(:transaction_system_layout, layout)
  end

  describe "execute/2" do
    test "succeeds with existing transaction system layout and transitions to completed" do
      expected_layout = mock_transaction_system_layout()
      recovery_attempt = base_recovery_attempt()

      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:ok, 1, 0} end)

      # Pattern match both result and next phase in single assertion
      assert {%{transaction_system_layout: ^expected_layout}, :completed} =
               PersistencePhase.execute(recovery_attempt, context)
    end

    test "fails when system transaction fails" do
      recovery_attempt = base_recovery_attempt()
      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:error, :timeout} end)

      # Pattern match tuple destructuring with expected stall reason
      assert {_, {:stalled, {:recovery_system_failed, :timeout}}} =
               PersistencePhase.execute(recovery_attempt, context)
    end
  end
end
