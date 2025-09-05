defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase

  describe "execute/2 with valid data" do
    test "succeeds with existing transaction system layout and transitions to monitoring" do
      # Mock transaction system layout as it would come from TransactionSystemLayoutPhase
      transaction_system_layout = %{
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

      recovery_attempt =
        recovery_attempt()
        |> with_sequencer(self())
        |> with_proxies([self()])
        |> with_resolvers([{<<0>>, self()}])
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })
        |> Map.put(:transaction_system_layout, transaction_system_layout)

      context =
        Map.put(recovery_context(), :commit_transaction_fn, fn _proxy, _transaction ->
          # Mock successful transaction
          {:ok, 1}
        end)

      {result, next_phase} = PersistencePhase.execute(recovery_attempt, context)

      assert next_phase == :completed
      assert result.transaction_system_layout == transaction_system_layout
    end

    test "fails when system transaction fails" do
      transaction_system_layout = %{
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

      recovery_attempt =
        recovery_attempt()
        |> with_sequencer(self())
        |> with_proxies([self()])
        |> Map.put(:transaction_system_layout, transaction_system_layout)

      context =
        Map.put(recovery_context(), :commit_transaction_fn, fn _proxy, _transaction ->
          # Mock failed transaction
          {:error, :timeout}
        end)

      {_result, next_phase_or_stall} = PersistencePhase.execute(recovery_attempt, context)

      assert next_phase_or_stall == {:stalled, {:recovery_system_failed, :timeout}}
    end
  end
end
