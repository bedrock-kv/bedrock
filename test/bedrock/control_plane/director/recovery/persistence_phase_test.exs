defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase

  import RecoveryTestSupport

  describe "execute/2 with valid data" do
    test "succeeds with valid recovery attempt and transitions to monitoring" do
      recovery_attempt =
        recovery_attempt()
        |> with_sequencer(self())
        |> with_proxies([self()])
        |> with_resolvers([{<<0>>, self()}])
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(:commit_transaction_fn, fn _proxy, _transaction ->
          # Mock successful transaction
          {:ok, 1}
        end)
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout ->
          # Mock successful unlock
          :ok
        end)
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout ->
          # Mock successful unlock
          :ok
        end)

      {result, next_phase} = PersistencePhase.execute(recovery_attempt, context)

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.MonitoringPhase
      assert result.transaction_system_layout != nil
    end
  end
end
