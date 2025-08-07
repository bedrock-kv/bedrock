defmodule Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase

  import RecoveryTestSupport

  describe "execute/2" do
    test "successfully unlocks services and transitions to persistence phase" do
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
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout ->
          # Mock successful unlock
          :ok
        end)
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout ->
          # Mock successful unlock
          :ok
        end)

      {result, next_phase} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.PersistencePhase
      assert result.transaction_system_layout != nil
      assert result.transaction_system_layout.epoch == recovery_attempt.epoch
      assert result.transaction_system_layout.sequencer == recovery_attempt.sequencer
      assert result.transaction_system_layout.proxies == recovery_attempt.proxies
      assert result.transaction_system_layout.resolvers == recovery_attempt.resolvers
    end

    test "fails when commit proxy unlocking fails" do
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
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout ->
          # Mock failed unlock
          {:error, :timeout}
        end)
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout ->
          # Mock successful unlock
          :ok
        end)

      {_result, next_phase_or_stall} =
        TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed,
                 {:unlock_failed, {:commit_proxy_unlock_failed, :timeout}}}}
    end

    test "fails when storage server unlocking fails" do
      recovery_attempt =
        recovery_attempt()
        |> with_sequencer(self())
        |> with_proxies([self()])
        |> with_resolvers([{<<0>>, self()}])
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_storage_teams([
          %{tag: 0, key_range: {"", :end}, storage_ids: ["storage_1"]}
        ])
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "storage_1" => %{status: {:up, self()}, kind: :storage, last_seen: {:storage_1, :node1}}
        })

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout ->
          # Mock successful unlock
          :ok
        end)
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout ->
          # Mock failed unlock
          {:error, :unavailable}
        end)

      {_result, next_phase_or_stall} =
        TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      assert next_phase_or_stall ==
               {:stalled,
                {:recovery_system_failed,
                 {:unlock_failed, {:storage_unlock_failed, :unavailable}}}}
    end

    test "builds transaction system layout with correct service descriptors" do
      recovery_attempt =
        recovery_attempt()
        |> with_sequencer(self())
        |> with_proxies([self()])
        |> with_resolvers([{<<0>>, self()}])
        |> with_logs(%{"log_1" => [1, 2], "log_2" => [3, 4]})
        |> with_storage_teams([
          %{tag: 0, key_range: {"", :end}, storage_ids: ["storage_1", "storage_2"]}
        ])
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "log_2" => %{status: {:up, self()}, kind: :log, last_seen: {:log_2, :node1}},
          "storage_1" => %{status: {:up, self()}, kind: :storage, last_seen: {:storage_1, :node1}},
          "storage_2" => %{status: {:up, self()}, kind: :storage, last_seen: {:storage_2, :node1}}
        })

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> with_available_services(%{
          "log_1" => {:log, {:log_1, :node1}},
          "log_2" => {:log, {:log_2, :node1}},
          "storage_1" => {:storage, {:storage_1, :node1}},
          "storage_2" => {:storage, {:storage_2, :node1}}
        })
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout -> :ok end)
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout -> :ok end)

      {result, _next_phase} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      layout = result.transaction_system_layout
      assert map_size(layout.services) == 4
      assert layout.services["log_1"].kind == :log
      assert layout.services["storage_1"].kind == :storage
    end
  end
end
