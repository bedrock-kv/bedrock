defmodule Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase

  # Helper functions for common test setup
  defp base_recovery_attempt do
    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
  end

  defp successful_unlock_context do
    recovery_context()
    |> with_lock_token("test_token")
    |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout -> :ok end)
    |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout -> :ok end)
  end

  describe "execute/2" do
    test "successfully unlocks services and transitions to persistence phase" do
      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      context = successful_unlock_context()

      expected_epoch = recovery_attempt.epoch
      expected_sequencer = recovery_attempt.sequencer
      expected_proxies = recovery_attempt.proxies
      expected_resolvers = recovery_attempt.resolvers

      {result, next_phase} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      # Pattern match the entire expected structure
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.MonitoringPhase

      assert %{
               transaction_system_layout: %{
                 epoch: ^expected_epoch,
                 sequencer: ^expected_sequencer,
                 proxies: ^expected_proxies,
                 resolvers: ^expected_resolvers
               }
             } = result
    end

    test "fails when commit proxy unlocking fails" do
      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(
          :unlock_commit_proxy_fn,
          fn _proxy, _token, _layout -> {:error, :timeout} end
        )
        |> Map.put(:unlock_storage_fn, fn _storage_pid, _version, _layout -> :ok end)

      expected_error = {:stalled, {:recovery_system_failed, {:unlock_failed, {:commit_proxy_unlock_failed, :timeout}}}}
      assert {_result, ^expected_error} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)
    end

    test "fails when storage server unlocking fails" do
      recovery_attempt =
        base_recovery_attempt()
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
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _layout -> :ok end)
        |> Map.put(
          :unlock_storage_fn,
          fn _storage_pid, _version, _layout -> {:error, :unavailable} end
        )

      expected_error = {:stalled, {:recovery_system_failed, {:unlock_failed, {:storage_unlock_failed, :unavailable}}}}
      assert {_result, ^expected_error} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)
    end

    test "builds transaction system layout with correct service descriptors" do
      recovery_attempt =
        base_recovery_attempt()
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
        with_available_services(successful_unlock_context(), %{
          "log_1" => {:log, {:log_1, :node1}},
          "log_2" => {:log, {:log_2, :node1}},
          "storage_1" => {:storage, {:storage_1, :node1}},
          "storage_2" => {:storage, {:storage_2, :node1}}
        })

      {result, _next_phase} = TransactionSystemLayoutPhase.execute(recovery_attempt, context)

      # Pattern match the expected service structure
      assert %{
               transaction_system_layout: %{
                 services:
                   %{
                     "log_1" => %{kind: :log},
                     "storage_1" => %{kind: :storage}
                   } = services
               }
             } = result

      assert map_size(services) == 4
    end
  end
end
