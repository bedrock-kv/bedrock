defmodule Bedrock.ControlPlane.Director.Recovery.TopologyPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.TopologyPhase

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
    |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _sequencer, _resolver_layout, _routing_data -> :ok end)
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

      {result, next_phase} = TopologyPhase.execute(recovery_attempt, context)

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
          fn _proxy, _token, _sequencer, _resolver_layout, _routing_data -> {:error, :timeout} end
        )

      expected_error = {:stalled, {:recovery_system_failed, {:unlock_failed, {:commit_proxy_unlock_failed, :timeout}}}}
      assert {_result, ^expected_error} = TopologyPhase.execute(recovery_attempt, context)
    end

    test "builds transaction system layout with correct service descriptors" do
      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2], "log_2" => [3, 4]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "log_2" => %{status: {:up, self()}, kind: :log, last_seen: {:log_2, :node1}}
        })

      context =
        with_available_services(successful_unlock_context(), %{
          "log_1" => {:log, {:log_1, :node1}},
          "log_2" => {:log, {:log_2, :node1}}
        })

      {result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      # Pattern match the expected service structure - only log services
      assert %{
               transaction_system_layout: %{
                 services:
                   %{
                     "log_1" => %{kind: :log},
                     "log_2" => %{kind: :log}
                   } = services
               }
             } = result

      assert map_size(services) == 2
    end

    test "the layout references active shard materializers alongside the logs" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "mat_1" => %{status: {:up, materializer_pid}, kind: :materializer, last_seen: {:mat_1, :node1}},
          "locked_but_inactive" => %{status: {:up, spawn(fn -> :ok end)}, kind: :materializer, last_seen: {:x, :node1}}
        })
        |> Map.put(:shard_materializers, %{0 => materializer_pid})

      context =
        with_available_services(successful_unlock_context(), %{
          "log_1" => {:log, {:log_1, :node1}},
          "mat_1" => {:materializer, {:mat_1, :node1}}
        })

      {result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      # The services map is the layout's statement of what should exist:
      # the logs, and exactly the materializers serving shards — a locked
      # but inactive materializer is not referenced (and reconciliation
      # will retire it).
      assert %{
               transaction_system_layout: %{
                 services: %{"log_1" => %{kind: :log}, "mat_1" => %{kind: :materializer}} = services
               }
             } = result

      assert map_size(services) == 2
    end

    test "a worker created this attempt (not yet advertised) keeps its place in the layout" do
      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"new_log" => []})
        |> with_transaction_services(%{
          "new_log" => %{status: {:up, self()}, kind: :log, last_seen: {:new_log_otp, :node1}}
        })

      # NOT in available_services: created moments ago, advertisement async
      context = with_available_services(successful_unlock_context(), %{})

      {result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      assert %{transaction_system_layout: %{services: %{"new_log" => %{kind: :log}}}} = result
    end
  end
end
