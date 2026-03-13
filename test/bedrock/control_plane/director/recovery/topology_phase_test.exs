defmodule Bedrock.ControlPlane.Director.Recovery.TopologyPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase
  alias Bedrock.ControlPlane.Director.Recovery.TopologyPhase
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.Internal.LayoutRouting

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
      assert next_phase == MonitoringPhase

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

    test "uses deterministic routing data and effective replication factor when unlocking commit proxies" do
      test_pid = self()
      logs = %{"log_c" => [], "log_a" => [], "log_b" => []}

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(logs)
        |> with_transaction_services(%{
          "log_a" => %{status: {:up, self()}, kind: :log, last_seen: {:log_a, :node1}},
          "log_b" => %{status: {:up, self()}, kind: :log, last_seen: {:log_b, :node1}},
          "log_c" => %{status: {:up, self()}, kind: :log, last_seen: {:log_c, :node1}}
        })
        |> Map.put(:shard_layout, %{<<0xFF, 0xFF>> => {0, <<>>}})

      context =
        successful_unlock_context()
        |> with_available_services(%{
          "log_a" => {:log, {:log_a, :node1}},
          "log_b" => {:log, {:log_b, :node1}},
          "log_c" => {:log, {:log_c, :node1}}
        })
        |> Map.update!(:cluster_config, &merge_parameters(&1, %{desired_replication_factor: 1}))
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _sequencer, _resolver_layout, routing_data ->
          send(test_pid, {:routing_data, routing_data})
          :ok
        end)

      assert {_result, MonitoringPhase} =
               TopologyPhase.execute(recovery_attempt, context)

      assert_receive {:routing_data, routing_data}
      assert routing_data.log_map == LayoutRouting.build_log_map(logs)
      assert routing_data.replication_factor == LayoutRouting.effective_replication_factor(3, 1)
      RoutingData.cleanup(routing_data)
    end
  end
end
