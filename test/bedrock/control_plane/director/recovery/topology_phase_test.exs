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

    test "unlocks proxies with a plain routing snapshot that carries no process-local handles" do
      test_pid = self()

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _sequencer, _resolver_layout, snapshot ->
          send(test_pid, {:routing_snapshot, snapshot})
          :ok
        end)

      {_result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      assert_received {:routing_snapshot, snapshot}

      # An ETS table reference is only usable by the process/node that made
      # it; the proxy must receive plain data and build its own table.
      refute is_struct(snapshot)

      assert %{
               shard_layout: shard_layout,
               log_map: %{0 => "log_1"},
               log_services: %{"log_1" => _},
               replication_factor: 1
             } = snapshot

      assert is_map(shard_layout)
      refute snapshot |> Map.values() |> Enum.any?(&is_reference/1)
    end

    test "routing snapshot carries string-encoded materializer refs (the q67.23 seed)" do
      test_pid = self()
      mat_sys = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}}
        })
        |> Map.put(:shard_materializers, %{0 => mat_sys})

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _sequencer, _resolver_layout, snapshot ->
          send(test_pid, {:routing_snapshot, snapshot})
          :ok
        end)

      {_result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      assert_received {:routing_snapshot, snapshot}

      # Refs are the same plain strings the persistence phase commits to the
      # materializers/ family - the seed and the keyspace cannot disagree
      # because both are derived from the same layout.
      node_string = Atom.to_string(node())
      assert snapshot.materializers == %{0 => {"wkr_sys", node_string}}
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

    test "the TSL is wiring only — it carries no membership map" do
      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
        })

      context =
        with_available_services(successful_unlock_context(), %{
          "log_1" => {:log, {:log_1, :node1}}
        })

      {result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      # ServerDBInfo parity: epoch, sequencer, proxies, resolvers, logs —
      # and nothing else. Membership questions are answered by workers
      # themselves (log-set check, keyspace rejoin validation), never by
      # an O(workers) map on the broadcast.
      assert Enum.sort(Map.keys(result.transaction_system_layout)) ==
               [:epoch, :logs, :proxies, :resolvers, :sequencer]
    end
  end
end
