defmodule Bedrock.ControlPlane.Director.Recovery.TopologyPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.TopologyPhase
  alias Bedrock.DataPlane.CommitProxy.RoutingData

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

    test "routing snapshot carries string-encoded materializer members (the q67.23 seed)" do
      test_pid = self()
      mat_sys = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}}
        })
        |> Map.put(:seated_materializer_members, %{0 => %{"wkr_sys" => Atom.to_string(node())}})

      context =
        recovery_context()
        |> with_lock_token("test_token")
        |> Map.put(:unlock_commit_proxy_fn, fn _proxy, _token, _sequencer, _resolver_layout, snapshot ->
          send(test_pid, {:routing_snapshot, snapshot})
          :ok
        end)

      {_result, _next_phase} = TopologyPhase.execute(recovery_attempt, context)

      assert_received {:routing_snapshot, snapshot}

      # Refs are the same member maps the persistence phase commits to the
      # materializers/ family - the seed and the keyspace cannot disagree
      # because both are derived from the same layout. The seed must be
      # member-shaped: RoutingData consumes it verbatim, so any other
      # shape crashes the proxy on the first read after recovery.
      node_string = Atom.to_string(node())
      assert snapshot.materializers == %{0 => %{"wkr_sys" => node_string}}

      # Cross the seam for real: the seed must survive the routing
      # constructor and answer the reads a proxy actually performs.
      routing =
        snapshot
        |> RoutingData.from_snapshot()
        |> RoutingData.insert_shard("z", 0, "")

      assert {:ok, %{"wkr_sys" => ^node_string}} = RoutingData.materializer_members(routing, 0)
      assert {:ok, {"", "z", 0, {"wkr_sys", ^node_string}}} = RoutingData.covering_entry(routing, "a")
    end

    test "the seed carries the committed family for the tags recovery did not seat" do
      # Recovery seats tag 0 and leaves every data tag to the distributor
      # (bedrock-q67.21.13). Seeding from what it seated would leave the
      # data tags unroutable for the epoch and — worse — make every
      # data-tag materializer fail rejoin validation against a proxy that
      # believes the keyspace names nobody for its shard, so it disposes
      # of its own store.
      #
      # The tag recovery DID seat is the other way round: the committed
      # family names members recovery could not put in service, and the
      # pick is by worker id — "aaa" here sorts below the seated member,
      # and it is dead. Routing tag 0 at it stops the distributor's own
      # startup snapshot read, which is the one read that would heal it.
      test_pid = self()
      mat_sys = spawn(fn -> Process.sleep(:infinity) end)
      node_string = Atom.to_string(node())

      recovery_attempt =
        base_recovery_attempt()
        |> with_logs(%{"log_1" => [1, 2]})
        |> with_transaction_services(%{
          "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
          "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}}
        })
        |> Map.put(:seated_materializer_members, %{0 => %{"wkr_sys" => node_string}})
        |> Map.put(:prior_materializer_members, %{
          0 => %{"aaa_wkr_gone" => "dead@nowhere"},
          1 => %{"wkr_data" => node_string}
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

      assert snapshot.materializers == %{
               0 => %{"wkr_sys" => node_string},
               1 => %{"wkr_data" => node_string}
             }

      # Cross the seam: the member clients are routed to for tag 0 is the
      # member recovery unlocked, not the lower-sorting corpse.
      routing =
        snapshot
        |> RoutingData.from_snapshot()
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 0, <<0xFF>>)

      assert {:ok, {_start, _end, 0, {"wkr_sys", ^node_string}}} =
               RoutingData.covering_entry(routing, <<0xFF, "system/x">>)
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
