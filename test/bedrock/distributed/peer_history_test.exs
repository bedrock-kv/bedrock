defmodule Bedrock.Distributed.PeerHistoryTest do
  use ExUnit.Case, async: false

  alias Bedrock.Test.History.Oracle
  alias Bedrock.Test.History.PeerCluster

  @moduletag :distributed
  @moduletag timeout: 120_000

  test "acknowledged writes survive coupled coordinator and log VM loss with retained WAL" do
    scenario("log_restart", fn cluster, record ->
      before = PeerCluster.ready(cluster)
      record.({:placement_before, before})
      {first, before_entries} = successful_attempt(cluster, record, "before", [{:put, "history/peer/a", "durable"}])
      assert first.status == :committed
      wal = PeerCluster.wal_files(cluster)
      assert wal != []
      record.({:wal_before_stop, wal})
      # bedrock-t0k: a live coordinator can restart a failed director in the same epoch.
      # Keep that standalone reproduction available; the normal schedule includes
      # coordinator loss explicitly and obtains a genuine new Raft term.
      coupled = System.get_env("BEDROCK_HISTORY_SINGLE_LOG_REPRO") != "1"

      if coupled do
        PeerCluster.suspend_coordinator(cluster)
        record.({:coordinator_suspended, :before_log_loss})
      end

      PeerCluster.stop_log(cluster)
      record.({:log_stopped, PeerCluster.nodes(cluster)})
      assert PeerCluster.log_down?(cluster)
      assert Enum.all?(wal, &File.exists?/1), "log node loss must retain its original WAL files"

      if coupled do
        PeerCluster.stop_coordinator(cluster)
        record.({:coordinator_stopped, :after_log_loss})
      end

      PeerCluster.restart_log(cluster)

      if coupled do
        PeerCluster.restart_coordinator(cluster)
        record.({:coordinator_restarted, :after_log_return})
      end

      record.({:log_restarted, PeerCluster.wal_files(cluster)})
      after_recovery = PeerCluster.ready(cluster, before.epoch)
      record.({:placement_after, after_recovery})
      assert after_recovery.epoch > before.epoch

      {second, after_entries} =
        successful_attempt(cluster, record, "after", [{:get, "history/peer/a"}, {:put, "history/peer/b", "after"}])

      assert second.status == :committed
      assert second.reads == [{:get, "history/peer/a", "durable"}]
      final = PeerCluster.final(cluster)
      record.({:final, final})
      assert {:ok, _} = Oracle.check(%{}, before_entries ++ after_entries, final)
    end)
  end

  test "a live log VM is isolated across every cut edge then recovers after heal and coordinator restart" do
    scenario("partition", fn cluster, record ->
      before = PeerCluster.ready(cluster)
      record.({:placement_before, before})
      {first, before_entries} = successful_attempt(cluster, record, "before", [{:put, "history/peer/a", "durable"}])
      assert first.status == :committed
      connected = PeerCluster.connected_edges(cluster)
      record.({:connected_before_cut, connected})
      assert Enum.all?(connected, fn {_, _, reply} -> reply == :pong end)
      PeerCluster.suspend_coordinator(cluster)
      record.({:coordinator_suspended, :before_partition})

      try do
        record.({:cut, PeerCluster.partition_log(cluster)})
        proof = PeerCluster.partition_proof(cluster)
        record.({:partition_proof, proof})
        assert Enum.all?(proof, fn edge -> edge.alive and edge.disconnected and edge.ping == :pang end)
        assert length(proof) == 4
      after
        record.({:heal, PeerCluster.heal(cluster)})
        PeerCluster.stop_coordinator(cluster)
        PeerCluster.restart_coordinator(cluster)
        record.({:coordinator_restarted, :after_heal})
      end

      after_recovery = PeerCluster.ready(cluster, before.epoch)
      record.({:placement_after, after_recovery})
      assert after_recovery.epoch > before.epoch

      {second, after_entries} =
        successful_attempt(cluster, record, "after", [{:get, "history/peer/a"}, {:put, "history/peer/b", "healed"}])

      assert second.status == :committed
      assert second.reads == [{:get, "history/peer/a", "durable"}]
      final = PeerCluster.final(cluster)
      record.({:final, final})
      assert {:ok, _} = Oracle.check(%{}, before_entries ++ after_entries, final)
    end)
  end

  defp successful_attempt(cluster, record, prefix, operations) do
    entries =
      Enum.reduce_while(1..4, [], fn index, entries ->
        entry = PeerCluster.attempt(cluster, "#{prefix}-#{index}", operations)
        record.({:attempt, entry})
        entries = entries ++ [entry]

        if entry.status == :committed do
          {:halt, entries}
        else
          record.({:retry_delay_ms, 1_000})
          Process.sleep(1_000)
          {:cont, entries}
        end
      end)

    {List.last(entries), entries}
  end

  defp scenario(name, run) do
    assert Node.alive?(), "run with elixir --sname bedrock_peer_history -S mix test --include distributed"
    root = Path.join(System.tmp_dir!(), "bedrock-peer-#{name}-#{System.system_time(:nanosecond)}")
    {:ok, events} = Agent.start_link(fn -> [] end)
    record = fn event -> Agent.update(events, &[{System.monotonic_time(), event} | &1]) end
    cluster = PeerCluster.start(root)

    try do
      run.(cluster, record)
    catch
      kind, reason ->
        record.({:failure, kind, reason, __STACKTRACE__})
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      record.({:cleanup_state, PeerCluster.diagnostics(cluster)})
      path = PeerCluster.artifact(root, name, Agent.get(events, &Enum.reverse/1))
      IO.puts("Peer history artifact: #{path}")
      PeerCluster.stop(cluster)
    end
  end
end
