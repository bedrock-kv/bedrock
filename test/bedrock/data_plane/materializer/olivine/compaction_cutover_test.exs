defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionCutoverTest do
  @moduledoc """
  Compaction cutover must not lose transactions ingested while the
  background compaction ran (bedrock-qzr.19).

  The cutover rewinds the index to the compacted durable snapshot. Every
  transaction applied after that snapshot must come back — not by special
  bookkeeping, but the same way recovery restores a suffix: the stream
  puller is stopped and restarted from the durable boundary, and the
  stream re-delivers everything after it.

  It also guards the cutover's precondition (bedrock-ngl): the durable
  version the compaction captured must still be in `versions` when the
  cutover arrives, so no window advance may run while a compaction is open.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Server
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # A shard-stream stand-in that owns the full transaction history and can
  # serve a pull from any position — exactly the contract chunks + buffer
  # provide. Pulls with nothing available park until data arrives (long
  # poll). Every pull position is reported to the test process.
  defmodule ReplayShardServer do
    @moduledoc false
    use GenServer

    def start_link(notify), do: GenServer.start_link(__MODULE__, notify)

    def append(server, version, slice), do: GenServer.call(server, {:append, version, slice})

    @impl true
    def init(notify), do: {:ok, %{txns: [], notify: notify, waiting: nil}}

    @impl true
    def handle_call({:pull, from, _limit, _timeout}, caller, state) do
      send(state.notify, {:pulled, from})

      case Enum.filter(state.txns, fn {version, _} -> version >= from end) do
        [] -> {:noreply, %{state | waiting: {caller, from}}}
        available -> {:reply, reply_for(available), state}
      end
    end

    def handle_call({:append, version, slice}, _from, state) do
      txns = state.txns ++ [{version, slice}]
      state = %{state | txns: txns}

      case state.waiting do
        {caller, from} when version >= from ->
          available = Enum.filter(txns, fn {v, _} -> v >= from end)
          GenServer.reply(caller, reply_for(available))
          {:reply, :ok, %{state | waiting: nil}}

        _ ->
          {:reply, :ok, state}
      end
    end

    defp reply_for(available) do
      {high_water, _} = List.last(available)
      {:ok, available, %{high_water: high_water, kcv: high_water}}
    end
  end

  # A log stand-in: discovery always hands out the same shard server.
  defmodule StubLog do
    @moduledoc false
    use GenServer

    def start_link(shard_server), do: GenServer.start_link(__MODULE__, shard_server)

    @impl true
    def init(shard_server), do: {:ok, shard_server}

    @impl true
    def handle_call({:get_shard_server, _shard_id}, _from, shard_server),
      do: {:reply, {:ok, shard_server}, shard_server}
  end

  defp wait_for_health_report(worker_id, pid, timeout \\ 5_000) do
    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      timeout -> flunk("Did not receive health report within #{timeout}ms")
    end
  end

  defp start_worker(tmp_dir) do
    worker_id = "cutover-worker-#{System.unique_integer([:positive])}"
    otp_name = :"olivine_cutover_#{System.unique_integer([:positive])}"

    child_spec = %{
      id: {Server, worker_id},
      start: {GenServer, :start_link, [Server, {otp_name, self(), worker_id, tmp_dir, [shard_id: 1]}, [name: otp_name]]}
    }

    {:ok, pid} = start_supervised(child_spec)
    wait_for_health_report(worker_id, pid)
    pid
  end

  defp unlock_with_stream(pid, log_stub) do
    {:ok, _pid, _info} = GenServer.call(pid, {:lock_for_recovery, 1})

    :ok = GenServer.call(pid, {:unlock_after_recovery, Version.zero(), [{"log-a", log_stub}]})
  end

  defp slice(version, value) do
    Transaction.encode(%{mutations: [{:set, "key", value}], commit_version: version})
  end

  # Generous waits: under full-suite parallelism the stream round-trips
  # (discovery, pull, apply, read wake-up) share cores with everything else.
  defp await_value(pid, version, expected) do
    assert {:ok, ^expected} = GenServer.call(pid, {:get, "key", version, [wait_ms: 15_000]}, 20_000)
  end

  setup do
    tmp_dir = "/tmp/olivine_cutover_#{System.unique_integer([:positive])}"
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  test "a suffix ingested during compaction survives cutover via stream re-delivery",
       %{tmp_dir: tmp_dir} do
    {:ok, shard_server} = ReplayShardServer.start_link(self())
    {:ok, log_stub} = StubLog.start_link(shard_server)

    pid = start_worker(tmp_dir)
    unlock_with_stream(pid, log_stub)

    v1000 = Version.from_integer(1_000)
    v2000 = Version.from_integer(2_000)
    v3000 = Version.from_integer(3_000)

    # Prefix, applied through the live stream before compaction starts.
    :ok = ReplayShardServer.append(shard_server, v1000, slice(v1000, "a"))
    await_value(pid, v1000, "a")

    # Hold compaction open deterministically: capture the durable snapshot
    # now (exactly what Logic.start_compaction's task does, with the result
    # delivered to the test instead of the server), ingest a suffix, and
    # only then deliver the cutover message.
    state = :sys.get_state(pid)
    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, _, _, _, _, _, _, _, durable_version, _, _, _} = cutover_msg,
                   10_000

    Task.await(task)

    # The suffix: ingested and visible while "compaction" is still open.
    :ok = ReplayShardServer.append(shard_server, v2000, slice(v2000, "b"))
    :ok = ReplayShardServer.append(shard_server, v3000, slice(v3000, "c"))
    await_value(pid, v3000, "c")

    # Drain pull notifications so post-cutover positions stand alone.
    flush_pulls()

    # Cutover.
    send(pid, cutover_msg)

    # Every transaction accepted during compaction remains readable — the
    # materializer was never restarted.
    await_value(pid, v3000, "c")
    await_value(pid, v2000, "b")
    await_value(pid, v1000, "a")

    # The stream resumed from the compacted durable boundary: no gap
    # (nothing after the boundary skipped) and no duplicates (the rewound
    # index re-applies each version exactly once; pull positions only move
    # forward).
    resumed_from = Version.increment(durable_version)
    assert_receive {:pulled, ^resumed_from}, 15_000
    assert_pulls_monotonic(resumed_from)
  end

  test "a superseded puller's queued ingest is acknowledged and discarded",
       %{tmp_dir: tmp_dir} do
    {:ok, shard_server} = ReplayShardServer.start_link(self())
    {:ok, log_stub} = StubLog.start_link(shard_server)

    pid = start_worker(tmp_dir)
    unlock_with_stream(pid, log_stub)

    v1000 = Version.from_integer(1_000)
    v9000 = Version.from_integer(9_000)

    :ok = ReplayShardServer.append(shard_server, v1000, slice(v1000, "a"))
    await_value(pid, v1000, "a")

    # The test process is not the current puller: its batch must be
    # acknowledged (a dead puller's parked call must never hang) but not
    # applied — grafting it in would leave a gap below it.
    assert :ok = GenServer.call(pid, {:ingest, [slice(v9000, "stale")], v9000})

    assert {:error, :version_too_new} =
             GenServer.call(pid, {:get, "key", v9000, [wait_ms: 0]})

    # The real stream still flows.
    v2000 = Version.from_integer(2_000)
    :ok = ReplayShardServer.append(shard_server, v2000, slice(v2000, "b"))
    await_value(pid, v2000, "b")
  end

  describe "window advancement while a compaction is open" do
    # start_compaction/1 labels its output with the durable version at the
    # moment it starts, and the cutover looks exactly that version up in
    # `versions` to recover the index's paging parameters (server.ex:479).
    # A window advance evicts every entry below the new eviction point and
    # carries index_db.durable_version up with it, so an advance while a
    # compaction is open takes the cutover's anchor away.
    #
    # handle_continue(:advance_window, ...) has always deferred on
    # allow_window_advancement; the :timeout path applies a batch and then
    # advances, and must defer for the same reason.
    test "the :timeout path defers the advance, leaving the compaction's durable version in place",
         %{tmp_dir: tmp_dir} do
      state = flushed_state(tmp_dir, "ngl_deferred")

      # What start_compaction/1 captured when the operator asked to compact.
      compaction_version = Database.durable_version(state.database)
      assert Version.to_integer(compaction_version) > 0

      {:noreply, after_timeout, _continue} =
        Server.handle_info(:timeout, queue_third_transaction(%{state | allow_window_advancement: false}))

      assert version_present?(after_timeout, compaction_version)
      assert Database.durable_version(after_timeout.database) == compaction_version

      # The batch still landed — compaction pauses the window, not ingest.
      assert after_timeout.index_manager.current_version == Version.from_integer(30_000_000)

      Logic.shutdown(after_timeout)
    end

    test "with no compaction open the :timeout path still advances the window", %{tmp_dir: tmp_dir} do
      state = flushed_state(tmp_dir, "ngl_allowed")
      durable_before = Database.durable_version(state.database)

      {:noreply, after_timeout, _continue} = Server.handle_info(:timeout, queue_third_transaction(state))

      assert Database.durable_version(after_timeout.database) == Version.from_integer(20_000_000)
      refute version_present?(after_timeout, durable_before)

      Logic.shutdown(after_timeout)
    end
  end

  # Two applies five seconds (in version-time) apart: the second's window
  # advance evicts the first, so the durable version is v10_000_000 and is
  # the oldest entry still in `versions`.
  defp flushed_state(tmp_dir, name) do
    dir = Path.join(tmp_dir, name)
    File.mkdir_p!(dir)

    {:ok, state} = Logic.startup(:"olivine_#{name}_#{System.unique_integer([:positive])}", self(), name, dir, [])

    state
    |> apply_and_flush("k1", "v1", 10_000_000)
    |> apply_and_flush("k2", "v2", 20_000_000)
  end

  defp apply_and_flush(state, key, value, version_int) do
    {:ok, state, _version} = Logic.apply_transactions(state, [commit(key, value, version_int)])
    {:ok, state} = Logic.advance_window(%{state | known_committed_version: Version.from_integer(version_int)})
    state
  end

  # A third transaction, far enough above the second to make the window
  # advanceable again, left in the intake queue so handle_info(:timeout, ...)
  # takes the apply-then-advance branch rather than the empty-queue one.
  defp queue_third_transaction(state) do
    %{
      state
      | intake_queue: IntakeQueue.add_transactions(state.intake_queue, [commit("k3", "v3", 30_000_000)]),
        known_committed_version: Version.from_integer(30_000_000)
    }
  end

  defp commit(key, value, version_int) do
    encoded = Transaction.encode(%{mutations: [{:set, key, value}], read_conflicts: {nil, []}, write_conflicts: []})
    {:ok, with_version} = Transaction.add_commit_version(encoded, Version.from_integer(version_int))
    with_version
  end

  defp version_present?(state, version), do: Enum.any?(state.index_manager.versions, fn {v, _} -> v == version end)

  defp flush_pulls do
    receive do
      {:pulled, _} -> flush_pulls()
    after
      0 -> :ok
    end
  end

  # Non-decreasing, not strictly increasing: a puller whose parked long
  # poll times out re-polls the SAME position — no data is re-delivered,
  # so equality is harmless. Only a regression would mean re-applying
  # transactions the index already holds.
  defp assert_pulls_monotonic(last_seen) do
    receive do
      {:pulled, from} ->
        assert from >= last_seen, "pull position regressed: #{inspect(from)} after #{inspect(last_seen)}"
        assert_pulls_monotonic(from)
    after
      0 -> :ok
    end
  end
end
