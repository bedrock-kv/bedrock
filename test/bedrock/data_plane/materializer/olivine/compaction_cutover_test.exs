defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionCutoverTest do
  @moduledoc """
  Compaction cutover must not lose transactions ingested while the
  background compaction ran (bedrock-qzr.19).

  The cutover rewinds the index to the compacted durable snapshot. Every
  transaction applied after that snapshot must come back — not by special
  bookkeeping, but the same way recovery restores a suffix: the stream
  puller is stopped and restarted from the durable boundary, and the
  stream re-delivers everything after it.
  """
  use ExUnit.Case, async: true

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

  # One new key per version, so a version applied twice is visible in the count.
  defp distinct_slice(version, key) do
    Transaction.encode(%{mutations: [{:set, key, "v"}], commit_version: version})
  end

  # A read-modify-write, unlike every other slice here: applying it twice
  # lands on a different value than applying it once.
  defp add_slice(version, key, amount) do
    Transaction.encode(%{mutations: [{:atomic, :add, key, <<amount::little-64>>}], commit_version: version})
  end

  defp await_key(pid, version, key) do
    assert {:ok, "v"} = GenServer.call(pid, {:get, key, version, [wait_ms: 15_000]}, 20_000)
  end

  defp await_counter(pid, version, expected) do
    assert {:ok, <<^expected::little-64>>} =
             GenServer.call(pid, {:get, "ctr", version, [wait_ms: 15_000]}, 20_000)
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

  # The rewind is the whole point of the cutover, so the key count has to
  # rewind with it: the replayed suffix adds its keys a second time, and a
  # count carried over from the live index would count them twice.
  test "the key count rewinds with the index and is rebuilt by the replay",
       %{tmp_dir: tmp_dir} do
    {:ok, shard_server} = ReplayShardServer.start_link(self())
    {:ok, log_stub} = StubLog.start_link(shard_server)

    pid = start_worker(tmp_dir)
    unlock_with_stream(pid, log_stub)

    v1000 = Version.from_integer(1_000)
    v2000 = Version.from_integer(2_000)
    v3000 = Version.from_integer(3_000)

    :ok = ReplayShardServer.append(shard_server, v1000, distinct_slice(v1000, "k1"))
    await_key(pid, v1000, "k1")

    state = :sys.get_state(pid)
    {:ok, task} = Logic.start_compaction(state)
    assert_receive {:compaction_ready, _, _, _, _, _, _, _, _, _, _, _} = cutover_msg, 10_000
    Task.await(task)

    :ok = ReplayShardServer.append(shard_server, v2000, distinct_slice(v2000, "k2"))
    :ok = ReplayShardServer.append(shard_server, v3000, distinct_slice(v3000, "k3"))
    await_key(pid, v3000, "k3")

    assert {:ok, %{n_keys: 3}} = GenServer.call(pid, {:info, [:n_keys]})

    send(pid, cutover_msg)

    # After the replay all three keys are back — and counted exactly once.
    await_key(pid, v3000, "k3")
    assert {:ok, %{n_keys: 3}} = GenServer.call(pid, {:info, [:n_keys]})
    assert {:ok, %{key_ranges: [{"k1", "k3"}]}} = GenServer.call(pid, {:info, [:key_ranges]})
  end

  # The compacted files are labelled with the durable version, so they must
  # hold the durable page map and nothing above it: the cutover rewinds to
  # that boundary and the stream re-delivers everything after it. A page
  # carrying an effect from above the boundary gets that effect applied a
  # second time on top of itself — invisible for an idempotent set, a
  # doubled increment for an atomic.
  test "an atomic applied above the durable boundary is not applied twice by the replay",
       %{tmp_dir: tmp_dir} do
    {:ok, shard_server} = ReplayShardServer.start_link(self())
    {:ok, log_stub} = StubLog.start_link(shard_server)

    pid = start_worker(tmp_dir)
    unlock_with_stream(pid, log_stub)

    v1000 = Version.from_integer(1_000)
    v2000 = Version.from_integer(2_000)

    :ok = ReplayShardServer.append(shard_server, v1000, add_slice(v1000, "ctr", 1))
    await_counter(pid, v1000, 1)

    state = :sys.get_state(pid)
    {:ok, task} = Logic.start_compaction(state)
    assert_receive {:compaction_ready, _, _, _, _, _, _, _, _, _, _, _} = cutover_msg, 10_000
    Task.await(task)

    send(pid, cutover_msg)

    # A marker below which the whole replayed suffix has landed: the stream
    # is ordered, so v2000 being readable means v1000 was applied first.
    :ok = ReplayShardServer.append(shard_server, v2000, distinct_slice(v2000, "marker"))
    await_key(pid, v2000, "marker")

    await_counter(pid, v2000, 1)
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
