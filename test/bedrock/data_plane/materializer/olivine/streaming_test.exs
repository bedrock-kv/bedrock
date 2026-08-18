defmodule Bedrock.DataPlane.Materializer.Olivine.StreamingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Streaming
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # A scripted ShardServer stand-in: replies to {:pull, from, limit, timeout}
  # with the next scripted response, notifying the test process of each pull.
  defmodule StubShardServer do
    @moduledoc false
    use GenServer

    def start_link(script, notify), do: GenServer.start_link(__MODULE__, {script, notify})

    @impl true
    def init({script, notify}), do: {:ok, %{script: script, notify: notify}}

    @impl true
    def handle_call({:pull, from, _limit, _timeout}, _from, %{script: [next | rest]} = state) do
      send(state.notify, {:pulled, from})

      case next do
        {:reply, response} -> {:reply, response, %{state | script: rest}}
        {:stop, response} -> {:stop, :normal, response, %{state | script: rest}}
      end
    end

    def handle_call({:pull, from, _limit, _timeout}, _from, %{script: []} = state) do
      send(state.notify, {:pulled, from})
      {:reply, {:error, :timeout}, state}
    end
  end

  # A log stand-in that introduces ShardServers from a queue: each discovery
  # call hands out the next one (the last is handed out repeatedly).
  defmodule StubLog do
    @moduledoc false
    use GenServer

    def start_link(shard_servers), do: GenServer.start_link(__MODULE__, List.wrap(shard_servers))

    @impl true
    def init(shard_servers), do: {:ok, shard_servers}

    @impl true
    def handle_call({:get_shard_server, _shard_id}, _from, [shard_server]),
      do: {:reply, {:ok, shard_server}, [shard_server]}

    def handle_call({:get_shard_server, _shard_id}, _from, [shard_server | rest]),
      do: {:reply, {:ok, shard_server}, rest}
  end

  defp make_slice(version) do
    Transaction.encode(%{
      mutations: [{:set, "key", "value"}],
      commit_version: version
    })
  end

  defp services_for(logs_to_stubs) do
    Map.new(logs_to_stubs, fn {log_id, stub} -> {log_id, %{kind: :log, status: {:up, stub}}} end)
  end

  describe "candidate_log_ids/2" do
    test "is deterministic and drawn from the sorted log-id list" do
      logs = %{"log-c" => [], "log-a" => [], "log-b" => []}

      ids = Streaming.candidate_log_ids(7, logs)

      assert Enum.sort(ids) == ["log-a", "log-b", "log-c"]
      assert ids == Streaming.candidate_log_ids(7, logs)
    end

    test "returns an empty list with no logs" do
      assert Streaming.candidate_log_ids(0, %{}) == []
    end
  end

  describe "pull_once/1" do
    defp state_with(shard_server, ingest_fn) do
      %{
        shard_num: 1,
        next_version: Version.from_integer(1001),
        logs: %{},
        services: %{},
        failed_logs: %{},
        shard_server: shard_server,
        current_log_id: "log-a",
        ingest_fn: ingest_fn
      }
    end

    test "ingests pulled slices and advances past the last version" do
      v1100 = Version.from_integer(1100)
      v1200 = Version.from_integer(1200)
      kcv = Version.from_integer(1150)
      slices = [{v1100, make_slice(v1100)}, {v1200, make_slice(v1200)}]
      test_pid = self()

      {:ok, stub} = StubShardServer.start_link([{:reply, {:ok, slices, %{high_water: v1200, kcv: kcv}}}], self())

      ingest_fn = fn transactions, ingest_kcv ->
        send(test_pid, {:ingested, transactions, ingest_kcv})
        :ok
      end

      state = Streaming.pull_once(state_with(stub, ingest_fn))

      assert_receive {:ingested, ingested, ^kcv}
      assert length(ingested) == 2
      assert state.next_version == Version.increment(v1200)
    end

    test "rematerializes an empty-current reply as a heartbeat at the high-water" do
      hw = Version.from_integer(5000)
      kcv = Version.from_integer(4900)
      test_pid = self()

      {:ok, stub} = StubShardServer.start_link([{:reply, {:ok, [], %{high_water: hw, kcv: kcv}}}], self())

      ingest_fn = fn transactions, ingest_kcv ->
        send(test_pid, {:ingested, transactions, ingest_kcv})
        :ok
      end

      state = Streaming.pull_once(state_with(stub, ingest_fn))

      assert_receive {:ingested, [heartbeat], ^kcv}
      assert {:ok, ^hw} = Transaction.commit_version(heartbeat)
      assert {:ok, mutations} = Transaction.mutations(heartbeat)
      assert Enum.to_list(mutations) == []
      assert state.next_version == Version.increment(hw)
    end

    test "a pull timeout keeps the same server and position" do
      {:ok, stub} = StubShardServer.start_link([{:reply, {:error, :timeout}}], self())
      state = state_with(stub, fn _t, _k -> flunk("must not ingest") end)

      after_state = Streaming.pull_once(state)

      assert after_state.next_version == state.next_version
      assert after_state.shard_server == stub
    end

    test "a dead shard server triggers failover: server dropped, log circuit-broken" do
      {:ok, stub} = StubShardServer.start_link([], self())
      GenServer.stop(stub)

      state = Streaming.pull_once(state_with(stub, fn _t, _k -> flunk("must not ingest") end))

      assert state.shard_server == nil
      assert Map.has_key?(state.failed_logs, "log-a")
    end
  end

  describe "ensure_shard_server/1" do
    test "discovers the shard server through a log and remembers which log" do
      {:ok, shard_stub} = StubShardServer.start_link([], self())
      {:ok, log_stub} = StubLog.start_link(shard_stub)

      state = %{
        shard_num: 1,
        next_version: Version.from_integer(1),
        logs: %{"log-a" => []},
        services: services_for(%{"log-a" => log_stub}),
        failed_logs: %{},
        shard_server: nil,
        current_log_id: nil,
        ingest_fn: fn _t, _k -> :ok end
      }

      assert {:ok, state} = Streaming.ensure_shard_server(state)
      assert state.shard_server == shard_stub
      assert state.current_log_id == "log-a"
    end

    test "waits when every replica is circuit-broken" do
      future = System.monotonic_time(:millisecond) + 60_000

      state = %{
        shard_num: 1,
        next_version: Version.from_integer(1),
        logs: %{"log-a" => []},
        services: %{"log-a" => %{kind: :log, status: {:up, self()}}},
        failed_logs: %{"log-a" => future},
        shard_server: nil,
        current_log_id: nil,
        ingest_fn: fn _t, _k -> :ok end
      }

      assert {:wait, _state} = Streaming.ensure_shard_server(state)
    end
  end

  describe "the stream loop end to end" do
    test "streams data, heartbeats, and re-discovers when the shard server dies" do
      v100 = Version.from_integer(100)
      v200 = Version.from_integer(200)
      hw = Version.from_integer(300)
      test_pid = self()

      # First incarnation: one data reply, then dies on the next pull.
      {:ok, shard_first} =
        StubShardServer.start_link(
          [
            {:reply, {:ok, [{v100, make_slice(v100)}], %{high_water: v100, kcv: v100}}},
            {:stop, {:error, :unavailable}}
          ],
          self()
        )

      # After re-discovery: more data, then an empty-current (heartbeat), then quiet.
      {:ok, shard_second} =
        StubShardServer.start_link(
          [
            {:reply, {:ok, [{v200, make_slice(v200)}], %{high_water: v200, kcv: v200}}},
            {:reply, {:ok, [], %{high_water: hw, kcv: hw}}}
          ],
          self()
        )

      {:ok, log} = StubLog.start_link([shard_first, shard_second])

      ingest_fn = fn transactions, _kcv ->
        Enum.each(transactions, fn t -> send(test_pid, {:applied, Transaction.commit_version!(t)}) end)
        :ok
      end

      puller =
        Streaming.start_pulling(
          1,
          Version.from_integer(99),
          %{"log-a" => []},
          services_for(%{"log-a" => log}),
          ingest_fn
        )

      # on_exit runs outside the task's owner, so stop the raw pid directly
      on_exit(fn -> Process.exit(puller.pid, :kill) end)

      # Data flows; then the shard server dies; the loop circuit-breaks the
      # log, waits out the retry delay, re-discovers the fresh incarnation,
      # and picks up exactly where it left off — finishing with a heartbeat.
      assert_receive {:applied, ^v100}, 5_000
      assert_receive {:applied, ^v200}, 5_000
      assert_receive {:applied, ^hw}, 5_000

      # The re-discovered stream resumed from the puller's own position.
      resumed_from = Version.increment(hw)
      assert_receive {:pulled, ^resumed_from}, 1_000
    end
  end
end
