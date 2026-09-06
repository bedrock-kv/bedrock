defmodule Bedrock.DataPlane.Materializer.Olivine.StreamReadTest do
  @moduledoc """
  A read at any version the client can legally hold must resolve from
  information already in the system — never by waiting for a future
  version to be minted.

  Wires the real streaming path — Demux.Server + ShardServer behind a stub
  log, olivine streaming its shard — pushes a fixed set of versions, and
  reads at them. Nothing else ever pushes: if any link in the chain waits
  on a timer or a future push instead of resolving event-driven, the read
  can never complete and the test fails on its call timeout.
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  # The log's only role in the materializer's data plane: introductions.
  defmodule StubLog do
    @moduledoc false
    use GenServer

    def start_link(demux), do: GenServer.start_link(__MODULE__, demux)

    @impl true
    def init(demux), do: {:ok, demux}

    @impl true
    def handle_call({:get_shard_server, shard_id}, _from, demux) do
      {:reply, Demux.Server.get_shard_server(demux, shard_id), demux}
    end
  end

  @shard 1

  defp make_transaction(mutations, version) do
    shard_index = if mutations == [], do: nil, else: [{@shard, length(mutations)}]

    Transaction.encode(%{
      mutations: mutations,
      shard_index: shard_index,
      commit_version: version
    })
  end

  defp wait_for_health_report(worker_id, pid, timeout \\ 5_000) do
    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, ^pid, {:ok, ^pid}}} -> :ok
    after
      timeout -> flunk("no health report within #{timeout}ms")
    end
  end

  setup do
    tmp_dir = "/tmp/olivine_latency_#{System.unique_integer([:positive])}"
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)

    backend = ObjectStorage.backend(LocalFilesystem, root: Path.join(tmp_dir, "objects"))

    # A real Demux, owned by the test (as the "log" for link purposes)
    {:ok, demux} =
      Demux.Server.start_link(cluster: "latency-test", object_storage: backend, log: self())

    {:ok, log} = StubLog.start_link(demux)

    worker_id = "latency-worker-#{System.unique_integer([:positive])}"
    otp_name = :"olivine_latency_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: Path.join(tmp_dir, "olivine"),
        cluster: __MODULE__,
        params: %{"shard_id" => @shard}
      )

    {:ok, olivine} = start_supervised(child_spec)
    wait_for_health_report(worker_id, olivine)

    {:ok, _pid, _info} = GenServer.call(olivine, {:lock_for_recovery, 1})

    :ok = GenServer.call(olivine, {:unlock_after_recovery, Version.zero(), [{"log-a", log}]})

    %{demux: demux, olivine: olivine}
  end

  test "a read at the freshly committed version resolves without any further push", %{
    demux: demux,
    olivine: olivine
  } do
    v1 = Version.from_integer(1_000_000)

    txn = make_transaction([{:set, "hello", "world"}], v1)

    # The commit proxy pushes with the KCV it knew at batch time — which
    # trails the version being pushed (here: zero).
    :ok = Demux.Server.push(demux, v1, txn, Version.zero())

    assert {:ok, "world"} = GenServer.call(olivine, {:get, "hello", v1, []}, 15_000)
  end

  test "a read at a later heartbeat-only version resolves without any further push", %{
    demux: demux,
    olivine: olivine
  } do
    v1 = Version.from_integer(1_000_000)
    v2 = Version.from_integer(1_100_000)

    :ok = Demux.Server.push(demux, v1, make_transaction([{:set, "hello", "world"}], v1), Version.zero())

    # A heartbeat advances the version stream without touching any shard;
    # the read version a client gets can sit on it.
    :ok = Demux.Server.push(demux, v2, make_transaction([], v2), v1)

    assert {:ok, "world"} = GenServer.call(olivine, {:get, "hello", v2, []}, 15_000)
  end
end
