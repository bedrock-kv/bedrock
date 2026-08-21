defmodule Bedrock.DataPlane.Materializer.Olivine.DisplacementTest do
  @moduledoc """
  A materializer decides its own retirement by rejoin validation (FDB's
  storage-server rejoin against a commit proxy's txnStateStore): on each
  relayed layout push from a progressed epoch, it asks a commit proxy
  whether the committed `materializers/<tag>` entry still names it.
  Absence or another worker's id is an authoritative verdict; errors are
  not — the next push revalidates.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Materializer.Olivine.Server
  alias Bedrock.DataPlane.Materializer.Olivine.State

  defmodule StubProxy do
    @moduledoc false
    use GenServer

    def start_link(reply), do: GenServer.start_link(__MODULE__, reply)

    @impl true
    def init(reply), do: {:ok, reply}

    @impl true
    def handle_call({:resolve_materializer, _tag}, _from, reply), do: {:reply, reply, reply}
  end

  defp state(overrides) do
    struct!(%State{id: "mat-1", shard_num: 3, epoch: 2, foreman: self()}, overrides)
  end

  defp tsl(epoch, proxies), do: %{epoch: epoch, proxies: proxies, logs: %{}, sequencer: nil, resolvers: []}

  test "the keyspace naming another worker retires this one" do
    {:ok, proxy} = StubProxy.start_link({:ok, {"someone-else", "node@host"}})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1"}}
    end)
  end

  test "the keyspace naming no materializer for the tag retires this one" do
    {:ok, proxy} = StubProxy.start_link({:error, :not_found})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1"}}
    end)
  end

  test "the keyspace still naming this worker keeps it" do
    {:ok, proxy} = StubProxy.start_link({:ok, {"mat-1", "node@host"}})

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "a locked proxy is not a verdict — revalidate on the next push" do
    {:ok, proxy} = StubProxy.start_link({:error, :locked})

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "an unreachable proxy is not a verdict" do
    {:ok, proxy} = StubProxy.start_link({:ok, {"mat-1", "node@host"}})
    GenServer.stop(proxy)

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "the completing push of our own locked epoch validates — strays are judged, not immortal" do
    # Every recovery locks every advertised materializer into its epoch,
    # so a stray (bootstrap contest loser, empty leftover) sees the
    # completing push at its own epoch. It must still be validated —
    # otherwise it is re-locked every recovery and never retired.
    {:ok, proxy} = StubProxy.start_link({:error, :not_found})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(2, [proxy])}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1"}}
    end)
  end

  test "a push older than our lock never validates — an in-flight recovery's past may not judge us" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(1, [])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "a never-locked resurrection is judged by any completed layout" do
    {:ok, proxy} = StubProxy.start_link({:error, :not_found})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(1, [proxy])}, state(epoch: nil))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1"}}
    end)
  end

  test "a static materializer (no shard assignment) ignores pushes" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(9, [])}, state(shard_num: nil, epoch: nil))
  end

  test "a push with no proxies cannot validate — keep" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [])}, state([]))
  end
end
