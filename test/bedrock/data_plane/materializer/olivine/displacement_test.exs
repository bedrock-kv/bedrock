defmodule Bedrock.DataPlane.Materializer.Olivine.DisplacementTest do
  @moduledoc """
  A materializer decides its own retirement — no component decides
  another process's — by two routes, both FDB's.

  IN-BAND, the mid-epoch route: the commit proxy privatizes a membership
  CLEAR onto the shard's own stream, and the worker retires when it sees
  its own key cleared, at exactly the version its assignment ends. This
  is FDB's `applyPrivateData` path, and it is the route that covers
  healing and adoption (bedrock-q67.21.6).

  REJOIN VALIDATION, at the recovery boundary: on a relayed layout push
  from a progressed epoch, it asks a commit proxy whether the committed
  entry still names it. Absence or another worker's id is an
  authoritative verdict; errors are not — the next push revalidates.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Server
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

  defmodule StubProxy do
    @moduledoc false
    use GenServer

    def start_link(reply), do: GenServer.start_link(__MODULE__, reply)

    @impl true
    def init(reply), do: {:ok, reply}

    @impl true
    def handle_call({:materializer_members, _tag}, _from, reply), do: {:reply, reply, reply}
  end

  defp state(overrides) do
    struct!(%State{id: "mat-1", shard_num: 3, epoch: 2, foreman: self()}, overrides)
  end

  defp txn(mutations) do
    encoded = Transaction.encode(%{mutations: mutations, read_conflicts: {nil, []}, write_conflicts: []})
    {:ok, with_version} = Transaction.add_commit_version(encoded, Version.from_integer(10))
    with_version
  end

  defp queued(mutations), do: state(intake_queue: IntakeQueue.add_transactions(IntakeQueue.new(), [txn(mutations)]))

  defp private(key), do: Bedrock.end_of_keyspace() <> key

  defp tsl(epoch, proxies), do: %{epoch: epoch, proxies: proxies, logs: %{}, sequencer: nil, resolvers: []}

  test "the keyspace naming another worker retires this one" do
    {:ok, proxy} = StubProxy.start_link({:ok, %{"someone-else" => "node@host"}})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1", reporter}}
      assert reporter == self()
    end)
  end

  test "the keyspace naming no materializer for the tag retires this one" do
    {:ok, proxy} = StubProxy.start_link({:error, :not_found})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1", reporter}}
      assert reporter == self()
    end)
  end

  test "the keyspace still naming this worker keeps it" do
    {:ok, proxy} = StubProxy.start_link({:ok, %{"mat-1" => "node@host"}})

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _, _}}
  end

  test "a locked proxy is not a verdict — revalidate on the next push" do
    {:ok, proxy} = StubProxy.start_link({:error, :locked})

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _, _}}
  end

  test "an unreachable proxy is not a verdict" do
    {:ok, proxy} = StubProxy.start_link({:ok, %{"mat-1" => "node@host"}})
    GenServer.stop(proxy)

    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [proxy])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _, _}}
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

      assert_received {:"$gen_cast", {:worker_retired, "mat-1", reporter}}
      assert reporter == self()
    end)
  end

  test "a push older than our lock never validates — an in-flight recovery's past may not judge us" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(1, [])}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _, _}}
  end

  test "a never-locked resurrection is judged by any completed layout" do
    {:ok, proxy} = StubProxy.start_link({:error, :not_found})

    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(1, [proxy])}, state(epoch: nil))

      assert_received {:"$gen_cast", {:worker_retired, "mat-1", reporter}}
      assert reporter == self()
    end)
  end

  test "a static materializer (no shard assignment) ignores pushes" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(9, [])}, state(shard_num: nil, epoch: nil))
  end

  test "a push with no proxies cannot validate — keep" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(3, [])}, state([]))
  end

  test "a sibling replica's presence is not displacement — membership, not resolution" do
    # The set names this worker AND another materializer for the same
    # shard. Under set-valued membership (bedrock-q67.21.9) a shard may
    # legitimately have several materializers, so the only question is
    # whether the set still contains ME.
    {:ok, proxy} = StubProxy.start_link({:ok, %{"mat-1" => "node@host", "sibling" => "other@host"}})

    assert {:noreply, _t} =
             Server.handle_info({:tsl_updated, %{epoch: 2, proxies: [proxy]}}, state(id: "mat-1", shard_num: 3))
  end

  describe "in-band retirement (bedrock-q67.21.6)" do
    test "a privatized CLEAR of my own key retires me, from my own stream" do
      # The notice rides the same commit that removed the membership
      # entry, on the stream this worker already follows — so it retires
      # at exactly the version its assignment ends, with no recovery
      # push and no proxy round trip.
      t = queued([{:clear, private(SystemKeys.materializer_key(3, "mat-1"))}])

      capture_log(fn ->
        assert {:stop, {:shutdown, :displaced}, _t} = Server.handle_continue(:process_transactions, t)
        assert_received {:"$gen_cast", {:worker_retired, "mat-1", reporter}}
        assert reporter == self()
      end)
    end

    test "the notice names me: another worker's clear, or my id on another shard, is not my business" do
      # Membership is a set: a sibling leaving says nothing about me. The
      # worker id is IN the key, so this is FDB's own question — "is this
      # about me?" — not "does this value name someone else".
      mine = Logic.retirement_notice_key(state([]))

      refute Logic.retirement_notice?([txn([{:clear, private(SystemKeys.materializer_key(3, "someone-else"))}])], mine)
      refute Logic.retirement_notice?([txn([{:clear, private(SystemKeys.materializer_key(9, "mat-1"))}])], mine)
      assert Logic.retirement_notice?([txn([{:clear, mine}])], mine)
    end

    test "the unprivatized membership key is NOT a notice" do
      # Only the proxy-synthesized copy retires anyone. The ordinary
      # committed key is shard data for tag 0 and must never be read as a
      # verdict — otherwise every materializer watching the system shard
      # would retire on someone else's removal.
      mine = Logic.retirement_notice_key(state([]))

      refute Logic.retirement_notice?([txn([{:clear, SystemKeys.materializer_key(3, "mat-1")}])], mine)
    end

    test "a worker with no shard assignment has no notice key" do
      assert Logic.retirement_notice_key(state(shard_num: nil)) == nil
      refute Logic.retirement_notice?([txn([{:clear, private("anything")}])], nil)
    end
  end
end
