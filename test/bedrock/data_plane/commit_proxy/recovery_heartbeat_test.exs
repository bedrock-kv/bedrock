defmodule Bedrock.DataPlane.CommitProxy.RecoveryHeartbeatTest do
  @moduledoc """
  A proxy that has just been unlocked by `recover_from/5` must beat on its
  own, before any client arrives. FDB arms the batcher the moment the proxy
  becomes usable (`commitBatcherActor = commitBatcher(...)` in
  CommitProxyServer.actor.cpp, whose loop unconditionally `out.send`s the
  batch when MAX_COMMIT_BATCH_INTERVAL elapses - empty or not), so versions
  advance smoothly from recovery onward.

  Pinned twice: once on the handler's return, the way the sibling cadence
  tests do it, and once end to end on a live proxy - a real Sequencer.Server
  and Resolver.Server with only the log faked, so the heartbeat is observable
  as the push the empty transaction makes to the logs.
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.Server, as: CommitProxyServer
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Sequencer.Server, as: SequencerServer
  alias Bedrock.DataPlane.Version

  @heartbeat_ms 100

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component), do: :"recovery_heartbeat_test_#{component}"
  end

  # Fake log that accepts every push and tells the test about it, so the
  # heartbeat is observable from outside the proxy.
  defmodule ReportingLog do
    @moduledoc false
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    def init(test_pid), do: {:ok, test_pid}

    def handle_call({:push, transaction, last_commit_version, _kcv}, _from, test_pid) do
      send(test_pid, {:log_push, transaction, last_commit_version})
      {:reply, :ok, test_pid}
    end
  end

  test "the unlock reply arms the heartbeat timeout" do
    t = %State{mode: :locked, lock_token: "tok", empty_transaction_timeout_ms: 1_234}
    snapshot = %{shard_layout: %{}, log_map: %{}, log_services: %{}, replication_factor: 1}

    assert {:noreply, %State{mode: :running}, 1_234} =
             CommitProxyServer.handle_call(
               {:recover_from, "tok", :sequencer, :resolver_layout, snapshot},
               {self(), make_ref()},
               t
             )

    assert_received {_ref, :ok}
  end

  describe "a live proxy" do
    setup do
      director = self()
      epoch = 1
      lock_token = :crypto.strong_rand_bytes(32)

      sequencer =
        start_supervised!(
          {SequencerServer,
           [
             cluster: TestCluster,
             otp_name: :recovery_heartbeat_test_sequencer,
             director: director,
             epoch: epoch,
             last_committed_version: Version.zero()
           ]}
        )

      resolver =
        start_supervised!(
          {ResolverServer,
           [
             lock_token: lock_token,
             key_range: {"", <<0xFF, 0xFF>>},
             epoch: epoch,
             last_version: Version.zero(),
             director: director,
             cluster: TestCluster,
             commit_proxy_count: 1
           ]}
        )

      log = start_supervised!({ReportingLog, self()})

      resolver_layout = ResolverLayout.from_layout(%{resolvers: [{"", resolver}]})

      routing_snapshot = %{
        shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}},
        log_map: %{0 => "log_1"},
        log_services: %{"log_1" => log},
        materializers: %{0 => %{"wkr_sys" => "n1@host"}},
        replication_factor: 1
      }

      proxy =
        start_supervised!(
          CommitProxyServer.child_spec(
            cluster: TestCluster,
            director: director,
            epoch: epoch,
            instance: 0,
            max_latency_in_ms: 1,
            max_per_batch: 10,
            empty_transaction_timeout_ms: @heartbeat_ms,
            lock_token: lock_token,
            sequencer: sequencer,
            resolver_layout: resolver_layout
          )
        )

      %{
        proxy: proxy,
        lock_token: lock_token,
        sequencer: sequencer,
        resolver_layout: resolver_layout,
        routing_snapshot: routing_snapshot
      }
    end

    test "beats without any client traffic once unlocked", ctx do
      %{
        proxy: proxy,
        lock_token: lock_token,
        sequencer: sequencer,
        resolver_layout: resolver_layout,
        routing_snapshot: routing_snapshot
      } = ctx

      :ok = GenServer.call(proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_snapshot})

      # No commit, no fetch, nothing: the reply to recover_from is the only
      # thing that has happened to this proxy.
      assert_receive {:log_push, _transaction, _last_commit_version}, @heartbeat_ms * 5
    end

    test "that never got unlocked stays quiet", %{proxy: _proxy} do
      refute_receive {:log_push, _transaction, _last_commit_version}, @heartbeat_ms * 3
    end

    test "whose unlock was refused stays locked and quiet", ctx do
      %{proxy: proxy, sequencer: sequencer, resolver_layout: resolver_layout, routing_snapshot: routing_snapshot} = ctx

      assert {:error, :unauthorized} =
               GenServer.call(proxy, {:recover_from, "wrong_token", sequencer, resolver_layout, routing_snapshot})

      refute_receive {:log_push, _transaction, _last_commit_version}, @heartbeat_ms * 3
    end
  end
end
