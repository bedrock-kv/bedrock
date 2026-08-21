defmodule Bedrock.DataPlane.CommitProxy.MetadataDistributionIntegrationTest do
  @moduledoc """
  End-to-end integration tests for the \\xFF system metadata distribution
  pipeline (bedrock-q67.15).

  A committed transaction carrying a system-key mutation flows through:

      commit proxy (extract) -> resolver (MetadataAccumulator) ->
      commit proxy (parse + apply into structured State.metadata)

  Uses a real Sequencer.Server, a real Resolver.Server, and a real
  CommitProxy.Server; only the log is faked (established seam).
  """
  use ExUnit.Case, async: false

  import Bedrock.Test.TelemetryTestHelper

  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.Server, as: CommitProxyServer
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Sequencer.Server, as: SequencerServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component), do: :"metadata_dist_test_#{component}"
  end

  # Fake log that always accepts pushes (established DI seam)
  defmodule FakeLog do
    @moduledoc false
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, %{})

    def init(state), do: {:ok, state}

    def handle_call({:push, _transaction, _last_commit_version, _kcv}, _from, state), do: {:reply, :ok, state}
  end

  setup do
    director = self()
    epoch = 1
    lock_token = :crypto.strong_rand_bytes(32)

    sequencer =
      start_supervised!(
        {SequencerServer,
         [
           cluster: TestCluster,
           otp_name: :metadata_dist_test_sequencer,
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
           commit_proxy_count: 2
         ]}
      )

    log = start_supervised!({FakeLog, []})

    resolver_layout = ResolverLayout.from_layout(%{resolvers: [{"", resolver}]})

    # Single shard (tag 0) covering the entire keyspace, system keys included
    routing_snapshot = %{
      shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}},
      log_map: %{0 => "log_1"},
      log_services: %{"log_1" => log},
      materializers: %{0 => {"wkr_sys", "n1@host"}},
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
          empty_transaction_timeout_ms: 60_000,
          lock_token: lock_token,
          sequencer: sequencer,
          resolver_layout: resolver_layout
        )
      )

    :ok = GenServer.call(proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_snapshot})

    %{proxy: proxy, resolver: resolver, epoch: epoch}
  end

  defp encode_tx(mutations, conflict_key) do
    Transaction.encode(%{
      mutations: mutations,
      read_conflicts: [],
      write_conflicts: [{conflict_key, conflict_key <> <<0>>}]
    })
  end

  # System mode: these transactions write \xFF metadata keys, which
  # user-mode commits are rejected for at ingress.
  defp commit!(proxy, epoch, mutations, conflict_key) do
    assert {:ok, version, _index} =
             GenServer.call(proxy, {:commit, epoch, encode_tx(mutations, conflict_key), :system}, 5_000)

    version
  end

  # Poll until condition is met (metadata application is asynchronous)
  defp wait_until(condition_fn, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(condition_fn, deadline)
  end

  defp do_wait_until(condition_fn, deadline) do
    if condition_fn.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        do_wait_until(condition_fn, deadline)
      else
        flunk("Condition not met before timeout")
      end
    end
  end

  defp proxy_applied_version(proxy), do: :sys.get_state(proxy).applied_version

  defp proxy_shard(proxy, end_key) do
    case :gb_trees.lookup(end_key, :sys.get_state(proxy).routing_data.shards) do
      {:value, {tag, _start}} -> tag
      :none -> nil
    end
  end

  test "a user-mode commit writing a system key is rejected end-to-end while the proxy keeps serving", %{
    proxy: proxy,
    epoch: epoch
  } do
    bad_key = <<0xFF, "/system/forbidden">>
    tx = encode_tx([{:set, bad_key, "x"}], "user_key")

    assert {:error, {:key_out_of_range, ^bad_key}} = GenServer.call(proxy, {:commit, epoch, tx, :user}, 5_000)

    # The rejection was per-transaction: the proxy is alive and commits still flow.
    assert commit!(proxy, epoch, [{:set, "after_rejection", "v"}], "after_rejection")
  end

  test "system-key mutation flows commit -> resolver accumulator -> parsed proxy metadata", %{
    proxy: proxy,
    resolver: resolver,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("m")
    encoded_tag = Values.encode_shard_key_entry(7, "")

    version =
      commit!(
        proxy,
        epoch,
        [{:set, "user_key", "user_value"}, {:set, shard_key, encoded_tag}],
        "user_key"
      )

    # 1. The resolver's MetadataAccumulator captured the system-key mutation
    #    at the batch's commit version.
    resolver_entries = MetadataAccumulator.entries(:sys.get_state(resolver).metadata_window)
    assert {^version, [{[{:set, ^shard_key, ^encoded_tag}], true}]} = List.keyfind(resolver_entries, version, 0)

    # 2. The commit proxy's routing view - the consumer of the metadata
    #    stream - contains the boundary, and the ack advanced to the batch.
    wait_until(fn -> proxy_shard(proxy, "m") == 7 end)

    assert proxy_applied_version(proxy) == version

    # 3. The resolver tracks the served floor under the commit proxy SERVER's
    #    stable identity, not the per-batch finalization task pid.
    assert %{^proxy => served} = :sys.get_state(resolver).last_served
    assert served == version
  end

  test "metadata updates arrive ordered across multiple batches; later value wins", %{
    proxy: proxy,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("m")

    _v1 = commit!(proxy, epoch, [{:set, shard_key, Values.encode_shard_key_entry(7, "")}], "key_a")
    wait_until(fn -> proxy_shard(proxy, "m") == 7 end)

    v2 = commit!(proxy, epoch, [{:set, shard_key, Values.encode_shard_key_entry(9, "")}], "key_b")
    wait_until(fn -> proxy_shard(proxy, "m") == 9 end)

    assert proxy_applied_version(proxy) == v2
  end

  test "a non-system mutation never pollutes metadata", %{proxy: proxy, resolver: resolver, epoch: epoch} do
    commit!(proxy, epoch, [{:set, "plain_key", "plain_value"}], "plain_key")

    # Give the pipeline a moment to (incorrectly) deliver anything
    Process.sleep(100)

    assert MetadataAccumulator.entries(:sys.get_state(resolver).metadata_window) == []
    # Windows are exact and always served: the ack advances with every batch
    # even when the window is empty - but nothing polluted the stream.
  end

  describe "fetch_routing/2 - the GetKeyServerLocations analogue" do
    test "a locked proxy refuses routing requests until recover_from seeds it", %{epoch: epoch} do
      locked_proxy =
        start_supervised!(
          CommitProxyServer.child_spec(
            cluster: TestCluster,
            director: self(),
            epoch: epoch,
            instance: 1,
            max_latency_in_ms: 1,
            max_per_batch: 10,
            empty_transaction_timeout_ms: 60_000,
            lock_token: :crypto.strong_rand_bytes(32),
            sequencer: self(),
            resolver_layout: ResolverLayout.from_layout(%{resolvers: []})
          ),
          id: :locked_proxy
        )

      assert {:error, :locked} = CommitProxy.fetch_routing(locked_proxy)
    end

    test "an unlocked proxy serves the seeded projection", %{proxy: proxy} do
      assert {:ok, projection} = CommitProxy.fetch_routing(proxy)

      assert projection == %{
               shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}},
               materializers: %{0 => {"wkr_sys", "n1@host"}}
             }
    end

    test "committed shard and materializer mutations reach the served projection", %{proxy: proxy, epoch: epoch} do
      mutations = [
        {:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(7, "")},
        {:set, SystemKeys.materializer_key(7), Values.encode_materializer_ref("wkr_new", "n2@host")}
      ]

      version = commit!(proxy, epoch, mutations, "routing_key")
      wait_until(fn -> proxy_applied_version(proxy) == version end)

      assert {:ok, projection} = CommitProxy.fetch_routing(proxy)
      assert projection.shard_layout["m"] == {7, ""}
      assert projection.materializers[7] == {"wkr_new", "n2@host"}

      # The projection is exactly the client slice - no log wiring leaks.
      assert projection |> Map.keys() |> Enum.sort() == [:materializers, :shard_layout]
    end

    test "commits still flow while routing is being served", %{proxy: proxy, epoch: epoch} do
      assert {:ok, _} = CommitProxy.fetch_routing(proxy)
      assert commit!(proxy, epoch, [{:set, "after_fetch", "v"}], "after_fetch")
      assert {:ok, _} = CommitProxy.fetch_routing(proxy)
    end
  end

  test "routing families update the routing view; unknown system keys are ignored", %{proxy: proxy, epoch: epoch} do
    mutations = [
      {:set, SystemKeys.shard_key("g"), Values.encode_shard_key_entry(3, "")},
      {:set, SystemKeys.layout_log("log-abc"), Values.encode_tag_list([0, 1])},
      {:set, <<0xFF, "/system/future/feature">>, "opaque"}
    ]

    version = commit!(proxy, epoch, mutations, "families_key")

    wait_until(fn -> proxy_applied_version(proxy) == version end)

    routing_data = :sys.get_state(proxy).routing_data
    assert proxy_shard(proxy, "g") == 3
    assert "log-abc" in Map.values(routing_data.log_map)
    # Unknown families ride the window harmlessly (forward compatibility) -
    # the version still advances so the resolver can prune.
    assert proxy_applied_version(proxy) == version
  end
end
