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

  alias Bedrock.DataPlane.CommitProxy.Metadata
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.Server, as: CommitProxyServer
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Sequencer.Server, as: SequencerServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

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

    def handle_call({:push, _transaction, _last_commit_version}, _from, state), do: {:reply, :ok, state}
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
           cluster: TestCluster
         ]}
      )

    log = start_supervised!({FakeLog, []})

    resolver_layout = ResolverLayout.from_layout(%{resolvers: [{"", resolver}]})

    shard_table = :ets.new(:metadata_dist_test_shards, [:ordered_set, :public])
    # Single shard (tag 0) covering the entire keyspace, system keys included
    :ets.insert(shard_table, {<<0xFF, 0xFF>>, 0})

    routing_data = %RoutingData{
      shard_table: shard_table,
      log_map: %{0 => "log_1"},
      log_services: %{"log_1" => log},
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
          resolver_layout: resolver_layout,
          routing_data: routing_data
        )
      )

    :ok = GenServer.call(proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_data})

    %{proxy: proxy, resolver: resolver, epoch: epoch}
  end

  defp encode_tx(mutations, conflict_key) do
    Transaction.encode(%{
      mutations: mutations,
      read_conflicts: [],
      write_conflicts: [{conflict_key, conflict_key <> <<0>>}]
    })
  end

  defp commit!(proxy, epoch, mutations, conflict_key) do
    assert {:ok, version, _index} = GenServer.call(proxy, {:commit, epoch, encode_tx(mutations, conflict_key)}, 5_000)

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

  defp proxy_metadata(proxy), do: :sys.get_state(proxy).metadata

  test "system-key mutation flows commit -> resolver accumulator -> parsed proxy metadata", %{
    proxy: proxy,
    resolver: resolver,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("m")
    encoded_tag = :erlang.term_to_binary(7)

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
    assert {^version, [{:set, ^shard_key, ^encoded_tag}]} = List.keyfind(resolver_entries, version, 0)

    # 2. The commit proxy's post-batch state contains the PARSED structured entry.
    wait_until(fn -> proxy_metadata(proxy).shards != %{} end)

    assert %Metadata{shards: %{"m" => 7}, version: ^version} = proxy_metadata(proxy)

    # 3. The resolver tracks progress under the commit proxy SERVER's stable
    #    identity, not the per-batch finalization task pid (bedrock-q67.16).
    assert %{^proxy => {_acked, _last_seen}} = :sys.get_state(resolver).proxy_progress
  end

  test "metadata updates arrive ordered across multiple batches; later value wins", %{
    proxy: proxy,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("m")

    _v1 = commit!(proxy, epoch, [{:set, shard_key, :erlang.term_to_binary(7)}], "key_a")
    wait_until(fn -> proxy_metadata(proxy).shards == %{"m" => 7} end)

    v2 = commit!(proxy, epoch, [{:set, shard_key, :erlang.term_to_binary(9)}], "key_b")
    wait_until(fn -> proxy_metadata(proxy).shards == %{"m" => 9} end)

    assert %Metadata{shards: %{"m" => 9}, version: ^v2} = proxy_metadata(proxy)
  end

  test "a non-system mutation never pollutes metadata", %{proxy: proxy, resolver: resolver, epoch: epoch} do
    commit!(proxy, epoch, [{:set, "plain_key", "plain_value"}], "plain_key")

    # Give the pipeline a moment to (incorrectly) deliver anything
    Process.sleep(100)

    assert MetadataAccumulator.entries(:sys.get_state(resolver).metadata_window) == []
    assert proxy_metadata(proxy) == Metadata.new()
  end

  test "multiple key families parse into their structured slots", %{proxy: proxy, epoch: epoch} do
    mutations = [
      {:set, SystemKeys.shard_key("g"), :erlang.term_to_binary(3)},
      {:set, SystemKeys.layout_log("log-abc"), :erlang.term_to_binary([0, 1])},
      {:set, SystemKeys.layout_services(), :erlang.term_to_binary(%{"log-abc" => %{kind: :log}})},
      {:set, SystemKeys.cluster_epoch(), :erlang.term_to_binary(1)},
      {:set, SystemKeys.cluster_parameters_desired_logs(), :erlang.term_to_binary(2)},
      {:set, SystemKeys.recovery_attempt(), :erlang.term_to_binary(1)}
    ]

    version = commit!(proxy, epoch, mutations, "families_key")

    wait_until(fn -> proxy_metadata(proxy).version == version end)

    metadata = proxy_metadata(proxy)
    assert metadata.shards == %{"g" => 3}
    assert metadata.logs == %{"log-abc" => [0, 1]}
    assert metadata.services == %{"log-abc" => %{kind: :log}}
    assert metadata.cluster == %{epoch: 1}
    assert metadata.parameters == %{desired_logs: 2}
    assert metadata.recovery == %{attempt: 1}
  end

  test "unknown system keys are ignored and counted in telemetry", %{proxy: proxy, epoch: epoch} do
    attach_telemetry_reflector(
      self(),
      [
        [:bedrock, :data_plane, :commit_proxy, :metadata_applied],
        [:bedrock, :data_plane, :commit_proxy, :unknown_key_skipped]
      ],
      "metadata-distribution-telemetry"
    )

    mutations = [
      {:set, SystemKeys.shard_key("z"), :erlang.term_to_binary(5)},
      {:set, <<0xFF, "/system/future/feature">>, "opaque"}
    ]

    version = commit!(proxy, epoch, mutations, "telemetry_key")

    wait_until(fn -> proxy_metadata(proxy).version == version end)

    metadata = proxy_metadata(proxy)
    assert metadata.shards == %{"z" => 5}
    refute Map.has_key?(metadata.shards, "/system/future/feature")

    {measurements, meta} = expect_telemetry([:bedrock, :data_plane, :commit_proxy, :metadata_applied], 2_000)
    assert measurements.count == 1
    assert :shard_key in meta.families

    {measurements, meta} = expect_telemetry([:bedrock, :data_plane, :commit_proxy, :unknown_key_skipped], 2_000)
    assert measurements.count == 1
    assert meta.keys == [<<0xFF, "/system/future/feature">>]
  end
end
