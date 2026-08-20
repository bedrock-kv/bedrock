defmodule Bedrock.DataPlane.CommitProxy.ShardedMetadataDistributionIntegrationTest do
  @moduledoc """
  End-to-end tests for \\xFF system metadata distribution under SHARDED
  resolvers (bedrock-q67.17).

  In sharded mode each resolver only sees its own shard's conflicts, so a
  transaction can be aborted by one resolver while every other resolver sees
  no conflict for it. The global abort set is the union merged at the commit
  proxy. Correctness requirements pinned here:

  - metadata from a GLOBALLY-aborted transaction must never be distributed
    (not into any resolver's window, not into any proxy's metadata);
  - metadata from committed transactions must always be distributed;
  - version ordering of metadata is preserved.

  Uses a real Sequencer.Server, two real Resolver.Servers, and a real
  CommitProxy.Server; only the log is faked (established seam).
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.CommitProxy.Metadata
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
    def otp_name(component) when is_atom(component), do: :"sharded_metadata_dist_test_#{component}"
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
           otp_name: :sharded_metadata_dist_test_sequencer,
           director: director,
           epoch: epoch,
           last_committed_version: Version.zero()
         ]}
      )

    start_resolver = fn key_range ->
      start_supervised!(
        {ResolverServer,
         [
           lock_token: lock_token,
           key_range: key_range,
           epoch: epoch,
           last_version: Version.zero(),
           director: director,
           cluster: TestCluster
         ]},
        id: {:resolver, key_range}
      )
    end

    # Shard split at "m": resolver_a covers ["", "m"), resolver_b ["m", ...)
    resolver_a = start_resolver.({"", "m"})
    resolver_b = start_resolver.({"m", <<0xFF, 0xFF>>})

    log = start_supervised!({FakeLog, []})

    resolver_layout = ResolverLayout.from_layout(%{resolvers: [{"", resolver_a}, {"m", resolver_b}]})

    # Single storage shard (tag 0) covering the entire keyspace
    routing_snapshot = %{
      shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}},
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
          # Deferred-metadata confirmations ride SUBSEQUENT resolver calls; the
          # tests drive those deterministically with filler commits (see
          # commit_until/4) rather than relying on the empty-batch cadence
          # (which :sys.get_state polling would starve by resetting the
          # GenServer timeout).
          empty_transaction_timeout_ms: 60_000,
          lock_token: lock_token,
          sequencer: sequencer,
          resolver_layout: resolver_layout
        )
      )

    :ok = GenServer.call(proxy, {:recover_from, lock_token, sequencer, resolver_layout, routing_snapshot})

    %{proxy: proxy, resolver_a: resolver_a, resolver_b: resolver_b, epoch: epoch}
  end

  defp encode_tx(mutations, write_key, read_conflicts \\ []) do
    Transaction.encode(%{
      mutations: mutations,
      read_conflicts: read_conflicts,
      write_conflicts: [{write_key, write_key <> <<0>>}]
    })
  end

  # System mode: these transactions write \xFF metadata keys, which
  # user-mode commits are rejected for at ingress.
  defp commit(proxy, epoch, tx), do: GenServer.call(proxy, {:commit, epoch, tx, :system}, 5_000)

  defp commit!(proxy, epoch, tx) do
    assert {:ok, version, _index} = commit(proxy, epoch, tx)
    version
  end

  # Drives convergence deterministically: deferred-metadata confirmations and
  # window replies ride subsequent resolver calls, so commit filler
  # transactions until the condition holds (application is asynchronous).
  defp commit_until(proxy, epoch, condition_fn, attempts \\ 50)

  defp commit_until(_proxy, _epoch, _condition_fn, 0), do: flunk("Condition not met before timeout")

  defp commit_until(proxy, epoch, condition_fn, attempts) do
    if condition_fn.() do
      :ok
    else
      key = "filler_#{System.unique_integer([:positive])}"
      commit!(proxy, epoch, encode_tx([{:set, key, "x"}], key))
      Process.sleep(10)
      commit_until(proxy, epoch, condition_fn, attempts - 1)
    end
  end

  defp proxy_metadata(proxy), do: :sys.get_state(proxy).metadata

  defp resolver_metadata_mutations(resolver) do
    resolver
    |> :sys.get_state()
    |> Map.fetch!(:metadata_window)
    |> MetadataAccumulator.entries()
    |> Enum.flat_map(fn {_version, mutations} -> mutations end)
  end

  test "metadata from a transaction aborted by only ONE shard's resolver is never distributed", %{
    proxy: proxy,
    resolver_a: resolver_a,
    resolver_b: resolver_b,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("victim")
    encoded_tag = Values.encode_shard_key_entry(7, "")
    metadata_mutation = {:set, shard_key, encoded_tag}

    # Establish a conflicting write at "zebra" (resolver_b's shard).
    commit!(proxy, epoch, encode_tx([{:set, "zebra", "z1"}], "zebra"))

    # This transaction read "zebra" at version 0 (stale) and writes "apple"
    # (resolver_a's shard) plus a system metadata key. Resolver B aborts it on
    # the read conflict; resolver A sees only the "apple" write and does not.
    # The GLOBAL outcome is abort - its metadata must never surface anywhere.
    assert {:error, :aborted} =
             commit(
               proxy,
               epoch,
               encode_tx(
                 [{:set, "apple", "a1"}, metadata_mutation],
                 "apple",
                 {Version.zero(), [{"zebra", "zebra" <> <<0>>}]}
               )
             )

    # Drive more batches so any (incorrect) accumulation would be confirmed
    # and distributed.
    for i <- 1..5 do
      commit!(proxy, epoch, encode_tx([{:set, "carrot_#{i}", "c"}], "carrot_#{i}"))
    end

    Process.sleep(100)

    refute metadata_mutation in resolver_metadata_mutations(resolver_a)
    refute metadata_mutation in resolver_metadata_mutations(resolver_b)
    assert proxy_metadata(proxy).shards == %{}
  end

  test "metadata from committed transactions is distributed to resolver windows and proxy metadata", %{
    proxy: proxy,
    resolver_a: resolver_a,
    resolver_b: resolver_b,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("q")
    metadata_mutation = {:set, shard_key, Values.encode_shard_key_entry(3, "")}

    version = commit!(proxy, epoch, encode_tx([metadata_mutation], "apple"))

    # The proxy's structured metadata converges on the committed value...
    commit_until(proxy, epoch, fn -> proxy_metadata(proxy).shards == %{"q" => 3} end)

    # ...and each resolver's window holds the entry at the ORIGINAL commit
    # version (ordering is preserved by version, not by confirmation arrival).
    for resolver <- [resolver_a, resolver_b] do
      entries = MetadataAccumulator.entries(:sys.get_state(resolver).metadata_window)
      assert {^version, [{[^metadata_mutation], true}]} = List.keyfind(entries, version, 0)
    end
  end

  test "a mixed workload distributes only committed metadata, in version order", %{
    proxy: proxy,
    resolver_a: resolver_a,
    epoch: epoch
  } do
    shard_key = SystemKeys.shard_key("q")
    set_tag = fn tag -> {:set, shard_key, Values.encode_shard_key_entry(tag, "")} end

    # Conflict anchor in resolver_b's shard.
    commit!(proxy, epoch, encode_tx([{:set, "zebra", "z1"}], "zebra"))

    # Committed metadata (tag 1).
    v1 = commit!(proxy, epoch, encode_tx([set_tag.(1)], "apple"))

    # Globally-aborted metadata (tag 2): aborted by resolver_b's shard only.
    assert {:error, :aborted} =
             commit(
               proxy,
               epoch,
               encode_tx([set_tag.(2)], "apple", {Version.zero(), [{"zebra", "zebra" <> <<0>>}]})
             )

    # Committed metadata (tag 3).
    v3 = commit!(proxy, epoch, encode_tx([set_tag.(3)], "apple"))

    # Later committed value wins; the aborted tag 2 never surfaces.
    commit_until(proxy, epoch, fn -> proxy_metadata(proxy).shards == %{"q" => 3} end)
    assert %Metadata{shards: %{"q" => 3}} = proxy_metadata(proxy)

    # The resolver window carries exactly the committed entries, in version order.
    commit_until(proxy, epoch, fn ->
      versions =
        resolver_a
        |> :sys.get_state()
        |> Map.fetch!(:metadata_window)
        |> MetadataAccumulator.entries()
        |> Enum.map(&elem(&1, 0))

      v1 in versions and v3 in versions
    end)

    entries = MetadataAccumulator.entries(:sys.get_state(resolver_a).metadata_window)
    metadata_entries = for {v, muts} <- entries, v in [v1, v3], do: {v, muts}
    assert [{^v1, [tag1_mutation]}, {^v3, [tag3_mutation]}] = metadata_entries
    assert tag1_mutation == {[set_tag.(1)], true}
    assert tag3_mutation == {[set_tag.(3)], true}

    refute set_tag.(2) in resolver_metadata_mutations(resolver_a)
  end
end
