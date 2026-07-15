defmodule Bedrock.DataPlane.Resolver.MetadataWindowDistributionTest do
  @moduledoc """
  Pins the resolver's metadata-window distribution protocol (bedrock-q67.16).

  ## Protocol

  Every resolve call carries a `metadata_ack: {proxy_id, applied_version}` -
  the COMMIT PROXY SERVER's identity (stable per epoch, not the ephemeral
  per-batch finalization task pid) and the highest window `to_version` that
  proxy has confirmed applying. The resolver replies with a window
  `{from_version, to_version, entries}` (or `nil` when there is nothing to
  report) covering `(from_version, to_version]`.

  Because differentials are computed from the CONFIRMED-applied version:

  - a reply lost to a call timeout is simply re-sent on the proxy's next call
    (progress only advances via acks - timeout-retry safe);
  - two in-flight batches carrying the same ack receive overlapping windows,
    so out-of-order arrival at the proxy is lossless by construction (the
    proxy's per-entry version guard skips already-applied entries);
  - once a proxy confirms, the window is pruned through the minimum confirmed
    ack, and steady-state replies are `nil` - O(new updates) per batch.

  Proxies not seen within the resolver's version retention are expired from
  `proxy_progress` (bounding it to ~live proxies and unblocking pruning). A
  returning laggard whose ack predates the pruned floor receives a window with
  `from_version > its applied version` - the gap signal that forces the proxy
  to fail fast rather than continue with silently incomplete metadata.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp start_resolver(opts \\ []) do
    start_supervised!(
      {ResolverServer,
       [
         lock_token: :crypto.strong_rand_bytes(32),
         key_range: {"", <<0xFF, 0xFF>>},
         epoch: 1,
         last_version: Version.zero(),
         director: self(),
         cluster: __MODULE__
       ] ++ opts}
    )
  end

  defp encode_tx(key) do
    Transaction.encode(%{
      mutations: [{:set, key, "v"}],
      read_conflicts: [],
      write_conflicts: [{key, key <> <<0>>}]
    })
  end

  # Resolve from a fresh pid, exactly like a commit proxy finalization task
  # does - the metadata_ack carries the stable proxy identity, not this pid.
  defp resolve_from_fresh_task(resolver, last_v, next_v, key, metadata, ack) do
    parent = self()

    spawn_link(fn ->
      result =
        Resolver.resolve_transactions(resolver, 1, last_v, next_v, [encode_tx(key)], [metadata], metadata_ack: ack)

      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, result}, 2_000
    result
  end

  defp v(n), do: Version.from_integer(n)

  setup do
    %{
      proxy: spawn_link(fn -> Process.sleep(:infinity) end),
      m1: {:set, <<0xFF, "/system/shard_keys/a">>, :erlang.term_to_binary(1)},
      m2: {:set, <<0xFF, "/system/shard_keys/b">>, :erlang.term_to_binary(2)},
      m3: {:set, <<0xFF, "/system/shard_keys/c">>, :erlang.term_to_binary(3)}
    }
  end

  test "windows are differential against the acked version; quiescent batches carry no window", %{
    proxy: proxy,
    m1: m1,
    m2: m2
  } do
    resolver = start_resolver()

    assert {:ok, [], {nil, to1, [{to1_entry, [^m1]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})

    assert to1 == v(1)
    assert to1_entry == v(1)

    # Proxy confirmed v(1): only the new update rides the next reply.
    assert {:ok, [], {from2, to2, [{entry_v2, [^m2]}]}} =
             resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, v(1)})

    assert {from2, to2, entry_v2} == {v(1), v(2), v(2)}

    # Quiescence: everything confirmed, no new metadata - no window at all.
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(2), v(3), "k3", [], {proxy, v(2)})
  end

  test "unconfirmed updates are re-sent: a reply lost to a call timeout cannot open a gap", %{
    proxy: proxy,
    m1: m1,
    m2: m2
  } do
    resolver = start_resolver()

    assert {:ok, [], {nil, _, [{_, [^m1]}]}} = resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})

    # The proxy never applied the first window (call timed out; reply lost).
    # Its next call carries the same ack - the resolver re-sends everything
    # since the confirmed version, so the second window is a superset of the
    # first and out-of-order arrival at the proxy is equally harmless.
    assert {:ok, [], {nil, to2, [{e1, [^m1]}, {e2, [^m2]}]}} =
             resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, nil})

    assert {to2, e1, e2} == {v(2), v(1), v(2)}
  end

  test "proxy_progress is keyed by stable proxy identity and the window is pruned to confirmed progress", %{
    proxy: proxy,
    m1: m1,
    m2: m2,
    m3: m3
  } do
    resolver = start_resolver()

    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, v(1)})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(2), v(3), "k3", [m3], {proxy, v(2)})

    state = :sys.get_state(resolver)

    # One entry per proxy - not one per finalization task pid.
    assert [{^proxy, {acked, last_seen}}] = Map.to_list(state.proxy_progress)
    assert {acked, last_seen} == {v(2), v(3)}

    # Entries at or below the confirmed version are pruned.
    assert [{entry_version, [^m3]}] = MetadataAccumulator.entries(state.metadata_window)
    assert entry_version == v(3)
  end

  test "proxies not seen within version retention expire; a returning laggard gets a gap-marked window", %{
    m1: m1,
    m2: m2,
    m3: m3
  } do
    # 1ms retention = 1000 versions (versions are microsecond-based).
    resolver = start_resolver(version_retention_ms: 1)
    proxy_a = spawn_link(fn -> Process.sleep(:infinity) end)
    proxy_b = spawn_link(fn -> Process.sleep(:infinity) end)

    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy_a, nil})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(1), v(2), "k2", [], {proxy_b, nil})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(2), v(3), "k3", [m2], {proxy_a, v(1)})

    # proxy_b (last seen at v(2)) falls out of the retention horizon; pruning
    # then follows proxy_a's confirmed progress alone.
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(3), v(5000), "k4", [m3], {proxy_a, v(3)})

    state = :sys.get_state(resolver)
    refute Map.has_key?(state.proxy_progress, proxy_b)
    assert [{entry_version, [^m3]}] = MetadataAccumulator.entries(state.metadata_window)
    assert entry_version == v(5000)

    # proxy_b returns with an ack below the pruned floor: the window's
    # from_version exceeds what proxy_b applied - the proxy-side gap signal.
    assert {:ok, [], {from, to, [{_, [^m3]}]}} =
             resolve_from_fresh_task(resolver, v(5000), v(5001), "k5", [], {proxy_b, nil})

    assert {from, to} == {v(3), v(5001)}
  end
end
