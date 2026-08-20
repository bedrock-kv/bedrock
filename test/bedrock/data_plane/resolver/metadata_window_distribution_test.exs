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

  Window entries carry `{mutations, local_verdict}` pairs per
  metadata-carrying transaction: each resolver records its own verdict and
  the commit proxy ANDs verdicts across all resolvers' windows to reach the
  global one (bedrock-q67.22, FDB's stateMutations relay).

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
  alias Bedrock.SystemKeys.Values

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
  defp resolve_from_fresh_task(resolver, last_v, next_v, key, metadata, ack, extra_opts \\ []) do
    parent = self()

    spawn_link(fn ->
      result =
        Resolver.resolve_transactions(
          resolver,
          1,
          last_v,
          next_v,
          [encode_tx(key)],
          [metadata],
          [metadata_ack: ack] ++ extra_opts
        )

      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, result}, 2_000
    result
  end

  defp v(n), do: Version.from_integer(n)

  setup do
    %{
      proxy: spawn_link(fn -> Process.sleep(:infinity) end),
      m1: {:set, <<0xFF, "/system/shard_keys/a">>, Values.encode_shard_key_entry(1, "")},
      m2: {:set, <<0xFF, "/system/shard_keys/b">>, Values.encode_shard_key_entry(2, "")},
      m3: {:set, <<0xFF, "/system/shard_keys/c">>, Values.encode_shard_key_entry(3, "")}
    }
  end

  test "windows are differential against the acked version; quiescent batches carry no window", %{
    proxy: proxy,
    m1: m1,
    m2: m2
  } do
    resolver = start_resolver()

    assert {:ok, [], {nil, to1, [{to1_entry, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})

    assert to1 == v(1)
    assert to1_entry == v(1)

    # Proxy confirmed v(1): only the new update rides the next reply.
    assert {:ok, [], {from2, to2, [{entry_v2, [{[^m2], true}]}]}} =
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

    assert {:ok, [], {nil, _, [{_, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})

    # The proxy never applied the first window (call timed out; reply lost).
    # Its next call carries the same ack - the resolver re-sends everything
    # since the confirmed version, so the second window is a superset of the
    # first and out-of-order arrival at the proxy is equally harmless.
    assert {:ok, [], {nil, to2, [{e1, [{[^m1], true}]}, {e2, [{[^m2], true}]}]}} =
             resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, nil})

    assert {to2, e1, e2} == {v(2), v(1), v(2)}
  end

  test "proxy_progress is keyed by stable proxy identity and the window is pruned to confirmed progress", %{
    proxy: proxy,
    m1: m1,
    m2: m2,
    m3: m3
  } do
    # 1ms retention = 1000 versions; pruning is capped at the retention
    # horizon, so drive the version stream past it.
    resolver = start_resolver(version_retention_ms: 1)

    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, v(1)})
    assert {:ok, [], _} = resolve_from_fresh_task(resolver, v(2), v(2500), "k3", [m3], {proxy, v(2)})

    state = :sys.get_state(resolver)

    # One entry per proxy - not one per finalization task pid.
    assert [{^proxy, {acked, last_seen}}] = Map.to_list(state.proxy_progress)
    assert {acked, last_seen} == {v(2), v(2500)}

    # Entries at or below the confirmed version (and older than the retention
    # horizon) are pruned.
    assert [{entry_version, [{[^m3], true}]}] = MetadataAccumulator.entries(state.metadata_window)
    assert entry_version == v(2500)
  end

  test "acks are monotone per proxy: a retried call carrying a stale ack neither regresses progress nor re-sends", %{
    proxy: proxy,
    m1: m1,
    m2: m2
  } do
    resolver = start_resolver()

    assert {:ok, [], {nil, _, [{_, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy, nil})

    assert {:ok, [], {_, _, [{_, [{[^m2], true}]}]}} =
             resolve_from_fresh_task(resolver, v(1), v(2), "k2", [m2], {proxy, v(1)})

    # Proxy confirmed through v(2); window pruned through v(2).
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(2), v(3), "k3", [], {proxy, v(2)})

    # A late retry re-carries the OLD ack v(1) (its reply was lost before the
    # proxy advanced). Recorded progress must not regress - the proxy already
    # applied through v(2) - and the resolver serves from the recorded ack,
    # so nothing is re-sent and no gap is signalled.
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(3), v(4), "k4", [], {proxy, v(1)})

    assert %{^proxy => {acked, _seen}} = :sys.get_state(resolver).proxy_progress
    assert acked == v(2)
  end

  test "pruning is capped at the retention horizon: acks alone cannot discard entries a not-yet-seen proxy needs", %{
    m1: m1
  } do
    # 1ms retention = 1000 versions. At epoch start one proxy can commit and
    # ack metadata before another proxy's FIRST call ever reaches the
    # resolver; only the retention cap keeps that entry alive for it.
    resolver = start_resolver(version_retention_ms: 1)
    proxy_a = spawn_link(fn -> Process.sleep(:infinity) end)
    proxy_b = spawn_link(fn -> Process.sleep(:infinity) end)

    assert {:ok, [], {nil, _, [{_, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1000), "k1", [m1], {proxy_a, nil})

    # proxy_a confirms v(1000) while the entry is still inside the horizon
    # (cutoff v(500)): the ack alone must NOT discard it.
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(1000), v(1500), "k2", [], {proxy_a, v(1000)})

    # proxy_b's first-ever call: full window, not a gap.
    assert {:ok, [], {nil, to, [{entry_version, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(1500), v(1600), "k3", [], {proxy_b, nil})

    assert {to, entry_version} == {v(1600), v(1000)}
  end

  test "a returning expired proxy whose ack covers every discarded entry gets a differential, not a gap", %{
    m1: m1
  } do
    # 1ms retention = 1000 versions. Only ONE metadata entry ever exists (at
    # v(1)); proxy_a confirms it via a window whose to_version is far ahead
    # (windows cover through the resolver's last_version, not the last entry).
    resolver = start_resolver(version_retention_ms: 1)
    proxy_a = spawn_link(fn -> Process.sleep(:infinity) end)
    proxy_b = spawn_link(fn -> Process.sleep(:infinity) end)

    assert {:ok, [], {nil, to_b, [{_, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(0), v(1), "k1", [m1], {proxy_b, nil})

    assert to_b == v(1)

    # proxy_b confirms v(1), then goes quiet.
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(1), v(2), "k2", [], {proxy_b, v(1)})

    # proxy_a first calls late: its window covers (nil, v(1500)] with the same
    # single entry, so it acks v(1500) - far beyond the entry's version.
    assert {:ok, [], {nil, to_a, [{_, [{[^m1], true}]}]}} =
             resolve_from_fresh_task(resolver, v(2), v(1500), "k3", [], {proxy_a, nil})

    assert to_a == v(1500)

    # proxy_b (seen at v(2)) falls out of the horizon; pruning follows
    # proxy_a's ack v(1500), discarding the entry at v(1).
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(1500), v(3100), "k4", [], {proxy_a, v(1500)})

    state = :sys.get_state(resolver)
    refute Map.has_key?(state.proxy_progress, proxy_b)
    assert MetadataAccumulator.entries(state.metadata_window) == []

    # proxy_b returns with ack v(1): it confirmed the only entry ever
    # discarded, so it missed NOTHING - it must get a plain (empty)
    # differential, not a gap-marked window that would force a full recovery.
    assert {:ok, [], nil} = resolve_from_fresh_task(resolver, v(3100), v(3200), "k5", [], {proxy_b, v(1)})
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
    assert [{entry_version, [{[^m3], true}]}] = MetadataAccumulator.entries(state.metadata_window)
    assert entry_version == v(5000)

    # proxy_b returns with an ack below the pruned floor: the window's
    # from_version exceeds what proxy_b applied - the proxy-side gap signal.
    assert {:ok, [], {from, to, [{_, [{[^m3], true}]}]}} =
             resolve_from_fresh_task(resolver, v(5000), v(5001), "k5", [], {proxy_b, nil})

    assert {from, to} == {v(3), v(5001)}
  end
end
