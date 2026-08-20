defmodule Bedrock.DataPlane.Resolver.ExactWindowRelayTest do
  @moduledoc """
  Pins the resolver's exact per-proxy metadata window relay (bedrock-q67.33).

  This is FDB's stateMutations relay: the resolver tracks each commit
  proxy's served floor (`last_served`, FDB's per-proxy `lastVersion`) and
  serves exact windows `(last_served, last_version]` computed entirely
  resolver-side - no acks, no overlap, no receiver-side filtering.
  Consecutive windows to one proxy tile exactly, so the proxy can assert
  each window's `from_version` equals its applied version. Entries carry
  `{mutations, local_verdict}` pairs for the proxy-side AND.

  Pruning discards entries every proxy has been served, but only once every
  one of the epoch's `commit_proxy_count` proxies has been served at least
  once - FDB's proxy-count gate - which structurally precludes discarding an
  entry a not-yet-heard-from proxy's first window still needs. A dead proxy
  freezes pruning only until the Director notices and recovers the epoch.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  defp v(n), do: Version.from_integer(n)

  defp start_resolver(opts \\ []) do
    start_supervised!(
      {ResolverServer,
       [
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

  defp shard_set(key, tag), do: {:set, SystemKeys.shard_key(key), Values.encode_shard_key_entry(tag, "")}

  # Resolve from a fresh pid, exactly like a commit proxy finalization task
  # does - proxy_id carries the stable proxy identity, not this pid.
  defp resolve_as(resolver, proxy_id, last_v, next_v, key, metadata) do
    parent = self()

    spawn_link(fn ->
      result =
        Resolver.resolve_transactions(resolver, 1, last_v, next_v, [encode_tx(key)], [metadata], proxy_id: proxy_id)

      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, result}, 1_000
    result
  end

  defp entries(resolver), do: MetadataAccumulator.entries(:sys.get_state(resolver).metadata_window)

  test "windows tile exactly: each from equals the previous to" do
    resolver = start_resolver()
    proxy = self()
    m1 = shard_set("a", 1)
    m2 = shard_set("b", 2)

    assert {:ok, [], {nil, to1, [{e1, [{[^m1], true}]}]}} = resolve_as(resolver, proxy, v(0), v(1), "k1", [m1])
    assert to1 == v(1)
    assert e1 == v(1)

    assert {:ok, [], {^to1, to2, [{e2, [{[^m2], true}]}]}} = resolve_as(resolver, proxy, v(1), v(2), "k2", [m2])
    assert to2 == v(2)
    assert e2 == v(2)
  end

  test "empty windows still tile and advance the served floor" do
    resolver = start_resolver()
    proxy = self()

    assert {:ok, [], {nil, to1, []}} = resolve_as(resolver, proxy, v(0), v(1), "k1", [])
    assert {:ok, [], {^to1, to2, []}} = resolve_as(resolver, proxy, v(1), v(2), "k2", [])
    assert to2 == v(2)
  end

  test "a second proxy's first window is full coverage from the epoch start" do
    resolver = start_resolver(commit_proxy_count: 2)
    proxy_a = spawn_link(fn -> Process.sleep(:infinity) end)
    proxy_b = spawn_link(fn -> Process.sleep(:infinity) end)
    m1 = shard_set("a", 1)

    assert {:ok, [], {nil, to1, [{_, [{[^m1], true}]}]}} = resolve_as(resolver, proxy_a, v(0), v(1), "k1", [m1])
    assert {:ok, [], {^to1, _, []}} = resolve_as(resolver, proxy_a, v(1), v(2), "k2", [])

    # B has never been served: its first window covers everything - the
    # count-gated pruning kept m1 alive for exactly this moment.
    assert {:ok, [], {nil, to_b, [{_, [{[^m1], true}]}]}} = resolve_as(resolver, proxy_b, v(2), v(3), "k3", [])
    assert to_b == v(3)
  end

  test "entries prune once every one of the epoch's proxies has been served" do
    resolver = start_resolver(commit_proxy_count: 2)
    proxy_a = spawn_link(fn -> Process.sleep(:infinity) end)
    proxy_b = spawn_link(fn -> Process.sleep(:infinity) end)
    m1 = shard_set("a", 1)

    assert {:ok, [], _} = resolve_as(resolver, proxy_a, v(0), v(1), "k1", [m1])
    # Only A served: the entry must survive for B's first contact.
    assert [{_, [{[^m1], true}]}] = entries(resolver)

    assert {:ok, [], _} = resolve_as(resolver, proxy_b, v(1), v(2), "k2", [])
    # Both served through the entry's version: pruned.
    assert entries(resolver) == []
  end

  test "a single-proxy epoch prunes as it serves" do
    resolver = start_resolver(commit_proxy_count: 1)
    proxy = self()
    m1 = shard_set("a", 1)

    assert {:ok, [], _} = resolve_as(resolver, proxy, v(0), v(1), "k1", [m1])
    assert entries(resolver) == []
  end

  test "local vetoes ride the window explicitly - absence is never a veto" do
    resolver = start_resolver()
    proxy = self()
    meta = shard_set("q", 9)

    # v1 writes "k"; the v2 metadata transaction read "k" at v0 - stale, so
    # this resolver's LOCAL verdict is a veto, recorded as {mutations, false}.
    assert {:ok, [], _} = resolve_as(resolver, proxy, v(0), v(1), "k", [])

    parent = self()

    conflicted =
      Transaction.encode(%{
        mutations: [{:set, "other", "v"}, elem({meta}, 0)],
        read_conflicts: {v(0), [{"k", "k" <> <<0>>}]},
        write_conflicts: [{"other", "other" <> <<0>>}]
      })

    spawn_link(fn ->
      result = Resolver.resolve_transactions(resolver, 1, v(1), v(2), [conflicted], [[meta]], proxy_id: parent)
      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, {:ok, [0], {_, _, [{entry_v, [{[^meta], false}]}]}}}, 1_000
    assert entry_v == v(2)
  end
end
