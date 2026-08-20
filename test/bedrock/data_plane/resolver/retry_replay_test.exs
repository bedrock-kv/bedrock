defmodule Bedrock.DataPlane.Resolver.RetryReplayTest do
  @moduledoc """
  Pins the resolver's reply replay for retried batches (bedrock-q67.22
  prerequisite).

  A commit proxy retries a resolve call when the reply is lost (timeout,
  network). The batch was already processed - conflict history advanced,
  verdicts recorded - so the retry must REPLAY the original abort set, never
  recompute or fabricate one. FDB keeps `outstandingBatches` for exactly
  this; Bedrock caches the abort set per processed version, pruned with the
  same retention horizon as conflict history, and recomputes only the
  metadata window (which is differential by the caller's ack, so recomputing
  is the correct, idempotent choice).

  A retry below the retention floor - or for a version this resolver never
  processed - has no truthful answer, so it fails explicitly instead of
  fabricating an all-aborted reply that would tell clients "aborted" for
  transactions the system may have committed.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp v(n), do: Version.from_integer(n)

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

  defp write_tx(key) do
    Transaction.encode(%{
      mutations: [{:set, key, "v"}],
      read_conflicts: [],
      write_conflicts: [{key, key <> <<0>>}]
    })
  end

  defp read_tx(key, read_version) do
    Transaction.encode(%{
      mutations: [{:set, "other", "v"}],
      read_conflicts: {read_version, [{key, key <> <<0>>}]},
      write_conflicts: [{"other", "other" <> <<0>>}]
    })
  end

  defp resolve(resolver, last_v, next_v, transactions) do
    parent = self()

    spawn_link(fn ->
      result =
        Resolver.resolve_transactions(resolver, 1, last_v, next_v, transactions, [[], []], metadata_ack: {parent, nil})

      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, result}, 1_000
    result
  end

  test "a retried batch replays the original abort set, not abort-all" do
    resolver = start_resolver()

    # v1 writes "k"; the v2 batch holds one clean write and one transaction
    # that read "k" at v0 - the read is stale, so exactly index 1 aborts.
    assert {:ok, [], _window} = resolve(resolver, v(0), v(1), [write_tx("k")])
    assert {:ok, [1], _window} = resolve(resolver, v(1), v(2), [write_tx("a"), read_tx("k", v(0))])

    # The reply was lost; the proxy retries the identical batch. The original
    # verdict must come back - not "all aborted".
    assert {:ok, [1], _window} = resolve(resolver, v(1), v(2), [write_tx("a"), read_tx("k", v(0))])
  end

  test "a replayed reply is stable across repeated retries" do
    resolver = start_resolver()

    assert {:ok, [], _} = resolve(resolver, v(0), v(1), [write_tx("k")])
    assert {:ok, [], _} = resolve(resolver, v(1), v(2), [write_tx("b")])

    assert {:ok, [], _} = resolve(resolver, v(0), v(1), [write_tx("k")])
    assert {:ok, [], _} = resolve(resolver, v(0), v(1), [write_tx("k")])
  end

  test "a retry for a version this resolver never processed fails explicitly" do
    resolver = start_resolver()

    assert {:ok, [], _} = resolve(resolver, v(0), v(10), [write_tx("k")])

    # (v3, v7) is below last_version but was never a processed batch here: a
    # truthful replay is impossible, so no verdict may be invented.
    assert {:error, :version_beyond_retention} = resolve(resolver, v(3), v(7), [write_tx("x")])
  end

  test "replies below the retention floor are pruned and retries fail explicitly" do
    # 1ms retention = 1000 versions; sweep on every call.
    resolver = start_resolver(version_retention_ms: 1, sweep_interval_ms: 0)

    assert {:ok, [], _} = resolve(resolver, v(0), v(1), [write_tx("k")])

    # Advance far past the horizon so the sweep drops the cached reply.
    assert {:ok, [], _} = resolve(resolver, v(1), v(2_000_000), [write_tx("z")])

    assert {:error, :version_beyond_retention} = resolve(resolver, v(0), v(1), [write_tx("k")])
  end
end
