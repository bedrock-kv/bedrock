defmodule Bedrock.DataPlane.Resolver.MetadataWindowDistributionTest do
  @moduledoc """
  Pins the resolver's metadata-window distribution semantics that the commit
  proxy's ordering soundness currently depends on (bedrock-q67.15 gating audit).

  ## Why this test exists

  The commit proxy applies metadata windows in the SERVER process, but the
  windows are *sent* from per-batch finalization tasks, so two batches' windows
  can arrive at the server out of commit-version order (batch N+1's task can
  win the race to the mailbox). `Metadata.apply_updates/2` drops any window
  whose entries are all at or below the already-applied version.

  Dropping an earlier-versioned window that arrives late is only lossless
  because the resolver keys `proxy_progress` by the *caller* pid - and each
  batch resolves from a fresh finalization task pid - so `last_seen` is always
  nil and every reply carries the FULL retained window. Later windows are
  therefore supersets of earlier ones, and applying the later one first already
  includes everything the dropped earlier one carried.

  If you make these windows truly differential (per-proxy identity, pruning),
  this test will fail - which is the point: you must simultaneously give the
  commit proxy in-order window application (or another gap-detection scheme),
  otherwise the late-arriving earlier window is dropped and its mutations are
  LOST from proxy metadata (fatal once the shard map rides this pipe, q67.9).
  See bedrock-q67 / the metadata window protocol follow-up ticket.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp encode_tx(key) do
    Transaction.encode(%{
      mutations: [{:set, key, "v"}],
      read_conflicts: [],
      write_conflicts: [{key, key <> <<0>>}]
    })
  end

  # Resolve from a fresh pid, exactly like a commit proxy finalization task does
  defp resolve_from_fresh_task(resolver, last_v, next_v, key, metadata) do
    parent = self()

    spawn_link(fn ->
      result = Resolver.resolve_transactions(resolver, 1, last_v, next_v, [encode_tx(key)], [metadata])
      send(parent, {:resolved, result})
    end)

    assert_receive {:resolved, result}, 2_000
    result
  end

  test "windows returned to successive per-batch task pids are supersets (commit proxy ordering relies on this)" do
    resolver =
      start_supervised!(
        {ResolverServer,
         [
           lock_token: :crypto.strong_rand_bytes(32),
           key_range: {"", <<0xFF, 0xFF>>},
           epoch: 1,
           last_version: Version.zero(),
           director: self(),
           cluster: __MODULE__
         ]}
      )

    v0 = Version.zero()
    v1 = Version.from_integer(1)
    v2 = Version.from_integer(2)

    mutation1 = {:set, <<0xFF, "/system/shard_keys/a">>, :erlang.term_to_binary(1)}
    mutation2 = {:set, <<0xFF, "/system/shard_keys/b">>, :erlang.term_to_binary(2)}

    assert {:ok, [], window1} = resolve_from_fresh_task(resolver, v0, v1, "k1", [mutation1])
    assert {:ok, [], window2} = resolve_from_fresh_task(resolver, v1, v2, "k2", [mutation2])

    assert window1 == [{v1, [mutation1]}]

    # The load-bearing invariant: the later batch's window contains everything
    # the earlier batch's window did, so the commit proxy may safely drop an
    # earlier window that loses the task->server mailbox race.
    assert window2 == [{v1, [mutation1]}, {v2, [mutation2]}]
  end
end
