defmodule Bedrock.DataPlane.CommitProxy.MetadataWindowApplicationTest do
  @moduledoc """
  Pins the commit proxy server's application of resolver metadata windows
  (bedrock-q67.16).

  Windows arrive from per-batch finalization tasks and can therefore reach the
  server mailbox out of commit-version order. The protocol makes that safe by
  construction: every window covers `(from_version, to_version]` starting at
  the version this proxy last CONFIRMED applying, so concurrent in-flight
  windows overlap and a late-arriving earlier window is a subset of what has
  already been applied - the per-entry version guard skips it.

  The one unrecoverable case is a coverage gap (`from_version` beyond what the
  proxy has applied): the resolver has pruned history this proxy never saw, so
  its metadata can never be completed differentially. The proxy fails fast
  (exit -> director-driven recovery rebuilds it with full state).
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Metadata
  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

  defp v(n), do: Version.from_integer(n)

  defp state(metadata) do
    %State{
      cluster: __MODULE__,
      director: self(),
      epoch: 1,
      empty_transaction_timeout_ms: 1_000,
      mode: :running,
      metadata: metadata
    }
  end

  defp shard_set(key, tag), do: {:set, SystemKeys.shard_key(key), :erlang.term_to_binary(tag)}

  defp apply_window(state, window) do
    {:noreply, updated, _timeout} = Server.handle_info({:metadata_updates, window}, state)
    updated
  end

  test "overlapping windows apply idempotently; a late-arriving earlier window is a no-op" do
    e1 = {v(1), [shard_set("a", 1)]}
    e2 = {v(2), [shard_set("a", 2)]}

    # Batch 2's window (same ack, superset) wins the race to the mailbox.
    updated = apply_window(state(Metadata.new()), {nil, v(2), [e1, e2]})
    assert %Metadata{shards: %{"a" => 2}, version: version} = updated.metadata
    assert version == v(2)

    # Batch 1's window arrives late: subset of what is applied - no effect.
    updated = apply_window(updated, {nil, v(1), [e1]})
    assert %Metadata{shards: %{"a" => 2}, version: version} = updated.metadata
    assert version == v(2)
  end

  test "applied version advances to the window's to_version, not just the last entry's version" do
    # The window covers through v(3) even though the last mutation is at v(1);
    # acking v(3) lets the resolver prune fully.
    updated = apply_window(state(Metadata.new()), {nil, v(3), [{v(1), [shard_set("a", 1)]}]})
    assert %Metadata{shards: %{"a" => 1}, version: version} = updated.metadata
    assert version == v(3)
  end

  test "a window whose from_version exceeds the applied version is a coverage gap: fail fast" do
    initial = apply_window(state(Metadata.new()), {nil, v(1), [{v(1), [shard_set("a", 1)]}]})

    assert {:metadata_coverage_gap, _} =
             catch_exit(Server.handle_info({:metadata_updates, {v(5), v(6), [{v(6), [shard_set("a", 6)]}]}}, initial))
  end

  test "a gap is detected even when the window carries no entries" do
    assert {:metadata_coverage_gap, _} =
             catch_exit(Server.handle_info({:metadata_updates, {v(5), v(6), []}}, state(Metadata.new())))
  end
end
