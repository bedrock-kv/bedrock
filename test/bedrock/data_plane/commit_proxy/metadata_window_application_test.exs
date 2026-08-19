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
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  defp v(n), do: Version.from_integer(n)

  defp state(metadata) do
    %State{
      cluster: __MODULE__,
      director: self(),
      epoch: 1,
      empty_transaction_timeout_ms: 1_000,
      mode: :running,
      metadata: metadata,
      routing_data: RoutingData.new_empty()
    }
  end

  defp shard_set(key, tag), do: {:set, SystemKeys.shard_key(key), Values.encode_shard_key_entry(tag, "")}

  defp log_set(log_id), do: {:set, SystemKeys.layout_log(log_id), Values.encode_tag_list([1])}

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

  test "window entries update routing data in the same step that advances the ack" do
    # A batch snapshots (routing_data, metadata.version) together at spawn.
    # The resolver keys its differential windows off the version, so routing
    # state must advance atomically with it - otherwise a batch could hold an
    # ack that promises entries its routing data never saw (bedrock-q67.24).
    updated = apply_window(state(Metadata.new()), {nil, v(1), [{v(1), [shard_set("a", 7), log_set("log_a")]}]})

    assert updated.metadata.version == v(1)
    assert updated.routing_data.log_map == %{0 => "log_a"}
    assert :ets.lookup(updated.routing_data.shard_table, "a") == [{"a", 7}]
  end

  test "a re-delivered window does not duplicate log entries" do
    window = {nil, v(1), [{v(1), [log_set("log_a")]}]}

    updated = apply_window(state(Metadata.new()), window)
    updated = apply_window(updated, window)

    assert updated.routing_data.log_map == %{0 => "log_a"}
  end

  test "an overlapping superset window applies only entries newer than the applied version" do
    e1 = {v(1), [log_set("log_a")]}
    e2 = {v(2), [log_set("log_b")]}

    updated = apply_window(state(Metadata.new()), {nil, v(1), [e1]})
    updated = apply_window(updated, {nil, v(2), [e1, e2]})

    assert updated.routing_data.log_map == %{0 => "log_a", 1 => "log_b"}
  end

  test "re-setting the same layout_log key at a newer version does not duplicate the log" do
    # A mid-epoch writer may legitimately re-set a layout_log key (e.g. a tag
    # change). The log keeps its index; only genuinely new logs append.
    updated = apply_window(state(Metadata.new()), {nil, v(1), [{v(1), [log_set("log_a")]}]})
    updated = apply_window(updated, {v(1), v(2), [{v(2), [log_set("log_a"), log_set("log_b")]}]})

    assert updated.routing_data.log_map == %{0 => "log_a", 1 => "log_b"}
  end

  test "a stale routing-data replacement message can no longer clobber window-applied state" do
    # Finalization tasks used to send {:routing_data_update, struct} after log
    # push; racing tasks made the last writer win, silently dropping log-map
    # changes. The message is retired - if one arrives, it is ignored.
    updated = apply_window(state(Metadata.new()), {nil, v(1), [{v(1), [log_set("log_a")]}]})

    assert {:noreply, after_stale} = Server.handle_info({:routing_data_update, RoutingData.new_empty()}, updated)

    assert after_stale.routing_data.log_map == %{0 => "log_a"}
  end
end
