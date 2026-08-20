defmodule Bedrock.DataPlane.CommitProxy.MetadataWindowApplicationTest do
  @moduledoc """
  Pins the commit proxy server's serialized apply-and-route step
  (bedrock-q67.16, bedrock-q67.24).

  Each finalization task asks the server to apply its batch's committed
  metadata window - which covers through the batch's own version, its own
  committed metadata included - and hand back the immutable routing snapshot
  the batch pushes with. Requests are served strictly in proxy-local
  batch-sequence order: a request whose predecessor has not applied yet
  waits, so every batch routes with exactly the metadata at or below its own
  commit version. This mirrors FDB's postResolution ordering (apply
  metadata, then assign mutations to logs, one batch at a time), keyed like
  FDB's latestLocalCommitBatchLogging on a per-proxy counter - global
  sequencer versions interleave across proxies.

  Windows still overlap (each covers `(from, to]` from the version the proxy
  had confirmed when the batch spawned), so entries at or below the applied
  version are dropped before application.

  The one unrecoverable case is a coverage gap (`from_version` beyond what the
  proxy has applied): the resolver has pruned history this proxy never saw, so
  its metadata can never be completed differentially. The proxy fails fast
  (exit -> director-driven recovery rebuilds it with full state).
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  defp v(n), do: Version.from_integer(n)

  defp state(opts \\ []) do
    %State{
      cluster: __MODULE__,
      director: self(),
      epoch: 1,
      empty_transaction_timeout_ms: 1_000,
      mode: :running,
      routing_data: RoutingData.new_empty(),
      routed_seq: Keyword.get(opts, :routed_seq, 0)
    }
  end

  defp shard_set(key, tag), do: {:set, SystemKeys.shard_key(key), Values.encode_shard_key_entry(tag, "")}

  defp log_set(log_id), do: {:set, SystemKeys.layout_log(log_id), Values.encode_tag_list([1])}

  defp request(state, seq, commit_version, window) do
    from = {self(), make_ref()}
    result = Server.handle_call({:apply_metadata_and_route, seq, commit_version, window}, from, state)
    {result, elem(from, 1)}
  end

  defp apply_in_order(state, seq, commit_version, window) do
    {{:noreply, updated, _timeout}, ref} = request(state, seq, commit_version, window)
    assert_receive {^ref, {:ok, routing_data}}
    {updated, routing_data}
  end

  test "an in-order request applies the window and returns the routing snapshot in one step" do
    window = {nil, v(1), [{v(1), [shard_set("a", 7), log_set("log_a")]}]}

    {updated, routing_data} = apply_in_order(state(), 1, v(1), window)

    assert updated.applied_version == v(1)
    assert updated.routed_seq == 1
    assert routing_data.log_map == %{0 => "log_a"}
    assert :gb_trees.lookup("a", routing_data.shards) == {:value, {7, ""}}
    assert updated.routing_data == routing_data
  end

  test "an out-of-order request waits for its predecessor, then both reply in chain order" do
    w1 = {nil, v(1), [{v(1), [log_set("log_a")]}]}
    w2 = {v(1), v(2), [{v(2), [log_set("log_b")]}]}

    # Batch 2's request arrives first: no reply, request held.
    {{:noreply, held, _timeout}, ref2} = request(state(), 2, v(2), w2)
    refute_receive {^ref2, _}, 10

    # Batch 1's request arrives: applies, then batch 2 drains behind it.
    {{:noreply, updated, _timeout}, ref1} = request(held, 1, v(1), w1)

    assert_receive {^ref1, {:ok, routing1}}
    assert_receive {^ref2, {:ok, routing2}}
    assert routing1.log_map == %{0 => "log_a"}
    assert routing2.log_map == %{0 => "log_a", 1 => "log_b"}
    assert updated.routed_seq == 2
    assert updated.pending_applies == %{}
  end

  test "a batch's own committed metadata arrives in its window and routes its own push" do
    # The window covers through the batch's own commit version (verdicts
    # already resolved at the merge), so applying it gives the batch
    # same-batch visibility and honestly advances the ack to its version.
    window = {nil, v(2), [{v(2), [shard_set("a", 3), log_set("log_a")]}]}

    {updated, routing_data} = apply_in_order(state(), 1, v(2), window)

    assert routing_data.log_map == %{0 => "log_a"}
    assert :gb_trees.lookup("a", routing_data.shards) == {:value, {3, ""}}
    assert updated.applied_version == v(2)
  end

  test "overlapping windows apply idempotently: entries at or below the applied version are dropped" do
    e1 = {v(1), [log_set("log_a")]}
    e2 = {v(2), [log_set("log_b")]}

    {updated, _routing} = apply_in_order(state(), 1, v(1), {nil, v(1), [e1]})
    {updated, routing} = apply_in_order(updated, 2, v(2), {nil, v(2), [e1, e2]})

    assert routing.log_map == %{0 => "log_a", 1 => "log_b"}
    assert updated.applied_version == v(2)
  end

  test "applied version advances to the window's to_version, not just the last entry's version" do
    # The window covers through v(3) even though the last mutation is at v(1);
    # acking v(3) lets the resolver prune fully.
    {updated, _routing} = apply_in_order(state(), 1, v(3), {nil, v(3), [{v(1), [shard_set("a", 1)]}]})

    assert updated.applied_version == v(3)
  end

  test "a window whose from_version exceeds the applied version is a coverage gap: fail fast" do
    {initial, _routing} = apply_in_order(state(), 1, v(1), {nil, v(1), [{v(1), [shard_set("a", 1)]}]})

    from = {self(), make_ref()}

    assert {:metadata_coverage_gap, _} =
             catch_exit(
               Server.handle_call(
                 {:apply_metadata_and_route, 2, v(6), {v(5), v(6), [{v(6), [shard_set("a", 6)]}]}},
                 from,
                 initial
               )
             )
  end

  test "a gap is detected even when the window carries no entries" do
    from = {self(), make_ref()}

    assert {:metadata_coverage_gap, _} =
             catch_exit(Server.handle_call({:apply_metadata_and_route, 1, v(6), {v(5), v(6), []}}, from, state()))
  end

  test "a nil window advances the chain without touching metadata" do
    {updated, routing} = apply_in_order(state(), 1, v(1), nil)

    assert updated.routed_seq == 1
    assert updated.applied_version == nil
    assert routing.log_map == %{}
  end

  test "the chain is keyed on the proxy-local sequence, not sequencer version numbering" do
    # With multiple proxies the global sequencer interleaves versions across
    # them, so this proxy's consecutive batches have non-adjacent versions.
    # The chain must still link: sequence 1 then 2, whatever the versions.
    {updated, _routing} = apply_in_order(state(), 1, v(100), {nil, v(100), []})
    {updated, routing} = apply_in_order(updated, 2, v(250), {v(100), v(250), [{v(250), [log_set("log_a")]}]})

    assert updated.routed_seq == 2
    assert routing.log_map == %{0 => "log_a"}
  end

  test "requests are rejected while locked" do
    locked = %{state() | mode: :locked}
    {{:reply, {:error, :locked}, _state}, _ref} = request(locked, 1, v(1), nil)
  end
end
