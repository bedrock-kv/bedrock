defmodule Bedrock.ControlPlane.Distributor.PlaceholderIntegrationTest do
  @moduledoc """
  Drives a read through the whole client path - PointReads/RangeReads →
  StorageRacing → LayoutIndex → Materializer client API - against a layout
  whose shard slot holds the placeholder pid, proving that clients cannot
  tell the placeholder apart from a real materializer: the request parks,
  coverage arrives, and the client receives the real value.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.RangeReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
  end

  # A single data shard (tag 1) covering the whole keyspace.
  @shard_layout %{<<0xFF, 0xFF>> => {1, <<>>}}
  @version Version.from_integer(1)

  defp start_placeholder(opts \\ []) do
    start_supervised!(
      Placeholder.Server.child_spec(
        cluster: TestCluster,
        distributor: self(),
        shard_layout: @shard_layout,
        hold_ms: Keyword.get(opts, :hold_ms, 2_000)
      )
    )
  end

  defp start_stub(kvs) do
    start_supervised!(%{
      id: {StubMaterializer, System.unique_integer([:positive])},
      start: {StubMaterializer, :start_link, [kvs]}
    })
  end

  defp client_state(placeholder, opts \\ []) do
    layout_index =
      LayoutIndex.build_index(%{
        shard_layout: @shard_layout,
        shard_materializers: %{1 => placeholder}
      })

    %State{
      state: :valid,
      layout_index: layout_index,
      read_version: @version,
      fetch_timeout_in_ms: Keyword.get(opts, :fetch_timeout_in_ms, 1_000)
    }
  end

  test "a get through the full client path parks, then succeeds after coverage" do
    placeholder = start_placeholder()
    state = client_state(placeholder)

    task = Task.async(fn -> PointReads.get_key(state, "apple") end)

    # The client's read is parked; the placeholder signaled coverage demand.
    assert_receive {:"$gen_cast", {:coverage_demand, 1}}

    stub = start_stub(%{"apple" => "red"})
    :ok = Placeholder.notify_covered(placeholder, 1, stub)

    assert {%State{}, {:ok, {"apple", "red"}}} = Task.await(task, 2_000)
  end

  test "a range read through the full client path parks, then succeeds after coverage" do
    placeholder = start_placeholder()
    state = client_state(placeholder)

    task = Task.async(fn -> RangeReads.get_range(state, {"a", "c"}, 100) end)

    assert_receive {:"$gen_cast", {:coverage_demand, 1}}

    stub = start_stub(%{"apple" => "red", "banana" => "yellow", "zebra" => "striped"})
    :ok = Placeholder.notify_covered(placeholder, 1, stub)

    assert {%State{}, {:ok, {[{"apple", "red"}, {"banana", "yellow"}], false}}} = Task.await(task, 2_000)
  end

  test "a get through the full client path fails with :unavailable when coverage never arrives" do
    placeholder = start_placeholder(hold_ms: 30)
    state = client_state(placeholder)

    task = Task.async(fn -> PointReads.get_key(state, "apple") end)

    assert_receive {:"$gen_cast", {:coverage_demand, 1}}

    assert {%State{}, {:failure, %{unavailable: [^placeholder]}}} = Task.await(task, 2_000)
  end

  test "after coverage, stale clients still racing the placeholder are forwarded" do
    placeholder = start_placeholder()
    stub = start_stub(%{"apple" => "red"})
    :ok = Placeholder.notify_covered(placeholder, 1, stub)

    # This client's layout still points at the placeholder (stale TSL).
    state = client_state(placeholder)

    assert {%State{}, {:ok, {"apple", "red"}}} = PointReads.get_key(state, "apple")
    refute_receive {:"$gen_cast", {:coverage_demand, _tag}}, 50
  end
end
