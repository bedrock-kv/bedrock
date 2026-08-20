defmodule Bedrock.DataPlane.CommitProxy.FetchRoutingCadenceTest do
  @moduledoc """
  A routing fetch must not swallow the proxy's batch cadence: replying
  without re-arming the GenServer timeout would strand an open batch (its
  pending zero-timeout is cancelled by the arriving call) or silence the
  empty-transaction heartbeat. Pinned here as direct handler assertions -
  the integration tests are sequential and cannot see a stranded batch.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State

  defp running_state(overrides) do
    struct!(
      %State{
        mode: :running,
        empty_transaction_timeout_ms: 1_234,
        routing_data: RoutingData.new_empty()
      },
      overrides
    )
  end

  test "with no open batch, the heartbeat timeout is re-armed" do
    assert {:noreply, _t, 1_234} = Server.handle_call(:fetch_routing, from(), running_state(batch: nil))
  end

  test "with an open batch, the zero timeout is re-armed so the batch still finalizes" do
    assert {:noreply, _t, 0} = Server.handle_call(:fetch_routing, from(), running_state(batch: %Batch{}))
  end

  test "the reply arrives before the cadence resumes" do
    Server.handle_call(:fetch_routing, from(), running_state(batch: nil))
    assert_received {_ref, {:ok, %{shard_layout: %{}, materializers: %{}}}}
  end

  defp from, do: {self(), make_ref()}
end
