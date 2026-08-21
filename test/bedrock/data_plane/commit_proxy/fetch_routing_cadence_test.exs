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

  describe "resolve_materializer" do
    defp routing_with(materializers), do: %{RoutingData.new_empty() | materializers: materializers}

    test "answers the tag's committed entry and resumes the cadence" do
      state = running_state(batch: nil, routing_data: routing_with(%{7 => {"w7", "node@host"}}))

      assert {:noreply, _t, 1_234} = Server.handle_call({:resolve_materializer, 7}, from(), state)
      assert_received {_ref, {:ok, {"w7", "node@host"}}}
    end

    test "an unnamed tag is authoritatively :not_found" do
      state = running_state(batch: nil, routing_data: routing_with(%{}))

      assert {:noreply, _t, 1_234} = Server.handle_call({:resolve_materializer, 7}, from(), state)
      assert_received {_ref, {:error, :not_found}}
    end

    test "with an open batch, the zero timeout is re-armed" do
      state = running_state(batch: %Batch{}, routing_data: routing_with(%{}))

      assert {:noreply, _t, 0} = Server.handle_call({:resolve_materializer, 1}, from(), state)
    end

    test "a locked proxy refuses — not a verdict" do
      state = struct!(%State{mode: :locked}, [])

      assert {:reply, {:error, :locked}, _t} = Server.handle_call({:resolve_materializer, 7}, from(), state)
    end
  end

  defp from, do: {self(), make_ref()}
end
