defmodule Bedrock.ControlPlane.Coordinator.TslReplayTest do
  @moduledoc """
  A Link that registers after the layout is already stable must still learn
  it (bedrock-qzr.22). Broadcasts only reach subscribers that existed when
  the layout was published; registration itself must replay the current
  snapshot, or a late-joining node keeps a nil TSL — clients stall and its
  foreman never receives the reconciliation trigger.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State

  defp follower_state(overrides) do
    # A follower coordinator: registration subscribes locally and forwards
    # the durable write to the leader, so the handler can run without Raft.
    struct!(
      State,
      Map.merge(
        %{
          my_node: :me@nowhere,
          leader_node: :leader@nowhere,
          otp_name: :tsl_replay_test_coordinator,
          tsl_subscribers: MapSet.new()
        },
        overrides
      )
    )
  end

  defp register(state) do
    Server.handle_call({:register_node_resources, self(), [], []}, {self(), make_ref()}, state)
  end

  test "registration replays the current layout to the new subscriber" do
    tsl = %{id: "layout-1", epoch: 7, logs: %{}, services: %{}}
    state = follower_state(%{transaction_system_layout: tsl})

    assert {:noreply, updated} = register(state)

    # The same message shape as a live broadcast: Link's existing handler
    # caches it and forwards it to the local foreman unchanged.
    assert_receive {:tsl_updated, ^tsl}
    assert MapSet.member?(updated.tsl_subscribers, self())
  end

  test "with no current layout, nothing is replayed" do
    state = follower_state(%{transaction_system_layout: nil})

    assert {:noreply, _updated} = register(state)

    refute_receive {:tsl_updated, _}, 50
  end

  test "duplicate registration is idempotent: one subscription, same snapshot each time" do
    tsl = %{id: "layout-1", epoch: 7, logs: %{}, services: %{}}
    state = follower_state(%{transaction_system_layout: tsl})

    assert {:noreply, state} = register(state)
    assert {:noreply, state} = register(state)

    assert_receive {:tsl_updated, ^tsl}
    assert_receive {:tsl_updated, ^tsl}
    assert MapSet.size(state.tsl_subscribers) == 1
  end
end
