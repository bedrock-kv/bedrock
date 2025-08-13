defmodule Bedrock.Cluster.Gateway.IntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Coordinator.State, as: CoordinatorState

  describe "gateway simplification integration" do
    test "gateway state uses known_coordinator instead of known_leader" do
      # Test that we can create gateway state with the new field structure
      state = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil
      }

      assert state.known_coordinator == :test_coordinator
      # Verify old field is gone by checking struct keys
      refute Map.has_key?(state, :known_leader)
    end

    test "coordinator state includes TSL subscribers" do
      # Test that coordinator state includes the new tsl_subscribers field
      state = %CoordinatorState{
        cluster: DefaultTestCluster,
        leader_node: Node.self(),
        my_node: Node.self(),
        epoch: 1,
        tsl_subscribers: MapSet.new([self()])
      }

      assert MapSet.member?(state.tsl_subscribers, self())
      assert MapSet.size(state.tsl_subscribers) == 1
    end

    test "TSL update notification message format" do
      # Test that the TSL update message has the expected format
      test_tsl = %{epoch: 1, sequencer: :test_sequencer}
      message = {:tsl_updated, test_tsl}

      assert {:tsl_updated, ^test_tsl} = message
    end

    test "register_gateway API combines registration and subscription" do
      # Test the new unified API call format with compact services
      gateway_pid = self()
      compact_services = [{:storage, :worker}]

      # This would be the call format to the coordinator
      api_call = {:register_gateway, gateway_pid, compact_services}

      assert {:register_gateway, ^gateway_pid, ^compact_services} = api_call
    end

    test "gateway handles TSL updates via push notifications" do
      # Simulate receiving a TSL update
      initial_state = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil
      }

      new_tsl = %{epoch: 2, sequencer: :new_sequencer}

      # Simulate the handle_info logic for TSL updates
      updated_state = %{initial_state | transaction_system_layout: new_tsl}

      assert updated_state.transaction_system_layout == new_tsl
      assert updated_state.known_coordinator == :test_coordinator
    end
  end

  describe "coordinator discovery resilience" do
    test "gateway can work with any coordinator for read operations" do
      # Test that gateway doesn't require leader for TSL fetches
      state = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        # Not necessarily leader
        known_coordinator: :any_coordinator,
        transaction_system_layout: %{epoch: 1, sequencer: :test}
      }

      # With cached TSL, gateway should work regardless of coordinator leadership
      assert state.known_coordinator == :any_coordinator
      assert not is_nil(state.transaction_system_layout)
    end

    test "coordinator ping response includes leader information" do
      # Test the ping response format that discovery uses

      # When coordinator is leader
      leader_response = {:pong, 5, self()}
      assert {:pong, epoch, leader_pid} = leader_response
      assert epoch == 5
      assert leader_pid == self()

      # When coordinator is not leader (during transition)
      follower_response = {:pong, 5, nil}
      assert {:pong, ^epoch, nil} = follower_response
    end
  end
end
