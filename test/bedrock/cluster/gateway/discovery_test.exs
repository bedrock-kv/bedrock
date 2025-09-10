defmodule Bedrock.Cluster.Gateway.DiscoveryTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.Discovery
  alias Bedrock.Cluster.Gateway.State

  describe "leader discovery and selection" do
    test "select_leader_from_responses/1 chooses coordinator with highest epoch" do
      responses = [
        {:node1, {:pong, 5, :coordinator_pid_1}},
        {:node2, {:pong, 8, :coordinator_pid_2}},
        {:node3, {:pong, 3, :coordinator_pid_3}}
      ]

      assert {:ok, {:coordinator_pid_2, 8}} =
               Discovery.select_leader_from_responses(responses)
    end

    test "select_leader_from_responses/1 filters out nil leaders" do
      responses = [
        {:node1, {:pong, 5, nil}},
        {:node2, {:pong, 8, :coordinator_pid_2}},
        {:node3, {:pong, 10, nil}}
      ]

      assert {:ok, {:coordinator_pid_2, 8}} =
               Discovery.select_leader_from_responses(responses)
    end

    test "select_leader_from_responses/1 returns error when no leaders available" do
      responses = [
        {:node1, {:pong, 5, nil}},
        {:node2, {:pong, 8, nil}},
        {:node3, {:pong, 3, nil}}
      ]

      assert Discovery.select_leader_from_responses(responses) == {:error, :unavailable}
    end

    test "select_leader_from_responses/1 handles empty responses" do
      assert Discovery.select_leader_from_responses([]) == {:error, :unavailable}
    end

    test "select_leader_from_responses/1 handles single leader" do
      responses = [
        {:node1, {:pong, 5, :coordinator_pid_1}}
      ]

      assert {:ok, {:coordinator_pid_1, 5}} =
               Discovery.select_leader_from_responses(responses)
    end
  end

  describe "change_coordinator/2" do
    setup do
      state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :coordinator_ref
      }

      %{state: state}
    end

    test "does not change when coordinator is the same", %{state: state} do
      assert %State{known_coordinator: :coordinator_ref} =
               Discovery.change_coordinator(state, :coordinator_ref)
    end

    test "sets known_coordinator to unavailable when requested", %{state: state} do
      assert %State{known_coordinator: :unavailable} =
               Discovery.change_coordinator(state, :unavailable)
    end
  end

  # Note: try_direct_coordinator_call/2 would require proper mocking setup
  # Integration tests would be more appropriate for testing this functionality
end
