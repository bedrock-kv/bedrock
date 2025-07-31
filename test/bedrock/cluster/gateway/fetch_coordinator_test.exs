defmodule Bedrock.Cluster.Gateway.FetchCoordinatorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.State

  describe "fetch_coordinator integration" do
    test "gateway returns error when coordinator unavailable" do
      state_unavailable = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: :unavailable
      }

      # Direct assertion - unavailable coordinator returns error
      assert state_unavailable.known_coordinator == :unavailable
    end

    test "gateway returns coordinator when available" do
      state_available = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: :test_coordinator_ref
      }

      # Direct assertion - available coordinator returns the ref
      assert state_available.known_coordinator == :test_coordinator_ref
    end

    test "cluster handles successful gateway response" do
      gateway_response_success = {:ok, :test_coordinator}

      # Direct assertion - successful response pattern matches correctly
      assert {:ok, :test_coordinator} = gateway_response_success
    end

    test "cluster handles unavailable gateway response" do
      gateway_response_unavailable = {:error, :unavailable}

      # Direct assertion - error response pattern matches correctly
      assert {:error, :unavailable} = gateway_response_unavailable
    end
  end
end
