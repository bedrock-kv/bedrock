defmodule Bedrock.Cluster.Gateway.FetchCoordinatorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.Server
  alias Bedrock.Cluster.Gateway.State

  describe "get_known_coordinator/0" do
    test "returns error when coordinator unavailable" do
      state = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: :unavailable
      }

      assert {:reply, {:error, :unavailable}, ^state} =
               Server.handle_call(:get_known_coordinator, self(), state)
    end

    test "returns coordinator when available" do
      coordinator_ref = :test_coordinator_ref

      state = %State{
        node: Node.self(),
        cluster: DefaultTestCluster,
        known_coordinator: coordinator_ref
      }

      assert {:reply, {:ok, ^coordinator_ref}, ^state} =
               Server.handle_call(:get_known_coordinator, self(), state)
    end
  end
end
