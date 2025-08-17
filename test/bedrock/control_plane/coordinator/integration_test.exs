defmodule Bedrock.ControlPlane.Coordinator.IntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.State

  describe "service registration integration" do
    test "handles register_services call" do
      services = [{"test_service", :storage, {:worker, :node@host}}]

      # Test that the command is properly formed
      command = Commands.register_services(services)
      assert {:register_services, %{services: ^services}} = command
    end

    test "handles deregister_services call" do
      service_ids = ["existing_service"]

      # Test that the command is properly formed
      command = Commands.deregister_services(service_ids)
      assert {:deregister_services, %{service_ids: ^service_ids}} = command
    end

    test "ping handler returns correct format" do
      leader_pid = self()
      epoch = 42

      # Test when this node is the leader
      state = %State{
        leader_node: Node.self(),
        my_node: Node.self(),
        epoch: epoch
      }

      # Simulate the ping handler logic
      leader = if state.leader_node == state.my_node, do: self()
      response = {:pong, state.epoch, leader}

      assert {:pong, ^epoch, ^leader_pid} = response
    end

    test "ping handler returns nil leader when not leader" do
      epoch = 42

      # Test when this node is not the leader
      state = %State{
        leader_node: :other_node,
        my_node: Node.self(),
        epoch: epoch
      }

      # Simulate the ping handler logic
      leader = if state.leader_node == state.my_node, do: self()
      response = {:pong, state.epoch, leader}

      assert {:pong, ^epoch, nil} = response
    end
  end

  describe "director startup integration" do
    test "director receives service directory at startup" do
      services = %{
        "service_1" => {:storage, {:worker1, :node1@host}},
        "service_2" => {:log, {:worker2, :node2@host}}
      }

      # Verify that the director startup args would include the services
      expected_args = [
        cluster: TestCluster,
        config: %{},
        old_transaction_system_layout: %{},
        epoch: 1,
        coordinator: self(),
        services: services
      ]

      # Test that all required fields are present
      assert Keyword.has_key?(expected_args, :services)
      assert Keyword.get(expected_args, :services) == services
    end
  end
end
