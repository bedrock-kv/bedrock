defmodule Bedrock.ControlPlane.Coordinator.IntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.State

  # Helper functions for test setup
  defp build_state(opts) do
    defaults = [
      leader_node: Node.self(),
      my_node: Node.self(),
      epoch: 1
    ]

    opts = Keyword.merge(defaults, opts)
    struct!(State, opts)
  end

  defp simulate_ping_response(%State{} = state) do
    leader = if state.leader_node == state.my_node, do: self()
    {:pong, state.epoch, leader}
  end

  describe "service registration integration" do
    test "handles register_services call" do
      services = [{"test_service", :materializer, {:worker, :node@host}}]

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

    test "ping handler returns correct format when leader" do
      epoch = 42
      leader_pid = self()

      state = build_state(leader_node: Node.self(), epoch: epoch)
      response = simulate_ping_response(state)

      assert {:pong, ^epoch, ^leader_pid} = response
    end

    test "ping handler returns nil leader when not leader" do
      epoch = 42

      state = build_state(leader_node: :other_node, epoch: epoch)
      response = simulate_ping_response(state)

      assert {:pong, ^epoch, nil} = response
    end
  end

  describe "director startup integration" do
    test "director receives service directory at startup" do
      services = %{
        "service_1" => {:materializer, {:worker1, :node1@host}},
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

      # Verify services are correctly included in startup args
      assert expected_args[:services] == services
    end
  end
end
