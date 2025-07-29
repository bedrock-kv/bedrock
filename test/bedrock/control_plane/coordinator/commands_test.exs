defmodule Bedrock.ControlPlane.Coordinator.CommandsTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Commands

  describe "register_services/1" do
    test "creates valid register_services command" do
      services = [
        {"service_1", :storage, {:worker1, :node1@host}},
        {"service_2", :log, {:worker2, :node2@host}}
      ]

      command = Commands.register_services(services)

      assert {:register_services, %{services: ^services}} = command
    end

    test "validates service info format" do
      # Valid service info
      assert {:register_services, _} =
               Commands.register_services([{"svc", :storage, {:worker, :node@host}}])

      # Invalid service ID (not string)
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{:invalid_id, :storage, {:worker, :node@host}}])
      end

      # Invalid kind (not atom)
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{"service", "invalid_kind", {:worker, :node@host}}])
      end

      # Invalid worker ref (not {name, node})
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{"service", :storage, "invalid_ref"}])
      end

      # Invalid worker ref (name not atom)
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{"service", :storage, {"invalid_name", :node@host}}])
      end

      # Invalid worker ref (node not atom)
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{"service", :storage, {:worker, "invalid_node"}}])
      end

      # Invalid tuple structure
      assert_raise ArgumentError, ~r/Invalid service info/, fn ->
        Commands.register_services([{"service", :storage}])
      end
    end

    test "handles empty service list" do
      command = Commands.register_services([])
      assert {:register_services, %{services: []}} = command
    end
  end

  describe "deregister_services/1" do
    test "creates valid deregister_services command" do
      service_ids = ["service_1", "service_2"]
      command = Commands.deregister_services(service_ids)

      assert {:deregister_services, %{service_ids: ^service_ids}} = command
    end

    test "validates service IDs are strings" do
      # Valid service IDs
      assert {:deregister_services, _} = Commands.deregister_services(["service_1", "service_2"])

      # Invalid service ID (not string)
      assert_raise ArgumentError, ~r/Invalid service ID/, fn ->
        Commands.deregister_services([:invalid_id])
      end

      assert_raise ArgumentError, ~r/Invalid service ID/, fn ->
        Commands.deregister_services([123])
      end
    end

    test "handles empty service ID list" do
      command = Commands.deregister_services([])
      assert {:deregister_services, %{service_ids: []}} = command
    end
  end
end
