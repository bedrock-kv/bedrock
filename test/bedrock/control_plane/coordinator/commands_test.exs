defmodule Bedrock.ControlPlane.Coordinator.CommandsTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Commands

  # Test data
  @valid_services [
    {"service_1", :materializer, {:worker1, :node1@host}},
    {"service_2", :log, {:worker2, :node2@host}}
  ]

  @valid_service_ids ["service_1", "service_2"]

  describe "register_services/1" do
    test "creates valid register_services command" do
      assert {:register_services, %{services: @valid_services}} =
               Commands.register_services(@valid_services)
    end

    test "accepts valid service info format" do
      assert {:register_services, _} =
               Commands.register_services([{"svc", :materializer, {:worker, :node@host}}])
    end

    test "rejects invalid service info with descriptive errors" do
      invalid_services = [
        {[:invalid_id, :materializer, {:worker, :node@host}], "service ID must be string"},
        {["service", "invalid_kind", {:worker, :node@host}], "kind must be atom"},
        {["service", :materializer, "invalid_ref"], "worker ref must be {name, node} tuple"},
        {["service", :materializer, {"invalid_name", :node@host}], "worker name must be atom"},
        {["service", :materializer, {:worker, "invalid_node"}], "worker node must be atom"},
        {["service", :materializer], "incomplete service tuple"}
      ]

      for {service_list, _description} <- invalid_services do
        assert_raise ArgumentError, ~r/Invalid service info/, fn ->
          Commands.register_services([service_list])
        end
      end
    end

    test "handles empty service list" do
      assert {:register_services, %{services: []}} = Commands.register_services([])
    end
  end

  describe "deregister_services/1" do
    test "creates valid deregister_services command" do
      assert {:deregister_services, %{service_ids: @valid_service_ids}} =
               Commands.deregister_services(@valid_service_ids)
    end

    test "accepts valid service IDs" do
      assert {:deregister_services, _} = Commands.deregister_services(["service_1", "service_2"])
    end

    test "rejects non-string service IDs" do
      invalid_ids = [[:invalid_id], [123], [:atom, "mixed"]]

      for invalid_list <- invalid_ids do
        assert_raise ArgumentError, ~r/Invalid service ID/, fn ->
          Commands.deregister_services(invalid_list)
        end
      end
    end

    test "handles empty service ID list" do
      assert {:deregister_services, %{service_ids: []}} = Commands.deregister_services([])
    end
  end
end
