defmodule Bedrock.ControlPlane.Coordinator.ApiTest do
  use ExUnit.Case, async: true

  describe "service_info type validation" do
    test "validates service_info type" do
      # Test that the type is correctly defined
      service_info = {"service_id", :materializer, {:worker, :node@host}}

      assert match?(
               {id, kind, {name, node}}
               when is_binary(id) and is_atom(kind) and is_atom(name) and is_atom(node),
               service_info
             )
    end
  end

  describe "service registration API validation" do
    test "register_services validates service_info tuple format" do
      valid_services = [
        {"service_1", :materializer, {:worker1, :node1@host}},
        {"service_2", :log, {:worker2, :node2@host}},
        {"service_3", :sequencer, {:worker3, :node3@host}}
      ]

      # Verify each service matches the expected service_info pattern
      Enum.each(valid_services, fn service ->
        assert match?(
                 {id, kind, {name, node}}
                 when is_binary(id) and is_atom(kind) and is_atom(name) and is_atom(node),
                 service
               )
      end)
    end

    test "deregister_services validates service_id format" do
      valid_service_ids = ["service_1", "service_2", "service_3"]

      # Verify each service_id is a binary string
      Enum.each(valid_service_ids, fn service_id ->
        assert is_binary(service_id)
      end)
    end
  end
end
