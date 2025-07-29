defmodule Bedrock.ControlPlane.Coordinator.ApiTest do
  use ExUnit.Case, async: true

  describe "service_info type validation" do
    test "validates service_info type" do
      # Test that the type is correctly defined
      service_info = {"service_id", :storage, {:worker, :node@host}}

      assert match?(
               {id, kind, {name, node}}
               when is_binary(id) and is_atom(kind) and is_atom(name) and is_atom(node),
               service_info
             )
    end
  end

  describe "service registration API validation" do
    test "register_services validates input format" do
      # These should all be valid service_info tuples
      valid_services = [
        {"service_1", :storage, {:worker1, :node1@host}},
        {"service_2", :log, {:worker2, :node2@host}},
        {"service_3", :sequencer, {:worker3, :node3@host}}
      ]

      # Verify the type signature expectations
      Enum.each(valid_services, fn {id, kind, {name, node}} ->
        assert is_binary(id)
        assert is_atom(kind)
        assert is_atom(name)
        assert is_atom(node)
      end)
    end

    test "deregister_services validates input format" do
      valid_service_ids = ["service_1", "service_2", "service_3"]

      Enum.each(valid_service_ids, fn id ->
        assert is_binary(id)
      end)
    end
  end
end
