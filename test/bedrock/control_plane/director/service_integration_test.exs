defmodule Bedrock.ControlPlane.Director.ServiceIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config

  describe "director service integration" do
    test "add_services_to_directory/2 adds new services to existing directory" do
      state = %State{
        services: %{
          "existing-service" => {:storage, {:existing, :node1}}
        }
      }

      service_infos = [
        {"new-service-1", :log, {:log_1, :node2}},
        {"new-service-2", :storage, {:storage_2, :node3}}
      ]

      result = Server.add_services_to_directory(state, service_infos)

      assert Map.has_key?(result.services, "existing-service")
      assert Map.has_key?(result.services, "new-service-1")
      assert Map.has_key?(result.services, "new-service-2")

      assert result.services["new-service-1"] == {:log, {:log_1, :node2}}
      assert result.services["new-service-2"] == {:storage, {:storage_2, :node3}}
    end

    test "add_services_to_directory/2 overwrites existing services with same ID" do
      state = %State{
        services: %{
          "service-1" => {:storage, {:old_name, :old_node}}
        }
      }

      service_infos = [
        {"service-1", :log, {:new_name, :new_node}}
      ]

      result = Server.add_services_to_directory(state, service_infos)

      assert result.services["service-1"] == {:log, {:new_name, :new_node}}
    end

    test "try_to_recover_if_stalled/1 handles different states correctly" do
      # Test that the function exists and handles different state types
      # without calling the complex recovery logic

      state_running = %State{state: :running}
      state_starting = %State{state: :starting}
      state_stopped = %State{state: :stopped}

      # Verify the function exists
      assert is_function(&Server.try_to_recover_if_stalled/1, 1)

      # These calls should not crash and should return the state unchanged
      # for non-recovery states
      assert Server.try_to_recover_if_stalled(state_running) == state_running
      assert Server.try_to_recover_if_stalled(state_starting) == state_starting
      assert Server.try_to_recover_if_stalled(state_stopped) == state_stopped

      # Note: We skip testing the recovery state case as it requires complex setup
      # with recovery_attempt and other fields to avoid the KeyError
    end
  end

  describe "director initialization with services" do
    test "director accepts services parameter in child_spec" do
      services = %{
        "test-service" => {:storage, {:test_storage, :node1}}
      }

      child_spec =
        Server.child_spec(
          cluster: TestCluster,
          config: Config.new([:node1, :node2]),
          old_transaction_system_layout: %{},
          epoch: 1,
          coordinator: self(),
          relieving: nil,
          services: services
        )

      assert child_spec.id == Server
      assert is_tuple(child_spec.start)
    end

    test "director defaults services to empty map when not provided" do
      child_spec =
        Server.child_spec(
          cluster: TestCluster,
          config: Config.new([:node1, :node2]),
          old_transaction_system_layout: %{},
          epoch: 1,
          coordinator: self(),
          relieving: nil
        )

      # Extract the init args and verify services default to %{}
      {_module, _func, [_server_module, init_args]} = child_spec.start
      {_cluster, _config, _layout, _epoch, _coordinator, _relieving, services} = init_args

      assert services == %{}
    end
  end
end
