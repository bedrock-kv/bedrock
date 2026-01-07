defmodule Bedrock.ControlPlane.Coordinator.ServiceDirectoryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.State

  # Helper function to create initial state
  defp initial_state(service_directory \\ %{}) do
    %State{service_directory: service_directory, director: :unavailable}
  end

  describe "register_services command" do
    test "registers multiple services to empty directory" do
      services = [
        {"service_1", :storage, {:worker1, :node1@host}},
        {"service_2", :log, {:worker2, :node2@host}}
      ]

      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state(), command)

      assert %State{
               service_directory: %{
                 "service_1" => {:storage, {:worker1, :node1@host}},
                 "service_2" => {:log, {:worker2, :node2@host}}
               }
             } = result_state
    end

    test "adds new service to existing directory" do
      existing_directory = %{
        "existing_service" => {:storage, {:existing_worker, :existing_node@host}}
      }

      services = [{"new_service", :log, {:new_worker, :new_node@host}}]
      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state(existing_directory), command)

      assert %State{
               service_directory: %{
                 "existing_service" => {:storage, {:existing_worker, :existing_node@host}},
                 "new_service" => {:log, {:new_worker, :new_node@host}}
               }
             } = result_state
    end

    test "overwrites existing service with same id" do
      existing_directory = %{"service_1" => {:storage, {:old_worker, :old_node@host}}}

      services = [{"service_1", :log, {:new_worker, :new_node@host}}]
      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state(existing_directory), command)

      assert %State{
               service_directory: %{"service_1" => {:log, {:new_worker, :new_node@host}}}
             } = result_state
    end
  end

  describe "deregister_services command" do
    test "removes specified service from directory" do
      existing_directory = %{
        "service_1" => {:storage, {:worker1, :node1@host}},
        "service_2" => {:log, {:worker2, :node2@host}}
      }

      command = {:deregister_services, %{service_ids: ["service_1"]}}
      result_state = Durability.process_command(initial_state(existing_directory), command)

      assert %State{
               service_directory: %{"service_2" => {:log, {:worker2, :node2@host}}}
             } = result_state
    end

    test "ignores non-existent service ids" do
      existing_directory = %{"service_1" => {:storage, {:worker, :node@host}}}

      command = {:deregister_services, %{service_ids: ["non_existent"]}}
      result_state = Durability.process_command(initial_state(existing_directory), command)

      assert %State{
               service_directory: %{"service_1" => {:storage, {:worker, :node@host}}}
             } = result_state
    end
  end

  describe "director notifications" do
    test "does not automatically notify director when services change" do
      # Use test process as director to receive any notifications
      test_state = %State{service_directory: %{}, director: self()}
      services = [{"service_1", :storage, {:worker, :node@host}}]
      command = {:register_services, %{services: services}}

      # Process the command (this should NOT send notification)
      result_state = Durability.process_command(test_state, command)

      # Verify the service was added but no notification was sent
      assert %State{
               service_directory: %{"service_1" => {:storage, {:worker, :node@host}}}
             } = result_state

      # Assert no notification is received
      refute_receive {:"$gen_cast", {:service_registered, _}}, 50
    end

    test "does not crash when director is unavailable" do
      services = [{"service_1", :storage, {:worker, :node@host}}]
      command = {:register_services, %{services: services}}

      # This should not crash
      result_state = Durability.process_command(initial_state(), command)

      assert %State{
               service_directory: %{"service_1" => {:storage, {:worker, :node@host}}}
             } = result_state
    end
  end
end
