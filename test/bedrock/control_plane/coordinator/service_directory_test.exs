defmodule Bedrock.ControlPlane.Coordinator.ServiceDirectoryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Coordinator.Durability

  describe "service directory processing" do
    test "process_command handles register_services" do
      initial_state = %State{
        service_directory: %{},
        director: :unavailable
      }

      services = [
        {"service_1", :storage, {:worker1, :node1@host}},
        {"service_2", :log, {:worker2, :node2@host}}
      ]

      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state, command)

      expected_directory = %{
        "service_1" => {:storage, {:worker1, :node1@host}},
        "service_2" => {:log, {:worker2, :node2@host}}
      }

      assert result_state.service_directory == expected_directory
    end

    test "process_command handles register_services with existing services" do
      initial_state = %State{
        service_directory: %{
          "existing_service" => {:storage, {:existing_worker, :existing_node@host}}
        },
        director: :unavailable
      }

      services = [{"new_service", :log, {:new_worker, :new_node@host}}]
      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state, command)

      expected_directory = %{
        "existing_service" => {:storage, {:existing_worker, :existing_node@host}},
        "new_service" => {:log, {:new_worker, :new_node@host}}
      }

      assert result_state.service_directory == expected_directory
    end

    test "process_command handles register_services overwrites existing service" do
      initial_state = %State{
        service_directory: %{"service_1" => {:storage, {:old_worker, :old_node@host}}},
        director: :unavailable
      }

      services = [{"service_1", :log, {:new_worker, :new_node@host}}]
      command = {:register_services, %{services: services}}
      result_state = Durability.process_command(initial_state, command)

      expected_directory = %{"service_1" => {:log, {:new_worker, :new_node@host}}}
      assert result_state.service_directory == expected_directory
    end

    test "process_command handles deregister_services" do
      initial_state = %State{
        service_directory: %{
          "service_1" => {:storage, {:worker1, :node1@host}},
          "service_2" => {:log, {:worker2, :node2@host}}
        },
        director: :unavailable
      }

      command = {:deregister_services, %{service_ids: ["service_1"]}}
      result_state = Durability.process_command(initial_state, command)

      expected_directory = %{"service_2" => {:log, {:worker2, :node2@host}}}
      assert result_state.service_directory == expected_directory
    end

    test "process_command handles deregister_services with non-existent service" do
      initial_state = %State{
        service_directory: %{"service_1" => {:storage, {:worker, :node@host}}},
        director: :unavailable
      }

      command = {:deregister_services, %{service_ids: ["non_existent"]}}
      result_state = Durability.process_command(initial_state, command)

      assert result_state.service_directory == %{"service_1" => {:storage, {:worker, :node@host}}}
    end

    test "process_command does not automatically notify director when services change" do
      director_pid =
        spawn(fn ->
          receive do
            {:"$gen_cast", {:service_registered, _directory}} ->
              flunk("Director should not receive automatic notifications")
          after
            50 ->
              :ok
          end
        end)

      initial_state = %State{
        service_directory: %{},
        director: director_pid
      }

      services = [{"service_1", :storage, {:worker, :node@host}}]
      command = {:register_services, %{services: services}}

      # Process the command (this should NOT send notification)
      result_state = Durability.process_command(initial_state, command)

      # Verify the service was added but no notification was sent
      assert result_state.service_directory == %{"service_1" => {:storage, {:worker, :node@host}}}

      # Give director process time to fail if notification was sent
      Process.sleep(60)
    end

    test "process_command does not crash when director is unavailable" do
      initial_state = %State{
        service_directory: %{},
        director: :unavailable
      }

      services = [{"service_1", :storage, {:worker, :node@host}}]
      command = {:register_services, %{services: services}}

      # This should not crash
      result_state = Durability.process_command(initial_state, command)
      assert result_state.service_directory == %{"service_1" => {:storage, {:worker, :node@host}}}
    end
  end
end
