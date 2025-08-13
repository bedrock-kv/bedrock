defmodule Bedrock.ControlPlane.CoordinatorTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.GenServerTestHelpers

  alias Bedrock.ControlPlane.Coordinator

  describe "API functions" do
    setup do
      # Mock coordinator process for testing API calls
      coordinator =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :fetch_config} ->
              GenServer.reply(
                from,
                {:ok, %{coordinators: [:node1], parameters: nil, policies: nil}}
              )

            {:"$gen_call", from, {:update_config, _config}} ->
              GenServer.reply(from, {:ok, :txn_123})

            {:"$gen_call", from, :fetch_transaction_system_layout} ->
              layout = %{
                id: "layout_1",
                epoch: 1,
                director: nil,
                sequencer: nil,
                rate_keeper: nil,
                proxies: [],
                resolvers: [],
                logs: %{},
                storage_teams: [],
                services: %{}
              }

              GenServer.reply(from, {:ok, layout})

            {:"$gen_call", from, {:update_transaction_system_layout, _layout}} ->
              GenServer.reply(from, {:ok, :txn_456})

            {:"$gen_call", from, {:register_services, _services}} ->
              GenServer.reply(from, {:ok, :txn_789})

            {:"$gen_call", from, {:deregister_services, _service_ids}} ->
              GenServer.reply(from, {:ok, :txn_abc})

            {:"$gen_call", from, {:register_gateway, _pid, _services, _capabilities}} ->
              GenServer.reply(from, {:ok, :txn_def})
          after
            1000 -> :timeout
          end
        end)

      {:ok, coordinator: coordinator}
    end

    test "config_key/0 returns coordinator atom" do
      assert Coordinator.config_key() == :coordinator
    end

    test "fetch_config/1 with default timeout", %{coordinator: coordinator} do
      result = Coordinator.fetch_config(coordinator)
      assert {:ok, config} = result
      assert config.coordinators == [:node1]
    end

    test "fetch_config/2 with custom timeout", %{coordinator: coordinator} do
      result = Coordinator.fetch_config(coordinator, 1000)
      assert {:ok, config} = result
      assert is_map(config)
    end

    test "update_config/2 sends properly formatted call message" do
      config = %{coordinators: [:node1], parameters: nil, policies: nil}
      test_pid = self()

      # Spawn a process that will make the call and we'll capture the message
      spawn(fn ->
        Coordinator.update_config(test_pid, config)
      end)

      # Use our helper macro to assert on the exact call message format
      assert_call_received({:update_config, actual_config}) do
        assert actual_config == config
        assert actual_config.coordinators == [:node1]
        assert actual_config.parameters == nil
        assert actual_config.policies == nil
      end
    end

    test "update_config/3 with custom timeout", %{coordinator: coordinator} do
      config = %{coordinators: [:node1], parameters: nil, policies: nil}
      result = Coordinator.update_config(coordinator, config, 2000)
      assert {:ok, :txn_123} = result
    end

    test "fetch_transaction_system_layout/1 with default timeout", %{coordinator: coordinator} do
      result = Coordinator.fetch_transaction_system_layout(coordinator)
      assert {:ok, layout} = result
      assert layout.id == "layout_1"
      assert layout.epoch == 1
    end

    test "fetch_transaction_system_layout/2 with custom timeout", %{coordinator: coordinator} do
      result = Coordinator.fetch_transaction_system_layout(coordinator, 1500)
      assert {:ok, layout} = result
      assert is_map(layout)
    end

    test "update_transaction_system_layout/2 with default timeout", %{coordinator: coordinator} do
      layout = %{
        id: "test_layout",
        epoch: 2,
        director: nil,
        sequencer: nil,
        rate_keeper: nil,
        proxies: [],
        resolvers: [],
        logs: %{},
        storage_teams: [],
        services: %{}
      }

      result = Coordinator.update_transaction_system_layout(coordinator, layout)
      assert {:ok, :txn_456} = result
    end

    test "update_transaction_system_layout/3 with custom timeout", %{coordinator: coordinator} do
      layout = %{
        id: "test_layout",
        epoch: 2,
        director: nil,
        sequencer: nil,
        rate_keeper: nil,
        proxies: [],
        resolvers: [],
        logs: %{},
        storage_teams: [],
        services: %{}
      }

      result = Coordinator.update_transaction_system_layout(coordinator, layout, 3000)
      assert {:ok, :txn_456} = result
    end

    test "register_services/2 with default timeout", %{coordinator: coordinator} do
      services = [
        {"service_1", :log, {:log_server, :node1}},
        {"service_2", :storage, {:storage_server, :node2}}
      ]

      result = Coordinator.register_services(coordinator, services)
      assert {:ok, :txn_789} = result
    end

    test "register_services/3 with custom timeout", %{coordinator: coordinator} do
      services = [{"service_1", :log, {:log_server, :node1}}]

      result = Coordinator.register_services(coordinator, services, 2500)
      assert {:ok, :txn_789} = result
    end

    test "deregister_services/2 with default timeout", %{coordinator: coordinator} do
      service_ids = ["service_1", "service_2"]

      result = Coordinator.deregister_services(coordinator, service_ids)
      assert {:ok, :txn_abc} = result
    end

    test "deregister_services/3 with custom timeout", %{coordinator: coordinator} do
      service_ids = ["service_1"]

      result = Coordinator.deregister_services(coordinator, service_ids, 1800)
      assert {:ok, :txn_abc} = result
    end

    test "register_gateway/4 with default timeout", %{coordinator: coordinator} do
      gateway_pid = self()
      compact_services = [{:log, :log_server}, {:storage, :storage_server}]
      capabilities = [:can_host_logs, :can_host_storage]

      result =
        Coordinator.register_gateway(coordinator, gateway_pid, compact_services, capabilities)

      assert {:ok, :txn_def} = result
    end

    test "register_gateway/5 with custom timeout", %{coordinator: coordinator} do
      gateway_pid = self()
      compact_services = [{:log, :log_server}]
      capabilities = [:can_host_logs]

      result =
        Coordinator.register_gateway(
          coordinator,
          gateway_pid,
          compact_services,
          capabilities,
          4000
        )

      assert {:ok, :txn_def} = result
    end
  end

  describe "type definitions" do
    test "service_info structure" do
      # Test that the type structure is correctly defined
      service_info = {"service_id", :log, {:service_name, :node1}}

      {service_id, kind, {name, node}} = service_info
      assert is_binary(service_id)
      assert kind in [:log, :storage]
      assert is_atom(name)
      assert is_atom(node)
    end

    test "compact_service_info structure" do
      # Test the compact service info structure
      compact_info = {:storage, :storage_server}

      {kind, name} = compact_info
      assert kind in [:log, :storage]
      assert is_atom(name)
    end
  end

  describe "error handling" do
    test "handles timeout errors gracefully" do
      # Create a coordinator that doesn't respond
      unresponsive_coordinator =
        spawn(fn ->
          receive do
            # Never respond
            _ -> Process.sleep(10_000)
          end
        end)

      result = Coordinator.fetch_config(unresponsive_coordinator, 100)
      assert {:error, :timeout} = result
    end

    test "handles process termination" do
      # Create a coordinator that terminates immediately
      terminated_coordinator = spawn(fn -> :ok end)
      # Ensure it's dead
      Process.sleep(10)

      result = Coordinator.fetch_config(terminated_coordinator, 100)
      assert {:error, _} = result
    end
  end

  describe "API call patterns" do
    setup do
      # Create a coordinator that responds to various error conditions
      coordinator =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :fetch_config} ->
              GenServer.reply(from, {:error, :unavailable})

            {:"$gen_call", from, {:update_config, _}} ->
              GenServer.reply(from, {:error, :not_leader})

            {:"$gen_call", from, {:register_services, _}} ->
              GenServer.reply(from, {:error, :failed})
          after
            1000 -> :timeout
          end
        end)

      {:ok, coordinator: coordinator}
    end

    test "handles unavailable error", %{coordinator: coordinator} do
      result = Coordinator.fetch_config(coordinator)
      assert {:error, :unavailable} = result
    end

    test "handles not_leader error", %{coordinator: coordinator} do
      config = %{coordinators: [:node1], parameters: nil, policies: nil}
      result = Coordinator.update_config(coordinator, config)
      assert {:error, :not_leader} = result
    end

    test "handles failed error", %{coordinator: coordinator} do
      services = [{"service_1", :log, {:log_server, :node1}}]
      result = Coordinator.register_services(coordinator, services)
      assert {:error, :failed} = result
    end
  end

  describe "service registration scenarios" do
    setup do
      coordinator =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:register_services, services}} ->
              # Echo back the services count for verification
              GenServer.reply(from, {:ok, {:registered, length(services)}})

            {:"$gen_call", from, {:deregister_services, service_ids}} ->
              GenServer.reply(from, {:ok, {:deregistered, length(service_ids)}})

            {:"$gen_call", from, {:register_gateway, _pid, services, capabilities}} ->
              GenServer.reply(
                from,
                {:ok, {:gateway_registered, length(services), length(capabilities)}}
              )
          after
            1000 -> :timeout
          end
        end)

      {:ok, coordinator: coordinator}
    end

    test "registers multiple services", %{coordinator: coordinator} do
      services = [
        {"log_1", :log, {:log_server_1, :node1}},
        {"log_2", :log, {:log_server_2, :node1}},
        {"storage_1", :storage, {:storage_server_1, :node2}}
      ]

      result = Coordinator.register_services(coordinator, services)
      assert {:ok, {:registered, 3}} = result
    end

    test "deregisters multiple services", %{coordinator: coordinator} do
      service_ids = ["log_1", "log_2", "storage_1", "storage_2"]

      result = Coordinator.deregister_services(coordinator, service_ids)
      assert {:ok, {:deregistered, 4}} = result
    end

    test "registers gateway with services and capabilities", %{coordinator: coordinator} do
      gateway_pid = self()
      compact_services = [{:log, :log_1}, {:log, :log_2}, {:storage, :storage_1}]
      capabilities = [:can_host_logs, :can_host_storage, :high_memory]

      result =
        Coordinator.register_gateway(coordinator, gateway_pid, compact_services, capabilities)

      assert {:ok, {:gateway_registered, 3, 3}} = result
    end
  end
end
