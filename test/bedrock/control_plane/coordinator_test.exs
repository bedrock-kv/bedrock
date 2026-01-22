defmodule Bedrock.ControlPlane.CoordinatorTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

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
                services: %{}
              }

              GenServer.reply(from, {:ok, layout})

            {:"$gen_call", from, {:update_transaction_system_layout, _layout}} ->
              GenServer.reply(from, {:ok, :txn_456})

            {:"$gen_call", from, {:register_services, _services}} ->
              GenServer.reply(from, {:ok, :txn_789})

            {:"$gen_call", from, {:deregister_services, _service_ids}} ->
              GenServer.reply(from, {:ok, :txn_abc})

            {:"$gen_call", from, {:register_node_resources, _pid, _services, _capabilities}} ->
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
      assert {:ok, %{coordinators: [:node1]}} = Coordinator.fetch_config(coordinator)
    end

    test "fetch_config/2 with custom timeout", %{coordinator: coordinator} do
      assert {:ok, config} = Coordinator.fetch_config(coordinator, 1000)
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
      assert_call_received({:update_config, %{coordinators: [:node1], parameters: nil, policies: nil}})
    end

    test "update_config/3 with custom timeout", %{coordinator: coordinator} do
      config = %{coordinators: [:node1], parameters: nil, policies: nil}
      assert {:ok, :txn_123} = Coordinator.update_config(coordinator, config, 2000)
    end

    test "fetch_transaction_system_layout/1 with default timeout", %{coordinator: coordinator} do
      assert {:ok, %{id: "layout_1", epoch: 1}} =
               Coordinator.fetch_transaction_system_layout(coordinator)
    end

    test "fetch_transaction_system_layout/2 with custom timeout", %{coordinator: coordinator} do
      assert {:ok, layout} = Coordinator.fetch_transaction_system_layout(coordinator, 1500)
      assert is_map(layout)
    end

    test "update_transaction_system_layout/2 with default timeout", %{coordinator: coordinator} do
      layout = create_test_layout()
      assert {:ok, :txn_456} = Coordinator.update_transaction_system_layout(coordinator, layout)
    end

    test "update_transaction_system_layout/3 with custom timeout", %{coordinator: coordinator} do
      layout = create_test_layout()
      assert {:ok, :txn_456} = Coordinator.update_transaction_system_layout(coordinator, layout, 3000)
    end

    test "register_services/2 with default timeout", %{coordinator: coordinator} do
      services = create_test_services()
      assert {:ok, :txn_789} = Coordinator.register_services(coordinator, services)
    end

    test "register_services/3 with custom timeout", %{coordinator: coordinator} do
      services = [{"service_1", :log, {:log_server, :node1}}]
      assert {:ok, :txn_789} = Coordinator.register_services(coordinator, services, 2500)
    end

    test "deregister_services/2 with default timeout", %{coordinator: coordinator} do
      service_ids = ["service_1", "service_2"]
      assert {:ok, :txn_abc} = Coordinator.deregister_services(coordinator, service_ids)
    end

    test "deregister_services/3 with custom timeout", %{coordinator: coordinator} do
      service_ids = ["service_1"]
      assert {:ok, :txn_abc} = Coordinator.deregister_services(coordinator, service_ids, 1800)
    end

    test "register_node_resources/4 with default timeout", %{coordinator: coordinator} do
      client_pid = self()
      compact_services = [{:log, :log_server}, {:materializer, :storage_server}]
      capabilities = [:can_host_logs, :can_host_storage]

      assert {:ok, :txn_def} =
               Coordinator.register_node_resources(coordinator, client_pid, compact_services, capabilities)
    end

    test "register_node_resources/5 with custom timeout", %{coordinator: coordinator} do
      client_pid = self()
      compact_services = [{:log, :log_server}]
      capabilities = [:can_host_logs]

      assert {:ok, :txn_def} =
               Coordinator.register_node_resources(
                 coordinator,
                 client_pid,
                 compact_services,
                 capabilities,
                 4000
               )
    end
  end

  describe "type definitions" do
    test "service_info structure" do
      service_info = {"service_id", :log, {:service_name, :node1}}
      assert {service_id, kind, {name, node}} = service_info
      assert is_binary(service_id) and kind in [:log, :materializer] and is_atom(name) and is_atom(node)
    end

    test "compact_service_info structure" do
      compact_info = {:materializer, :storage_server}
      assert {kind, name} = compact_info
      assert kind in [:log, :materializer] and is_atom(name)
    end
  end

  describe "error handling" do
    test "handles timeout errors gracefully" do
      unresponsive_coordinator = spawn_unresponsive_coordinator()
      assert {:error, :timeout} = Coordinator.fetch_config(unresponsive_coordinator, 100)
    end

    test "handles process termination" do
      terminated_coordinator = spawn(fn -> :ok end)
      ref = Process.monitor(terminated_coordinator)
      assert_receive {:DOWN, ^ref, :process, _, _}

      assert {:error, _} = Coordinator.fetch_config(terminated_coordinator, 100)
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
      assert {:error, :unavailable} = Coordinator.fetch_config(coordinator)
    end

    test "handles not_leader error", %{coordinator: coordinator} do
      config = %{coordinators: [:node1], parameters: nil, policies: nil}
      assert {:error, :not_leader} = Coordinator.update_config(coordinator, config)
    end

    test "handles failed error", %{coordinator: coordinator} do
      services = [{"service_1", :log, {:log_server, :node1}}]
      assert {:error, :failed} = Coordinator.register_services(coordinator, services)
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

            {:"$gen_call", from, {:register_node_resources, _pid, services, capabilities}} ->
              GenServer.reply(
                from,
                {:ok, {:node_resources_registered, length(services), length(capabilities)}}
              )
          after
            1000 -> :timeout
          end
        end)

      {:ok, coordinator: coordinator}
    end

    test "registers multiple services", %{coordinator: coordinator} do
      services = create_multiple_test_services()
      assert {:ok, {:registered, 3}} = Coordinator.register_services(coordinator, services)
    end

    test "deregisters multiple services", %{coordinator: coordinator} do
      service_ids = ["log_1", "log_2", "storage_1", "storage_2"]
      assert {:ok, {:deregistered, 4}} = Coordinator.deregister_services(coordinator, service_ids)
    end

    test "registers gateway with services and capabilities", %{coordinator: coordinator} do
      gateway_pid = self()
      compact_services = [{:log, :log_1}, {:log, :log_2}, {:materializer, :storage_1}]
      capabilities = [:can_host_logs, :can_host_storage, :high_memory]

      assert {:ok, {:node_resources_registered, 3, 3}} =
               Coordinator.register_node_resources(coordinator, gateway_pid, compact_services, capabilities)
    end
  end

  # Helper functions

  defp create_test_layout do
    %{
      id: "test_layout",
      epoch: 2,
      director: nil,
      sequencer: nil,
      rate_keeper: nil,
      proxies: [],
      resolvers: [],
      logs: %{},
      services: %{}
    }
  end

  defp create_test_services do
    [
      {"service_1", :log, {:log_server, :node1}},
      {"service_2", :materializer, {:storage_server, :node2}}
    ]
  end

  defp create_multiple_test_services do
    [
      {"log_1", :log, {:log_server_1, :node1}},
      {"log_2", :log, {:log_server_2, :node1}},
      {"storage_1", :materializer, {:storage_server_1, :node2}}
    ]
  end

  defp spawn_unresponsive_coordinator do
    spawn(fn ->
      receive do
        # Never respond
        _ -> Process.sleep(10_000)
      end
    end)
  end
end
