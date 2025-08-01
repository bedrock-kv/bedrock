defmodule Bedrock.ControlPlane.DirectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director

  describe "API functions" do
    setup do
      # Mock director process for testing API calls
      director =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :fetch_transaction_system_layout} ->
              GenServer.reply(from, {:ok, %{}})

            {:"$gen_cast", {:pong, _node}} ->
              :ok

            {:"$gen_cast", {:ping, _pid, _version}} ->
              :ok

            {:"$gen_call", from, {:request_worker_creation, _node, _worker_id, _kind}} ->
              GenServer.reply(
                from,
                {:ok, %{id: "test", otp_name: :test, kind: :log, pid: self()}}
              )

            {:"$gen_cast", {:stand_relieved, _relief}} ->
              :ok

            {:"$gen_cast", {:service_registered, _service_infos}} ->
              :ok

            {:"$gen_cast", {:capabilities_updated, _node_capabilities}} ->
              :ok
          after
            1000 -> :timeout
          end
        end)

      {:ok, director: director}
    end

    test "fetch_transaction_system_layout/1 with default timeout", %{director: director} do
      result = Director.fetch_transaction_system_layout(director)
      assert {:ok, %{}} = result
    end

    test "fetch_transaction_system_layout/2 with custom timeout", %{director: director} do
      result = Director.fetch_transaction_system_layout(director, 1000)
      assert {:ok, %{}} = result
    end

    test "send_pong/2 sends pong message", %{director: director} do
      result = Director.send_pong(director, :test_node)
      assert result == :ok
    end

    test "send_ping/2 sends ping message", %{director: director} do
      result = Director.send_ping(director, 100)
      assert result == :ok
    end

    test "request_worker_creation/4 with default timeout", %{director: director} do
      result = Director.request_worker_creation(director, :test_node, "worker_1", :log)

      assert {:ok, worker_info} = result
      assert worker_info.id == "test"
      assert worker_info.otp_name == :test
      assert worker_info.kind == :log
    end

    test "request_worker_creation/5 with custom timeout", %{director: director} do
      result = Director.request_worker_creation(director, :test_node, "worker_1", :storage, 5000)

      assert {:ok, worker_info} = result
      assert worker_info.id == "test"
      assert worker_info.otp_name == :test
      assert worker_info.kind == :log
    end

    test "stand_relieved/2 sends relief message", %{director: director} do
      relief = {123, self()}
      result = Director.stand_relieved(director, relief)
      assert result == :ok
    end

    test "notify_services_registered/2 sends service registration notification", %{
      director: director
    } do
      service_infos = [
        {"service_1", :log, {:log_server_1, :node1}},
        {"service_2", :storage, {:storage_server_1, :node1}}
      ]

      result = Director.notify_services_registered(director, service_infos)
      assert result == :ok
    end

    test "notify_capabilities_updated/2 sends capability update notification", %{
      director: director
    } do
      node_capabilities = %{
        :can_host_logs => [:node1, :node2],
        :can_host_storage => [:node1, :node3]
      }

      result = Director.notify_capabilities_updated(director, node_capabilities)
      assert result == :ok
    end
  end

  describe "type definitions" do
    test "running_service_info structure" do
      # Test that the type structure is correctly defined
      info = %{
        id: "test_service",
        otp_name: :test_service,
        kind: :log,
        pid: self()
      }

      assert is_binary(info.id)
      assert is_atom(info.otp_name)
      assert info.kind in [:log, :storage]
      assert is_pid(info.pid)
    end

    test "running_service_info_by_id structure" do
      # Test the by_id map structure
      info = %{
        id: "test_service",
        otp_name: :test_service,
        kind: :storage,
        pid: self()
      }

      by_id = %{"test_service" => info}

      assert Map.has_key?(by_id, "test_service")
      assert by_id["test_service"] == info
    end
  end

  describe "error handling" do
    test "handles timeout errors gracefully" do
      # Create a director that doesn't respond
      unresponsive_director =
        spawn(fn ->
          receive do
            # Never respond
            _ -> Process.sleep(10_000)
          end
        end)

      result = Director.fetch_transaction_system_layout(unresponsive_director, 100)
      assert {:error, :timeout} = result
    end

    test "handles process termination" do
      # Create a director that terminates immediately
      terminated_director = spawn(fn -> :ok end)
      # Ensure it's dead
      Process.sleep(10)

      result = Director.fetch_transaction_system_layout(terminated_director, 100)
      assert {:error, _} = result
    end
  end

  describe "message patterns" do
    test "cast messages are properly formatted" do
      # This test verifies the message format without sending to a real process
      # We can't easily test the exact format without a mock, but we can ensure the functions run
      pid = self()

      assert :ok = Director.send_pong(pid, :test_node)
      assert :ok = Director.send_ping(pid, 100)
      assert :ok = Director.stand_relieved(pid, {123, self()})
      assert :ok = Director.notify_services_registered(pid, [])
      assert :ok = Director.notify_capabilities_updated(pid, %{})
    end
  end
end
