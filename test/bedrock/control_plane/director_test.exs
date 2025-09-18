defmodule Bedrock.ControlPlane.DirectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director

  describe "API functions" do
    setup do
      # Mock director process for testing API calls
      director =
        spawn(fn ->
          loop = fn loop_fn ->
            receive do
              {:"$gen_call", from, :fetch_transaction_system_layout} ->
                GenServer.reply(from, {:ok, %{}})
                loop_fn.(loop_fn)

              {:"$gen_cast", {:pong, _node}} ->
                loop_fn.(loop_fn)

              {:"$gen_cast", {:ping, _pid, _version}} ->
                loop_fn.(loop_fn)

              {:"$gen_call", from, {:request_worker_creation, _node, _worker_id, _kind}} ->
                GenServer.reply(
                  from,
                  {:ok, %{id: "test", otp_name: :test, kind: :log, pid: self()}}
                )

                loop_fn.(loop_fn)

              {:"$gen_cast", {:service_registered, _service_infos}} ->
                loop_fn.(loop_fn)

              {:"$gen_cast", {:capabilities_updated, _node_capabilities}} ->
                loop_fn.(loop_fn)
            after
              1000 -> :timeout
            end
          end

          loop.(loop)
        end)

      {:ok, director: director}
    end

    test "fetch_transaction_system_layout returns transaction layout", %{director: director} do
      assert {:ok, %{}} = Director.fetch_transaction_system_layout(director)
      assert {:ok, %{}} = Director.fetch_transaction_system_layout(director, 1000)
    end

    test "send_pong/2 sends pong message", %{director: director} do
      result = Director.send_pong(director, :test_node)
      assert result == :ok
    end

    test "send_ping/2 sends ping message", %{director: director} do
      result = Director.send_ping(director, 100)
      assert result == :ok
    end

    test "request_worker_creation returns worker info", %{director: director} do
      assert {:ok, %{id: "test", otp_name: :test, kind: :log, pid: _}} =
               Director.request_worker_creation(director, :test_node, "worker_1", :log)

      assert {:ok, %{id: "test", otp_name: :test, kind: :log, pid: _}} =
               Director.request_worker_creation(director, :test_node, "worker_1", :storage, 5000)
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
    test "running_service_info structure contains expected fields" do
      info = %{
        id: "test_service",
        otp_name: :test_service,
        kind: :log,
        pid: self()
      }

      assert %{id: id, otp_name: name, kind: kind, pid: pid} = info
      assert is_binary(id) and is_atom(name) and kind in [:log, :storage] and is_pid(pid)
    end

    test "running_service_info_by_id structure supports lookup" do
      info = %{id: "test_service", otp_name: :test_service, kind: :storage, pid: self()}
      by_id = %{"test_service" => info}

      assert %{"test_service" => ^info} = by_id
    end
  end

  describe "error handling" do
    test "handles timeout errors gracefully" do
      unresponsive_director =
        spawn(fn ->
          receive do
            _ -> Process.sleep(10_000)
          end
        end)

      assert {:error, :timeout} =
               Director.fetch_transaction_system_layout(unresponsive_director, 100)
    end

    test "handles process termination" do
      terminated_director = spawn(fn -> :ok end)
      Process.sleep(10)

      assert {:error, _} = Director.fetch_transaction_system_layout(terminated_director, 100)
    end
  end

  describe "cast messages" do
    test "all cast operations complete successfully" do
      pid = self()

      assert :ok = Director.send_pong(pid, :test_node)
      assert :ok = Director.send_ping(pid, 100)
      assert :ok = Director.notify_services_registered(pid, [])
      assert :ok = Director.notify_capabilities_updated(pid, %{})
    end
  end
end
