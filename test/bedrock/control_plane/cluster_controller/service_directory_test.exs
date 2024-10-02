defmodule Bedrock.ControlPlane.ClusterController.ServiceDirectoryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.ClusterController.ServiceDirectory
  alias Bedrock.ControlPlane.ClusterController.ServiceInfo

  describe "new/0" do
    test "creates a new ETS table" do
      table = ServiceDirectory.new()
      assert :ets.info(table)[:type] == :set
    end
  end

  describe "update_service_info/2" do
    setup do
      table = ServiceDirectory.new()
      {:ok, table: table}
    end

    test "returns :unchanged when status is the same", %{table: table} do
      service_info = %ServiceInfo{id: 1, kind: :web, status: :up}
      :ets.insert(table, {1, :web, :up, true})
      assert ServiceDirectory.update_service_info(table, service_info) == :unchanged

      :ets.insert(table, {1, :web, :up, false})
      assert ServiceDirectory.update_service_info(table, service_info) == :unchanged
    end

    test "returns :changed when status is different", %{table: table} do
      service_info = %ServiceInfo{id: 1, kind: :web, status: :down}
      :ets.insert(table, {1, :web, :up, true})
      assert ServiceDirectory.update_service_info(table, service_info) == :changed
      assert [{1, :web, :down, false}] = :ets.lookup(table, 1)
    end

    test "inserts new service info when not present", %{table: table} do
      service_info = %ServiceInfo{id: 2, kind: :db, status: :up}
      assert ServiceDirectory.update_service_info(table, service_info) == :changed
      assert [{2, :db, :up, false}] = :ets.lookup(table, 2)
    end
  end

  describe "node_down/2" do
    setup do
      table = ServiceDirectory.new()
      {:ok, table: table}
    end

    test "returns empty list when no services run on the node", %{table: table} do
      assert ServiceDirectory.node_down(table, :node1) == []
    end

    test "updates and returns affected services when node is down", %{table: table} do
      :ets.insert(table, {1, :web, {:up, :_, :_, :node1}, true})
      :ets.insert(table, {2, :db, {:up, :_, :_, :node1}, true})
      :ets.insert(table, {3, :db, {:up, :_, :_, :node2}, true})
      :ets.insert(table, {4, :db, {:up, :_, :_, :node3}, true})

      expected_services = [
        %ServiceInfo{id: 1, kind: :web, status: :down},
        %ServiceInfo{id: 2, kind: :db, status: :down}
      ]

      assert expected_services ==
               ServiceDirectory.node_down(table, :node1)
               |> Enum.sort(&(&1.id < &2.id))

      assert [{1, :web, :down, true}] = :ets.lookup(table, 1)
      assert [{2, :db, :down, true}] = :ets.lookup(table, 2)
      assert [{3, :db, {:up, :_, :_, :node2}, true}] = :ets.lookup(table, 3)
      assert [{4, :db, {:up, :_, :_, :node3}, true}] = :ets.lookup(table, 4)
    end
  end
end
