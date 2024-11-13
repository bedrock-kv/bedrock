defmodule Bedrock.ControlPlane.Director.NodeTrackingTest do
  use ExUnit.Case, async: true
  alias Bedrock.ControlPlane.Director.NodeTracking

  defp nodes, do: [:node1, :node2, :node3]

  setup do
    {:ok, table: NodeTracking.new(nodes())}
  end

  test "new/1 initializes the table with nodes", %{table: table} do
    assert :ets.lookup(table, :node1) != []
    assert :ets.lookup(table, :node2) != []
    assert :ets.lookup(table, :node3) != []
  end

  test "add_node/3 adds a new node", %{table: table} do
    NodeTracking.add_node(table, :node4, true)
    assert :ets.lookup(table, :node4) != []
  end

  test "dead_nodes/3 returns nodes that are considered dead", %{table: table} do
    now = :os.system_time(:millisecond)
    liveness_timeout_in_ms = 1000
    assert NodeTracking.dead_nodes(table, now, liveness_timeout_in_ms) == nodes()
  end

  test "exists?/2 checks if a node exists", %{table: table} do
    assert NodeTracking.exists?(table, :node1)
    refute NodeTracking.exists?(table, :node4)
  end

  test "alive?/2 checks if a node is alive", %{table: table} do
    refute NodeTracking.alive?(table, :node1)
    NodeTracking.update_last_seen_at(table, :node1, :os.system_time(:millisecond))
    assert NodeTracking.alive?(table, :node1)
  end

  test "authorized?/2 checks if a node is authorized", %{table: table} do
    assert NodeTracking.authorized?(table, :node1)
    NodeTracking.add_node(table, :node4, false)
    refute NodeTracking.authorized?(table, :node4)
  end

  test "capabilities/2 returns the advertised services of a node", %{table: table} do
    assert NodeTracking.capabilities(table, :node1) == :unknown
    NodeTracking.update_capabilities(table, :node1, [:service1, :service2])
    assert NodeTracking.capabilities(table, :node1) == [:service1, :service2]
  end

  test "update_last_seen_at/3 updates the last seen time of a node", %{table: table} do
    last_seen_at = :os.system_time(:millisecond)
    NodeTracking.update_last_seen_at(table, :node1, last_seen_at)
    [{_, updated_last_seen_at, _, _, _, _}] = :ets.lookup(table, :node1)
    assert updated_last_seen_at == last_seen_at
  end

  test "update_capabilities/3 updates the advertised services of a node", %{table: table} do
    NodeTracking.update_capabilities(table, :node1, [:service1, :service2])
    [{_, _, updated_services, _, _, _}] = :ets.lookup(table, :node1)
    assert updated_services == [:service1, :service2]
  end

  test "down/2 marks a node as down", %{table: table} do
    NodeTracking.down(table, :node1)
    [{_, _, _, status, _, _}] = :ets.lookup(table, :node1)
    assert status == :down
  end
end
