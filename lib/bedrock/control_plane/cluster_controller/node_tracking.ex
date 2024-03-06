defmodule Bedrock.ControlPlane.ClusterController.NodeTracking do
  @moduledoc """
  The NodeTracking module is responsible for keeping track of the state of the
  nodes in the cluster. This includes the list of services that the node is
  advertising, the last time it responded to a ping and whether or not it is
  considered to be alive.
  """

  use Bedrock, :types

  @type t :: :ets.table()

  @spec new(nodes :: [node()]) :: t()
  def new(nodes) do
    t = :ets.new(:node_tracking, [:ordered_set])
    :ets.insert(t, nodes |> Enum.map(&{&1, :unknown, :unknown}))

    t
  end

  @spec add_node(t(), node(), integer(), services :: [atom()]) :: t()
  def add_node(t, node, last_pong_received_at, services) do
    :ets.insert(t, {node, last_pong_received_at, services})
    t
  end

  @spec alive?(t(), node(), now :: non_neg_integer(), liveness_timeout_in_ms :: timeout_in_ms()) ::
          boolean()
  def alive?(t, node, now, liveness_timeout_in_ms) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        false

      [{^node, :unknown, _services}] ->
        false

      [{^node, last_pong_received_at, _services}] ->
        liveness_timeout_in_ms > now - last_pong_received_at
    end
  end

  @spec advertised_services(t(), node()) :: [atom()] | :unknown
  def advertised_services(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] -> :unknown
      [{^node, _last_pong_received_at, advertised_services}] -> advertised_services
    end
  end

  @spec update_last_pong_received_at(t(), node(), last_pong_received_at :: non_neg_integer()) ::
          t()
  def update_last_pong_received_at(t, node, last_pong_received_at) do
    :ets.update_element(t, node, {2, last_pong_received_at})
    t
  end

  @spec update_advertised_services(t(), node(), advertised_services :: [atom()]) :: t()
  def update_advertised_services(t, node, advertised_services) do
    :ets.update_element(t, node, {3, advertised_services})
    t
  end
end
