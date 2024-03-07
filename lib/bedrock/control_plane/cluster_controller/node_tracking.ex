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
    :ets.insert(t, nodes |> Enum.map(&{&1, :unknown, :unknown, :down, true}))
    t
  end

  @spec add_node(t(), node(), authorized :: boolean()) :: t()
  def add_node(t, node, authorized) do
    :ets.insert(t, {node, :unknown, :unknown, :up, authorized})
    t
  end

  @spec dead_nodes(t(), now :: integer(), liveness_timeout_in_ms :: timeout_in_ms()) ::
          [node()]
  def dead_nodes(t, now, liveness_timeout_in_ms) do
    :ets.select(t, [
      {{:"$1", :"$2", :_, :up, :_}, [{:<, :"$2", {:const, now - liveness_timeout_in_ms}}],
       [:"$1"]}
    ])
  end

  @spec exists?(t(), node()) :: boolean()
  def exists?(t, node), do: [] != :ets.lookup(t, node)

  @spec alive?(t(), node()) :: boolean()
  def alive?(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        false

      [{^node, _last_seen_at, _services, up_down, _authorized}] ->
        :up == up_down
    end
  end

  @spec authorized?(t(), node()) :: boolean()
  def authorized?(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] -> false
      [{^node, _last_seen_at, _services, _up_down, authorized}] -> authorized
    end
  end

  @spec advertised_services(t(), node()) :: [atom()] | :unknown
  def advertised_services(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        :unknown

      [{^node, _last_seen_at, advertised_services, _up_down, _authorized}] ->
        advertised_services
    end
  end

  @spec update_last_seen_at(t(), node(), last_seen_at :: integer()) ::
          t()
  def update_last_seen_at(t, node, last_seen_at) do
    :ets.update_element(t, node, [{2, last_seen_at}, {4, :up}])
    t
  end

  @spec update_advertised_services(t(), node(), advertised_services :: [atom()]) :: t()
  def update_advertised_services(t, node, advertised_services) do
    :ets.update_element(t, node, {3, advertised_services})
    t
  end

  @spec down(t(), node()) :: t()
  def down(t, node) do
    :ets.update_element(t, node, {4, :down})
    t
  end
end
