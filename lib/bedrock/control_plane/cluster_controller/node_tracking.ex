defmodule Bedrock.ControlPlane.ClusterController.NodeTracking do
  @moduledoc """
  The NodeTracking module is responsible for keeping track of the state of the
  nodes in the cluster. This includes the list of services that the node is
  advertising, the last time it responded to a ping and whether or not it is
  considered to be alive.
  """

  use Bedrock, :types

  @type t :: :ets.table()

  @typep last_seen_at :: integer() | :unknown
  @typep advertised_services :: [atom()] | :unknown
  @typep up_down :: :up | :down
  @typep authorized :: boolean()

  @spec row(node(), last_seen_at(), advertised_services(), up_down(), authorized()) ::
          {node(), last_seen_at(), advertised_services(), up_down(), authorized()}
  defp row(node, last_seen_at, advertised_services, up_down, authorized),
    do: {node, last_seen_at, advertised_services, up_down, authorized}

  @doc """
  Create a new node tracking table with the given nodes.
  """
  @spec new(nodes :: [node()]) :: t()
  def new(nodes) do
    t = :ets.new(:node_tracking, [:ordered_set])
    :ets.insert(t, nodes |> Enum.map(&row(&1, :unknown, :unknown, :down, true)))
    t
  end

  @doc """
  Add a new node to the node tracking table.
  """
  @spec add_node(t(), node(), authorized :: boolean()) :: t()
  def add_node(t, node, authorized) do
    :ets.insert(t, row(node, :unknown, :unknown, :up, authorized))
    t
  end

  @doc """
  Get the list of nodes that have not responded to a ping in the last
  `liveness_timeout_in_ms` milliseconds or have never responded to a ping.
  """
  @spec dying_nodes(t(), now :: integer(), liveness_timeout_in_ms :: timeout_in_ms()) ::
          [node()]
  def dying_nodes(t, now, liveness_timeout_in_ms) do
    :ets.select(t, [
      {{:"$1", :"$2", :_, :up, :_},
       [
         {:orelse, {:==, :"$2", :unknown}, {:<, :"$2", {:const, now - liveness_timeout_in_ms}}}
       ], [:"$1"]}
    ])
  end

  @doc """
  Get the list of nodes that have not responded to a ping in the last
  `liveness_timeout_in_ms` milliseconds, have never responded to a ping or are
  explicitly marked as :down.
  """
  @spec dead_nodes(t(), now :: integer(), liveness_timeout_in_ms :: timeout_in_ms()) ::
          [node()]
  def dead_nodes(t, now, liveness_timeout_in_ms) do
    :ets.select(t, [
      {{:"$1", :"$2", :_, :"$4", :_},
       [
         {:orelse, {:==, :"$4", :down},
          {:orelse, {:==, :"$2", :unknown}, {:<, :"$2", {:const, now - liveness_timeout_in_ms}}}}
       ], [:"$1"]}
    ])
  end

  @doc """
  Check if a node exists in the node tracking table.
  """
  @spec exists?(t(), node()) :: boolean()
  def exists?(t, node), do: [] != :ets.lookup(t, node)

  @doc """
  Check if a node is currently considered to be alive.
  """
  @spec alive?(t(), node()) :: boolean()
  def alive?(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        false

      [_row = {^node, _last_seen_at, _services, up_down, _authorized}] ->
        :up == up_down
    end
  end

  @doc """
  Check if a node is currently considered to be authorized.
  """
  @spec authorized?(t(), node()) :: boolean()
  def authorized?(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] -> false
      [_row = {^node, _last_seen_at, _services, _up_down, authorized}] -> authorized
    end
  end

  @doc """
  Get a list of the services that a node is advertising.
  """
  @spec advertised_services(t(), node()) :: [atom()] | :unknown
  def advertised_services(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        :unknown

      [_row = {^node, _last_seen_at, advertised_services, _up_down, _authorized}] ->
        advertised_services
    end
  end

  @doc """
  Get the last time that a node responded to a ping.
  """
  @spec update_last_seen_at(t(), node(), last_seen_at :: integer()) ::
          t()
  def update_last_seen_at(t, node, last_seen_at) do
    :ets.update_element(t, node, [{2, last_seen_at}, {4, :up}])
    t
  end

  @doc """
  Update the list of services that a node is advertising.
  """
  @spec update_advertised_services(t(), node(), advertised_services :: [atom()]) :: t()
  def update_advertised_services(t, node, advertised_services) do
    :ets.update_element(t, node, {3, advertised_services})
    t
  end

  @doc """
  Mark a node as being down.
  """
  @spec down(t(), node()) :: t()
  def down(t, node) do
    :ets.update_element(t, node, {4, :down})
    t
  end
end
