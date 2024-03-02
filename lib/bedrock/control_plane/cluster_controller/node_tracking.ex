defmodule Bedrock.ControlPlane.ClusterController.NodeTracking do
  @moduledoc """
  The NodeTracking module is responsible for keeping track of the state of the
  nodes in the cluster. This includes the list of services that the node is
  advertising, the last time it responded to a ping and whether or not it is
  considered to be alive.
  """

  use Bedrock, :types

  @type t :: %__MODULE__{
          table: :ets.table(),
          liveness_timeout_in_ms: Bedrock.timeout_in_ms()
        }
  defstruct [
    # The table that is used to store the state of the nodes in the cluster.
    table: nil,
    # The timeout that is used to determine if a node is considered to be alive.
    liveness_timeout_in_ms: nil
  ]

  @spec new(nodes :: [node()], liveness_timeout_in_ms :: Bedrock.timeout_in_ms()) :: t()
  def new(nodes, liveness_timeout_in_ms \\ 300) do
    table = :ets.new(:node_tracking, [:ordered_set])
    :ets.insert(table, nodes |> Enum.map(&{&1, :unknown, :unknown}))

    %__MODULE__{
      table: table,
      liveness_timeout_in_ms: liveness_timeout_in_ms
    }
  end

  def add_node(t, node, last_pong_received_at, services) do
    :ets.insert(t.table, {node, last_pong_received_at, services})
    t
  end

  @spec alive?(t(), node(), now :: non_neg_integer()) :: boolean()
  def alive?(t, node, now) do
    :ets.lookup(t.table, node)
    |> case do
      [] ->
        false

      [{^node, :unknown, _services}] ->
        false

      [{^node, last_pong_received_at, _services}] ->
        t.liveness_timeout_in_ms > now - last_pong_received_at
    end
  end

  @spec services(t(), node()) :: [atom()] | :unknown
  def services(t, node) do
    :ets.lookup(t.table, node)
    |> case do
      [] -> :unknown
      [{^node, _last_pong_received_at, services}] -> services
    end
  end

  @spec update_last_pong_received_at(t(), node(), last_pong_received_at :: non_neg_integer()) ::
          t()
  def update_last_pong_received_at(t, node, last_pong_received_at) do
    :ets.update_element(t.table, node, {2, last_pong_received_at})
    t
  end

  @spec update_services(t(), node(), services :: [atom()]) :: t()
  def update_services(t, node, services) do
    :ets.update_element(t.table, node, {3, services})
    t
  end
end
