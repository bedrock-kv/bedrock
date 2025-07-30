defmodule Bedrock.ControlPlane.Director.NodeTracking do
  @moduledoc """
  The NodeTracking module is responsible for keeping track of the state of the
  nodes in the cluster. This includes the list of capabilities that the node is
  advertising, the last time it responded to a ping and whether or not it is
  considered to be alive.
  """

  alias Bedrock.Cluster

  @type t :: :ets.table()
  @type node_capabilities :: %{Cluster.capability() => [node()]}

  @typep last_seen_at :: integer() | :unknown
  @typep capabilities :: [Cluster.capability()] | :unknown
  @typep up_down :: :up | :down
  @typep authorized :: boolean()

  @spec row(
          node(),
          last_seen_at(),
          capabilities(),
          up_down(),
          authorized(),
          Bedrock.version() | nil
        ) ::
          {node(), last_seen_at(), capabilities(), up_down(), authorized(),
           Bedrock.version() | nil}
  defp row(node, last_seen_at, capabilities, up_down, authorized, minimum_read_version),
    do: {node, last_seen_at, capabilities, up_down, authorized, minimum_read_version}

  @doc """
  Create a new node tracking table with the given nodes.
  """
  @spec new(nodes :: [node()]) :: t()
  def new(nodes) do
    t = :ets.new(nil, [:ordered_set])
    :ets.insert(t, nodes |> Enum.map(&row(&1, :unknown, :unknown, :down, true, nil)))
    t
  end

  @doc """
  Add a new node to the node tracking table.
  """
  @spec add_node(t(), node(), authorized :: boolean()) :: t()
  def add_node(t, node, authorized) do
    :ets.insert(t, row(node, :unknown, :unknown, :up, authorized, nil))
    t
  end

  @doc """
  Get the list of nodes that have not responded to a ping in the last
  `liveness_timeout_in_ms` milliseconds or have never responded to a ping.
  """
  @spec dying_nodes(t(), now :: integer(), liveness_timeout_in_ms :: Bedrock.timeout_in_ms()) ::
          [node()]
  def dying_nodes(t, now, liveness_timeout_in_ms) do
    :ets.select(t, [
      {{:"$1", :"$2", :_, :up, :_, :_},
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
  @spec dead_nodes(t(), now :: integer(), liveness_timeout_in_ms :: Bedrock.timeout_in_ms()) ::
          [node()]
  def dead_nodes(t, now, liveness_timeout_in_ms) do
    :ets.select(t, [
      {{:"$1", :"$2", :_, :"$4", :_, :_},
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

      [_row = {^node, _last_seen_at, _capabilities, up_down, _authorized, _minimum_read_version}] ->
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
      [] ->
        false

      [_row = {^node, _last_seen_at, _capabilities, _up_down, authorized, _minimum_read_version}] ->
        authorized
    end
  end

  @doc """
  Get a list of the capabilities that a node is advertising.
  """
  @spec capabilities(t(), node()) :: [Cluster.capability()] | :unknown
  def capabilities(t, node) do
    :ets.lookup(t, node)
    |> case do
      [] ->
        :unknown

      [_row = {^node, _last_seen_at, capabilities, _up_down, _authorized, _minimum_read_version}] ->
        capabilities
    end
  end

  @doc """
  Update the last time that a node responded to a ping.
  """
  @spec update_last_seen_at(
          t(),
          node(),
          last_seen_at :: integer()
        ) ::
          t()
  def update_last_seen_at(t, node, last_seen_at) do
    :ets.update_element(t, node, [{2, last_seen_at}, {4, :up}])
    t
  end

  @doc """
  Get the last time that a node responded to a ping.
  """
  @spec update_minimum_read_version(
          t(),
          node(),
          minimum_read_version :: Bedrock.version() | nil
        ) ::
          t()
  def update_minimum_read_version(t, node, minimum_read_version) do
    :ets.update_element(t, node, {6, minimum_read_version})
    t
  end

  @doc """
  Update the list of capabilities that a node is advertising.
  """
  @spec update_capabilities(t(), node(), capabilities :: [Cluster.capability()]) :: t()
  def update_capabilities(t, node, capabilities) do
    :ets.update_element(t, node, {3, capabilities})
    t
  end

  @doc """
  Get a list of nodes that are alive, authorized, and have the specified capability.
  """
  @spec nodes_with_capability(t(), capability :: Cluster.capability()) :: [node()]
  def nodes_with_capability(t, capability) do
    # Get all alive and authorized nodes first
    alive_authorized_nodes =
      :ets.select(t, [
        {{:"$1", :_, :_, :up, true, :_}, [], [:"$1"]}
      ])

    # Filter by capability (ETS can't easily match list membership, so we do it in Elixir)
    alive_authorized_nodes
    |> Enum.filter(fn node ->
      case capabilities(t, node) do
        :unknown ->
          false

        node_capabilities when is_list(node_capabilities) ->
          capability in node_capabilities
      end
    end)
  end

  @doc """
  Mark a node as being down.
  """
  @spec down(t(), node()) :: t()
  def down(t, node) do
    :ets.update_element(t, node, [{4, :down}, {6, nil}])
    t
  end

  @doc """
  Extract a static snapshot of node capabilities for recovery phases.
  Only includes nodes that are up and authorized.
  """
  @spec extract_node_capabilities(t()) :: node_capabilities()
  def extract_node_capabilities(t) do
    # Get all alive and authorized nodes with their capabilities
    alive_authorized_nodes_with_caps =
      :ets.select(t, [
        {{:"$1", :_, :"$3", :up, true, :_}, [], [{{:"$1", :"$3"}}]}
      ])

    # Group nodes by capability, filtering out nodes with unknown capabilities
    alive_authorized_nodes_with_caps
    |> Enum.filter(fn {_node, capabilities} -> is_list(capabilities) end)
    |> Enum.flat_map(fn {node, capabilities} ->
      Enum.map(capabilities, fn capability -> {capability, node} end)
    end)
    |> Enum.group_by(fn {capability, _node} -> capability end, fn {_capability, node} -> node end)
  end
end
