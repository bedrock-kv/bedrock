defmodule Bedrock.ControlPlane.Config do
  @moduledoc """
  A `Config` is a data structure that describes the configuration of the
  control plane. It contains the current state of the cluster, the parameters
  that are used to configure the cluster, the policies that are used to
  configure the cluster and the layout of the transaction system.
  """
  alias Bedrock.ControlPlane.Config.Policies
  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  def default_ping_rate_in_hz, do: 5

  @typedoc """
  Struct representing the control plane configuration.

  ## Fields
    - `state` - The current state of the cluster.
    - `nodes` - The nodes that are part of the cluster.
    - `parameters` - The parameters that are used to configure the cluster.
    - `policies` - The policies that are used to configure the cluster.
    - `transaction_system_layout` - The layout of the transaction system.
  """
  @type t :: %__MODULE__{
          state: state(),
          nodes: [node()],
          parameters: Parameters.t() | nil,
          policies: Policies.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil
        }
  defstruct state: :uninitialized,
            nodes: [],
            parameters: nil,
            policies: nil,
            transaction_system_layout: nil

  @type state :: :uninitialized | :recovery | :running | :stopping

  @doc """
  Creates a new `Config` struct.
  """
  @spec new(nodes :: [node()]) :: t()
  def new(nodes) do
    %__MODULE__{
      state: :uninitialized,
      nodes: nodes
    }
  end

  @doc """
  Creates a new `Config` struct.
  """
  @spec new(
          state(),
          nodes :: [node()],
          Parameters.t(),
          Policies.t(),
          TransactionSystemLayout.t()
        ) :: t()
  def new(state, nodes, parameters, policies, transaction_system_layout) do
    %__MODULE__{
      state: state,
      nodes: nodes,
      parameters: parameters,
      policies: policies,
      transaction_system_layout: transaction_system_layout
    }
  end

  @spec nodes(t()) :: [node()]
  def nodes(t), do: t.nodes

  @doc "Returns true if the cluster will allow volunteer nodes to join."
  @spec allow_volunteer_nodes_to_join?(t()) :: boolean()
  def allow_volunteer_nodes_to_join?(t),
    do: get_in(t.policies.allow_volunteer_nodes_to_join) || false

  @doc "Returns the nodes that are part of the cluster."
  @spec coordinators(t()) :: [node()]
  def coordinators(t), do: t.coordinators || []

  @doc "Returns the pid of the current `Sequencer`."
  @spec sequencer(t()) :: pid() | nil
  def sequencer(t), do: get_in(t.transaction_system_layout.sequencer)

  @doc "Returns the pid of the current `DataDistributor`."
  @spec data_distributor(t()) :: pid() | nil
  def data_distributor(t), do: get_in(t.transaction_system_layout.data_distributor)

  @doc "Returns the ping rate in milliseconds."
  @spec ping_rate_in_ms(t()) :: non_neg_integer()
  def ping_rate_in_ms(t),
    do: div(1000, get_in(t.parameters.ping_rate_in_hz) || default_ping_rate_in_hz())
end
