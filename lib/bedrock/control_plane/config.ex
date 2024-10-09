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

  @typedoc """
  Struct representing the control plane configuration.

  ## Fields
    - `state` - The current state of the cluster.
    - `coordinators` - The coordinators of the cluster.
    - `parameters` - The parameters that are used to configure the cluster.
    - `policies` - The policies that are used to configure the cluster.
    - `transaction_system_layout` - The layout of the transaction system.
  """
  @type t :: %__MODULE__{
          state: state(),
          coordinators: [node()],
          epoch: non_neg_integer(),
          parameters: Parameters.t() | nil,
          policies: Policies.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil
        }
  defstruct state: :uninitialized,
            coordinators: [],
            epoch: 0,
            parameters: nil,
            policies: nil,
            transaction_system_layout: nil

  @type state :: :uninitialized | :recovery | :running | :stopping

  @doc """
  Creates a new `Config` struct.
  """
  @spec new(coordinators :: [node()]) :: t()
  def new(coordinators) do
    %__MODULE__{
      state: :uninitialized,
      coordinators: coordinators,
      epoch: 0,
      parameters: Parameters.new(),
      policies: Policies.new(),
      transaction_system_layout: TransactionSystemLayout.new()
    }
  end

  @doc "Returns true if the cluster will allow volunteer nodes to join."
  @spec allow_volunteer_nodes_to_join?(t()) :: boolean()
  def allow_volunteer_nodes_to_join?(t),
    do: get_in(t.policies.allow_volunteer_nodes_to_join) || true

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
  @spec ping_rate_in_ms(t()) :: pos_integer()
  def ping_rate_in_ms(t),
    do: div(1000, get_in(t.parameters.ping_rate_in_hz))

  defmodule Mutations do
    @type t :: Bedrock.ControlPlane.Config.t()

    @spec with_new_epoch(t(), pos_integer()) :: t()
    def with_new_epoch(config, epoch), do: put_in(config.epoch, epoch)

    @spec with_new_controller(t(), pid()) :: t()
    def with_new_controller(config, controller),
      do:
        update_in(
          config.transaction_system_layout,
          &TransactionSystemLayout.Tools.set_controller(&1, controller)
        )
  end
end
