defmodule Bedrock.ControlPlane.ClusterController.State do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type state :: :starting | :recovery | :running | :stopped

  @type t :: %__MODULE__{
          state: state(),
          epoch: Bedrock.epoch(),
          my_relief: {Bedrock.epoch(), controller :: pid()} | nil,
          cluster: module(),
          config: Config.t() | nil,
          coordinator: pid(),
          node_tracking: NodeTracking.t(),
          timers: map() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          last_transaction_layout_id: TransactionSystemLayout.id()
        }
  defstruct state: :starting,
            epoch: nil,
            my_relief: nil,
            cluster: nil,
            config: nil,
            coordinator: nil,
            node_tracking: nil,
            service_directory: nil,
            timers: nil,
            transaction_system_layout: nil,
            last_transaction_layout_id: 0

  defmodule Changes do
    alias Bedrock.ControlPlane.ClusterController.State

    @spec put_state(State.t(), State.state()) :: State.t()
    def put_state(t, state), do: %{t | state: state}

    @spec put_my_relief(State.t(), {Bedrock.epoch(), pid()}) :: State.t()
    def put_my_relief(t, my_relief), do: %{t | my_relief: my_relief}

    @spec update_config(State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}

    @spec put_last_transaction_layout_id(State.t(), TransactionSystemLayout.id()) :: State.t()
    def put_last_transaction_layout_id(t, last_transaction_layout_id),
      do: %{t | last_transaction_layout_id: last_transaction_layout_id}
  end
end
