defmodule Bedrock.ControlPlane.ClusterController.State do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type state :: :starting | :recovery | :running | :stopping

  @type t :: %__MODULE__{
          epoch: Bedrock.epoch(),
          otp_name: atom(),
          cluster: module(),
          config: Config.t() | nil,
          coordinator: pid(),
          node_tracking: NodeTracking.t(),
          timers: map() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          last_transaction_layout_id: TransactionSystemLayout.id()
        }
  defstruct epoch: nil,
            otp_name: nil,
            cluster: nil,
            config: nil,
            state: :starting,
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

    @spec update_config(State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}

    @spec put_last_transaction_layout_id(State.t(), TransactionSystemLayout.id()) :: State.t()
    def put_last_transaction_layout_id(t, last_transaction_layout_id),
      do: %{t | last_transaction_layout_id: last_transaction_layout_id}
  end
end
