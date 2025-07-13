defmodule Bedrock.ControlPlane.Director.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director.NodeTracking
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.Service.Worker

  @type state :: :starting | :recovery | :running | :stopped

  @type t :: %__MODULE__{
          state: state(),
          epoch: Bedrock.epoch(),
          my_relief: {Bedrock.epoch(), director :: pid()} | nil,
          cluster: module(),
          config: Config.t() | nil,
          coordinator: pid(),
          node_tracking: NodeTracking.t(),
          timers: map() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          services: %{Worker.id() => ServiceDescriptor.t()},
          lock_token: binary()
        }
  defstruct state: :starting,
            epoch: nil,
            my_relief: nil,
            cluster: nil,
            config: nil,
            coordinator: nil,
            node_tracking: nil,
            timers: nil,
            transaction_system_layout: nil,
            services: %{},
            lock_token: nil

  defmodule Changes do
    alias Bedrock.ControlPlane.Director.State

    @spec put_state(State.t(), State.state()) :: State.t()
    def put_state(t, state), do: %{t | state: state}

    @spec put_my_relief(State.t(), {Bedrock.epoch(), pid()}) :: State.t()
    def put_my_relief(t, my_relief), do: %{t | my_relief: my_relief}

    @spec update_config(State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}
  end
end
