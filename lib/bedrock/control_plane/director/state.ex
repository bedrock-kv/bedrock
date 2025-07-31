defmodule Bedrock.ControlPlane.Director.State do
  @moduledoc false

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.Service.Worker

  @type state :: :starting | :recovery | :running | :stopped
  @type timer_registry :: %{atom() => reference()}

  @type t :: %__MODULE__{
          state: state(),
          epoch: Bedrock.epoch(),
          my_relief: {Bedrock.epoch(), director :: pid()} | nil,
          cluster: module(),
          config: Config.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          old_transaction_system_layout: TransactionSystemLayout.t() | nil,
          coordinator: pid(),
          node_capabilities: %{Cluster.capability() => [node()]},
          timers: timer_registry() | nil,
          services: %{Worker.id() => {atom(), {atom(), node()}}},
          lock_token: binary(),
          recovery_attempt: Config.RecoveryAttempt.t() | nil
        }
  defstruct state: :starting,
            epoch: nil,
            my_relief: nil,
            cluster: nil,
            config: nil,
            transaction_system_layout: nil,
            old_transaction_system_layout: nil,
            coordinator: nil,
            node_capabilities: %{},
            timers: nil,
            services: %{},
            lock_token: nil,
            recovery_attempt: nil

  defmodule Changes do
    @moduledoc false

    alias Bedrock.ControlPlane.Director.State

    @spec put_state(State.t(), State.state()) :: State.t()
    def put_state(t, state), do: %{t | state: state}

    @spec put_my_relief(State.t(), {Bedrock.epoch(), pid()}) :: State.t()
    def put_my_relief(t, my_relief), do: %{t | my_relief: my_relief}

    @spec update_config(State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}
  end
end
