defmodule Bedrock.ControlPlane.Director.State do
  @moduledoc """
  Internal state structure for the Director process.
  """

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.Service.Worker

  @type state :: :starting | :recovery | :running | :stopped
  @type timer_registry :: %{atom() => reference()}

  @type t :: %__MODULE__{
          distributor: pid() | nil,
          distributor_monitor: reference() | nil,
          distributor_retry_ms: pos_integer(),
          distributor_start_fn: (keyword() -> {:ok, pid()} | {:error, term()}) | nil,
          distributor_wiring:
            %{
              logs: map(),
              log_refs: map(),
              recovery_authority: Bedrock.Service.RecoveryAuthority.input()
            }
            | nil,
          state: state(),
          epoch: Bedrock.epoch(),
          publication_sequence: non_neg_integer(),
          bootstrap_reservation: map() | nil,
          pending_publication: map() | nil,
          cluster: module(),
          config: Config.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t() | nil,
          prior_core_state: CoreState.t() | nil,
          coordinator: pid(),
          node_capabilities: %{Cluster.capability() => [node()]},
          timers: timer_registry() | nil,
          services: %{Worker.id() => {atom(), {atom(), node()}}},
          lock_token: binary(),
          recovery_attempt: Config.RecoveryAttempt.t() | nil
        }
  defstruct distributor: nil,
            distributor_monitor: nil,
            distributor_retry_ms: 1_000,
            distributor_start_fn: nil,
            distributor_wiring: nil,
            state: :starting,
            epoch: nil,
            publication_sequence: 0,
            bootstrap_reservation: nil,
            pending_publication: nil,
            cluster: nil,
            config: nil,
            transaction_system_layout: nil,
            prior_core_state: nil,
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

    @spec update_config(State.t(), updater :: (Config.t() -> Config.t())) :: State.t()
    def update_config(t, updater), do: %{t | config: updater.(t.config)}
  end
end
