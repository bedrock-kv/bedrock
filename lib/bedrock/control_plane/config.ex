defmodule Bedrock.ControlPlane.Config do
  @moduledoc """
  A `Config` is a data structure that describes the configuration of the
  control plane. It contains the current state of the cluster, the parameters
  that are used to configure the cluster, the policies that are used to
  configure the cluster and the layout of the transaction system.
  """
  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Config.Policies
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @typedoc """
  Struct representing the control plane configuration.

  ## Fields
    - `coordinators` - The coordinators of the cluster.
    - `parameters` - The parameters that are used to configure the cluster.
    - `policies` - The policies that are used to configure the cluster.
    - `transaction_system_layout` - The layout of the transaction system.
  """
  @type t :: %{
          optional(:recovery_attempt) => RecoveryAttempt.t(),
          coordinators: [node()],
          epoch: non_neg_integer(),
          parameters: Parameters.t() | nil,
          policies: Policies.t() | nil,
          transaction_system_layout: TransactionSystemLayout.t()
        }

  @type state :: :uninitialized | :recovery | :running | :stopping

  @spec key_range(min_key :: Bedrock.key(), max_key_exclusive :: Bedrock.key()) ::
          Bedrock.key_range()
  def key_range(min_key, max_key_exclusive) when min_key < max_key_exclusive,
    do: {min_key, max_key_exclusive}

  @doc """
  Creates a new `Config` struct.
  """
  @spec new(coordinators :: [node()], epoch :: non_neg_integer()) :: t()
  def new(coordinators, epoch \\ 0) do
    %{
      coordinators: coordinators,
      epoch: epoch,
      parameters: Parameters.new(coordinators),
      policies: Policies.default_policies(),
      transaction_system_layout: TransactionSystemLayout.default()
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

  @doc "Returns the ping rate in milliseconds."
  @spec ping_rate_in_ms(t()) :: pos_integer()
  def ping_rate_in_ms(t),
    do: div(1000, get_in(t.parameters.ping_rate_in_hz))

  defmodule Changes do
    alias Bedrock.ControlPlane.Config

    # Puts

    @spec put_epoch(Config.t(), Bedrock.epoch()) :: Config.t()
    def put_epoch(t, epoch), do: %{t | epoch: epoch}

    @spec put_recovery_attempt(Config.t(), RecoveryAttempt.t() | nil) :: Config.t()
    def put_recovery_attempt(t, recovery_attempt),
      do: Map.put(t, :recovery_attempt, recovery_attempt)

    @spec put_transaction_system_layout(Config.t(), TransactionSystemLayout.t()) :: Config.t()
    def put_transaction_system_layout(t, transaction_system_layout),
      do: %{t | transaction_system_layout: transaction_system_layout}

    @spec put_parameters(Config.t(), Parameters.t()) :: Config.t()
    def put_parameters(t, parameters), do: %{t | parameters: parameters}

    # Updates

    @spec update_recovery_attempt!(Config.t(), (RecoveryAttempt.t() -> RecoveryAttempt.t())) ::
            Config.t()
    def update_recovery_attempt!(t, f),
      do: Map.update!(t, :recovery_attempt, f)

    @spec update_transaction_system_layout(
            Config.t(),
            (TransactionSystemLayout.t() ->
               TransactionSystemLayout.t())
          ) ::
            Config.t()
    def update_transaction_system_layout(t, updater),
      do: %{t | transaction_system_layout: updater.(t.transaction_system_layout)}

    @spec update_parameters(Config.t(), (Parameters.t() -> Parameters.t())) :: Config.t()
    def update_parameters(t, updater), do: %{t | parameters: updater.(t.parameters)}
  end
end
