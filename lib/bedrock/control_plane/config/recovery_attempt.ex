defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type state ::
          :recruiting
          | :replaying_logs
          | :repairing_data_distribution
          | :defining_proxies_and_resolvers
          | :final_checks
          | :completed

  @type t :: %__MODULE__{
          attempt: pos_integer(),
          epoch: non_neg_integer() | nil,
          started_at: DateTime.t() | nil,
          state: state() | nil,
          last_transaction_system_layout: TransactionSystemLayout.t() | nil
        }
  defstruct attempt: nil,
            epoch: nil,
            started_at: nil,
            state: :recruiting,
            last_transaction_system_layout: nil

  @spec new(
          previous_recovery_attempt :: t() | nil,
          Bedrock.epoch(),
          started_at :: DateTime.t(),
          state(),
          TransactionSystemLayout.t()
        ) ::
          t()
  def new(nil, epoch, started_at, state, transaction_system_layout) do
    %__MODULE__{
      attempt: 1,
      epoch: epoch,
      started_at: started_at,
      state: state,
      last_transaction_system_layout: transaction_system_layout
    }
  end

  def new(%{attempt: previous_attempt}, epoch, started_at, state, transaction_system_layout) do
    %__MODULE__{
      attempt: 1 + previous_attempt,
      epoch: epoch,
      started_at: started_at,
      state: state,
      last_transaction_system_layout: transaction_system_layout
    }
  end

  defmodule Mutations do
    alias Bedrock.ControlPlane.Config.RecoveryAttempt
    @type t :: RecoveryAttempt.t()

    @spec update_state(t(), RecoveryAttempt.state()) :: t()
    def update_state(t, state), do: put_in(t.state, state)

    @spec update_started_at(t(), DateTime.t()) :: t()
    def update_started_at(t, started_at), do: put_in(t.started_at, started_at)
  end
end
