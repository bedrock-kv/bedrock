defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type state ::
          :recruiting
          | :replaying_logs
          | :repairing_data_distribution
          | :defining_proxies_and_resolvers
          | :final_checks
          | :completed
          | :stalled

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
          Bedrock.epoch(),
          started_at :: DateTime.t(),
          state(),
          TransactionSystemLayout.t()
        ) ::
          t()
  def new(epoch, started_at, state, transaction_system_layout) do
    %__MODULE__{
      attempt: 1,
      epoch: epoch,
      started_at: started_at,
      state: state,
      last_transaction_system_layout: transaction_system_layout
    }
  end

  defmodule Mutations do
    alias Bedrock.ControlPlane.Config.RecoveryAttempt
    @type t :: RecoveryAttempt.t()

    @spec set_state(t(), RecoveryAttempt.state()) :: t()
    def set_state(t, state), do: put_in(t.state, state)

    @spec set_started_at(t(), DateTime.t()) :: t()
    def set_started_at(t, started_at), do: put_in(t.started_at, started_at)
  end
end
