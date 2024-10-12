defmodule Bedrock.ControlPlane.Config.RecoveryAttempt do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{
          attempt: pos_integer()
        }
  defstruct attempt: 1

  @spec new(transaction_system_layout :: TransactionSystemLayout.t()) :: t()
  def new(_transaction_system_layout) do
    %__MODULE__{
      attempt: 1
    }
  end

  def new_from_previous(previous_recovery_attempt, _transaction_system_layout) do
    %__MODULE__{
      attempt: previous_recovery_attempt.attempt + 1
    }
  end
end
