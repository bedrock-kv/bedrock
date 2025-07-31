defmodule Bedrock.ControlPlane.Director.Recovery.RecoveryPhase do
  @moduledoc """
  Behavior for recovery phases in the Bedrock recovery process.

  All recovery phases should implement this behavior to ensure consistent
  interfaces and access to necessary context data.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  @type context :: %{
          cluster_config: Config.t(),
          old_transaction_system_layout: Config.TransactionSystemLayout.t(),
          node_capabilities: %{Bedrock.Cluster.capability() => [node()]},
          lock_token: binary(),
          available_services: %{String.t() => {atom(), {atom(), node()}}},
          coordinator: pid()
        }

  @doc """
  Execute the recovery phase with the given recovery attempt and context.

  The context provides access to Director state that phases need but
  shouldn't be stored in the recovery attempt itself.

  Returns either:
  - `{updated_recovery_attempt, next_phase_module}` - Normal transition with next phase
  - `updated_recovery_attempt` - For backward compatibility or terminal states
  """
  @callback execute(RecoveryAttempt.t(), context()) ::
              RecoveryAttempt.t() | {RecoveryAttempt.t(), module()}

  defmacro __using__(_) do
    quote do
      alias Bedrock.ControlPlane.Config.RecoveryAttempt
      alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

      @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
    end
  end
end
