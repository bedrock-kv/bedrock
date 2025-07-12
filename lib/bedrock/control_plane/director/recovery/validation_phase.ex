defmodule Bedrock.ControlPlane.Director.Recovery.ValidationPhase do
  @moduledoc """
  Handles the :final_checks phase of recovery.

  This phase is responsible for performing final validation
  before proceeding to system state persistence.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @doc """
  Execute the validation phase of recovery.

  Performs final checks to ensure the system is ready
  for state persistence. Currently just transitions to
  the next phase.
  """
  @impl true
  def execute(%{state: :final_checks} = recovery_attempt, _context) do
    recovery_attempt |> Map.put(:state, :persist_system_state)
  end
end
