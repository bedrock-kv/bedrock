defmodule Bedrock.ControlPlane.Director.Recovery.ValidationPhase do
  @moduledoc """
  Handles the :final_checks phase of recovery.

  This phase is responsible for performing final validation
  before proceeding to system state persistence.
  """

  @doc """
  Execute the validation phase of recovery.

  Performs final checks to ensure the system is ready
  for state persistence. Currently just transitions to
  the next phase.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :final_checks} = recovery_attempt) do
    recovery_attempt |> Map.put(:state, :persist_system_state)
  end
end
