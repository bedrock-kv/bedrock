defmodule Bedrock.ControlPlane.Director.Recovery.StartPhase do
  @moduledoc """
  Handles the :start phase of recovery.

  This phase is responsible for initializing the recovery attempt with a timestamp
  and transitioning to the service locking phase.
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  import Bedrock.Internal.Time, only: [now: 0]

  @doc """
  Execute the start phase of recovery.

  Sets the started_at timestamp and transitions to :lock_available_services.
  """
  @spec execute(RecoveryAttempt.t()) :: RecoveryAttempt.t()
  def execute(%RecoveryAttempt{state: :start} = recovery_attempt) do
    %{recovery_attempt | started_at: now(), state: :lock_available_services}
  end
end
