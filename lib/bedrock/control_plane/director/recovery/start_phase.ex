defmodule Bedrock.ControlPlane.Director.Recovery.StartPhase do
  @moduledoc """
  This is the entry point for all recovery attempts, whether triggered by
  coordinator startup, director failure, or node rejoin events.

  ## Purpose

  - **Recovery Tracking**: Records the exact timestamp when recovery begins
  - **State Initialization**: Ensures recovery attempt has proper started_at field
  - **Phase Transition**: Moves to service locking, the first substantive recovery step
  - **Telemetry Foundation**: Provides timing baseline for recovery duration metrics

  ## Why This Phase Exists

  The start phase serves as a clean entry point that separates recovery attempt
  creation (which happens in the Director) from recovery execution (which happens
  in the phase pipeline). This separation allows:

  - Consistent timing measurement across all recovery scenarios
  - Clear audit trail of when each recovery attempt actually began execution
  - Proper initialization before any potentially blocking operations

  ## State Transitions

  - **Input**: `RecoveryAttempt` with `state: :start`
  - **Output**: `RecoveryAttempt` with `state: :lock_available_services` and populated `started_at`

  The phase always succeeds and never stalls, making it a reliable foundation
  for the more complex phases that follow.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  import Bedrock.Internal.Time, only: [now: 0]

  @doc """
  Execute the start phase of recovery.

  Sets the started_at timestamp and transitions to :lock_available_services.
  """
  @impl true
  def execute(%RecoveryAttempt{state: :start} = recovery_attempt, _context) do
    %{recovery_attempt | started_at: now(), state: :lock_available_services}
  end
end
