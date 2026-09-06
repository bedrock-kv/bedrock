defmodule Bedrock.ControlPlane.Director.Recovery.CoreStateValidationPhase do
  @moduledoc """
  Early recovery phase that type-checks the PRIOR CORE STATE before any
  later phase trusts it.

  On a cold boot the record is genuinely durable — written by a previous
  epoch, possibly by a previous version of this software, and read back
  off object storage. (On a warm relaunch it is projected in memory from
  this coordinator's own last layout and never round-trips storage, so
  the check is cheap there and meaningful here.) Everything after this
  point locks and copies from the logs it names, so a type mismatch
  (integer tag ranges arriving as Version.t() binaries, say) would
  otherwise surface as an MVCC lookup failure far from its cause.
  Validating at the boundary makes the durable record the thing that
  fails, with diagnostics naming it.

  What it checks is the `logs` field: each entry's tag ranges must be
  integers, not binaries. The validator also has a `resolvers` check,
  which is vacuous against this record — the prior core state carries no
  resolvers, and the previous epoch's resolver pids would be worthless
  if it did.

  ## Error Handling

  On validation failure, this phase stalls recovery with `{:corrupted_core_state, details}`
  to allow operators to investigate and fix the underlying data corruption rather
  than failing silently or propagating errors further into the recovery process.

  ## Integration Point

  Runs first in the recovery pipeline, on the prior core state the director
  hands recovery in its context and before any phase that depends on
  type-correct fields in it. This provides a clear failure point with detailed
  diagnostics.

  Transitions to the next appropriate recovery phase on successful validation.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.ControlPlane.Config.TypeSafetyValidator

  @doc """
  Validates the prior core state's type safety.

  Returns `{:stalled, {:corrupted_core_state, validation_error}}` on validation failure
  to halt recovery and provide clear diagnostics. Logs detailed error information
  for debugging the underlying data corruption.

  On success, transitions to the next recovery phase without modifying the
  recovery attempt (this is a pure validation phase).
  """
  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, %{prior_core_state: %{} = core_state}) do
    case TypeSafetyValidator.validate_type_safety(core_state) do
      :ok ->
        trace_recovery_core_state_validation_success()
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LockingPhase}

      {:error, validation_error} ->
        trace_recovery_core_state_validation_failed(core_state, validation_error)
        {recovery_attempt, {:stalled, {:corrupted_core_state, validation_error}}}
    end
  end

  def execute(recovery_attempt, _context),
    do: {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.InitializationPhase}
end
