defmodule Bedrock.ControlPlane.Director.Recovery.TSLValidationPhase do
  @moduledoc """
  Early recovery phase that type-checks the PRIOR CORE STATE before any
  later phase trusts it.

  The record is durable: it was written by a previous epoch, possibly a
  previous version of this software, and read back off object storage.
  Everything after this point locks and copies from the logs it names,
  so a type mismatch here (integer ranges arriving as Version.t()
  binaries, say) surfaces later as an MVCC lookup failure far from its
  cause. Validating at the boundary makes the durable record the thing
  that fails, with diagnostics naming it.

  It validates the `logs` field: each entry's tag ranges must be
  integers, not binaries. That is the whole prior record today
  (`Bedrock.ControlPlane.Config.CoreState`), so it is the whole check.

  ## Error Handling

  On validation failure, this phase stalls recovery with `{:corrupted_tsl, details}`
  to allow operators to investigate and fix the underlying data corruption rather
  than failing silently or propagating errors further into the recovery process.

  ## Integration Point

  Should run early in the recovery pipeline after TSL data is loaded but before
  any processing that depends on type-correct TSL fields. This provides a clear
  failure point with detailed diagnostics.

  Transitions to the next appropriate recovery phase on successful validation.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.ControlPlane.Config.TSLTypeValidator

  @doc """
  Validates the prior core state's type safety.

  Returns `{:stalled, {:corrupted_tsl, validation_error}}` on validation failure
  to halt recovery and provide clear diagnostics. Logs detailed error information
  for debugging the underlying data corruption.

  On success, transitions to the next recovery phase without modifying the
  recovery attempt (this is a pure validation phase).
  """
  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, %{prior_core_state: %{} = core_state}) do
    case TSLTypeValidator.validate_type_safety(core_state) do
      :ok ->
        trace_recovery_tsl_validation_success()
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LockingPhase}

      {:error, validation_error} ->
        trace_recovery_tsl_validation_failed(core_state, validation_error)
        {recovery_attempt, {:stalled, {:corrupted_tsl, validation_error}}}
    end
  end

  def execute(recovery_attempt, _context),
    do: {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.InitializationPhase}
end
