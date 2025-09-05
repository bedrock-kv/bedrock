defmodule Bedrock.ControlPlane.Director.Recovery.TSLValidationPhase do
  @moduledoc """
  Early recovery phase that validates TSL type safety to prevent data corruption.

  This phase runs defensively on recovered TSL data, checking for type mismatches
  like integer-to-binary version conversion errors that can cause MVCC lookup failures.
  It specifically validates:

  - `logs` field has integer ranges (not Version.t() binaries)
  - `version_vector` field contains Version.t() binaries (not integers)
  - `durable_version` field contains Version.t() binary (not integer)
  - `storage_teams` structure is valid
  - `resolvers` structure is valid

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
  Validates TSL type safety using defensive validation.

  Returns `{:stalled, {:corrupted_tsl, validation_error}}` on validation failure
  to halt recovery and provide clear diagnostics. Logs detailed error information
  for debugging the underlying data corruption.

  On success, transitions to the next recovery phase without modifying the
  recovery attempt (this is a pure validation phase).
  """
  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, %{old_transaction_system_layout: %{} = tsl_to_validate}) do
    case TSLTypeValidator.validate_type_safety(tsl_to_validate) do
      :ok ->
        trace_recovery_tsl_validation_success()
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LockingPhase}

      {:error, validation_error} ->
        trace_recovery_tsl_validation_failed(tsl_to_validate, validation_error)
        {recovery_attempt, {:stalled, {:corrupted_tsl, validation_error}}}
    end
  end

  def execute(recovery_attempt, _context),
    do: {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.InitializationPhase}
end
