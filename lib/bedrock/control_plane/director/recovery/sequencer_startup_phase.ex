defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase do
  @moduledoc """
  Solves the fundamental ordering problem by starting the sequencer component that
  provides authoritative global version numbers for all transactions.

  Without global ordering, transaction processing would collapse as different components
  assign conflicting version numbers to concurrent transactions. The sequencer is a
  critical singleton component where only one instance runs cluster-wide to ensure
  consistent version assignment.

  Starts the sequencer process on the director's current node with the last committed
  version from the version vector as the starting point. The sequencer assigns version
  numbers incrementally from this baseline, ensuring no gaps or overlaps in the sequence.
  Configured with current epoch, director PID, and OTP name for service coordination.

  Immediately halts recovery with a fatal error if sequencer startup fails, since version
  assignment is fundamental to transaction processing. Unlike temporary resource shortages,
  sequencer startup failure indicates serious system problems requiring immediate attention.

  Transitions to proxy startup once the sequencer is operational and ready.

  See the Sequencer Startup section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the ordering problem and startup process.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.DataPlane.Sequencer

  @doc """
  Execute the sequencer startup phase of recovery.

  Starts the sequencer component with the current epoch and last committed version
  from the version vector.
  """
  @impl true
  def execute(recovery_attempt, context) do
    starter_fn = get_starter_function(recovery_attempt, context)

    recovery_attempt
    |> build_sequencer_child_spec()
    |> starter_fn.(node())
    |> handle_sequencer_result(recovery_attempt)
  end

  # Private helper functions

  @spec get_starter_function(RecoveryAttempt.t(), map()) :: (Supervisor.child_spec(), node() ->
                                                               {:ok, pid()} | {:error, term()})
  defp get_starter_function(recovery_attempt, context) do
    Map.get(context, :start_supervised_fn, fn child_spec, _node ->
      starter_fn = recovery_attempt.cluster.otp_name(:sup) |> Shared.starter_for()
      starter_fn.(child_spec, node())
    end)
  end

  @spec build_sequencer_child_spec(RecoveryAttempt.t()) :: Supervisor.child_spec()
  defp build_sequencer_child_spec(recovery_attempt) do
    {_first_version, last_committed_version} = recovery_attempt.version_vector

    Sequencer.child_spec(
      director: self(),
      epoch: recovery_attempt.epoch,
      last_committed_version: last_committed_version,
      otp_name: recovery_attempt.cluster.otp_name(:sequencer)
    )
  end

  @spec handle_sequencer_result({:ok, pid()} | {:error, term()}, RecoveryAttempt.t()) ::
          {RecoveryAttempt.t(), module()} | {RecoveryAttempt.t(), {:stalled, term()}}
  defp handle_sequencer_result({:ok, sequencer}, recovery_attempt) do
    updated_recovery_attempt = %{recovery_attempt | sequencer: sequencer}

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase}
  end

  defp handle_sequencer_result({:error, reason}, recovery_attempt) do
    {recovery_attempt, {:error, {:failed_to_start, :sequencer, node(), reason}}}
  end
end
