defmodule Bedrock.ControlPlane.Director.Recovery.SequencerPhase do
  @moduledoc """
  Handles the :define_sequencer phase of recovery.

  This phase is responsible for starting the sequencer component
  which assigns global version numbers to transactions.
  """

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared

  @doc """
  Execute the sequencer definition phase of recovery.

  Starts the sequencer component with the current epoch and version vector.
  """
  @spec execute(RecoveryAttempt.t()) :: RecoveryAttempt.t()
  def execute(%RecoveryAttempt{state: :define_sequencer} = recovery_attempt) do
    starter_fn = get_starter_function(recovery_attempt)

    recovery_attempt
    |> build_sequencer_child_spec()
    |> starter_fn.(node())
    |> handle_sequencer_result(recovery_attempt)
  end

  # Private helper functions

  defp get_starter_function(recovery_attempt) do
    :sup
    |> recovery_attempt.cluster.otp_name()
    |> Shared.starter_for()
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

  defp handle_sequencer_result({:ok, sequencer}, recovery_attempt),
    do: %{recovery_attempt | sequencer: sequencer, state: :define_commit_proxies}

  defp handle_sequencer_result({:error, reason}, recovery_attempt),
    do: %{
      recovery_attempt
      | state: {:stalled, {:failed_to_start, :sequencer, node(), reason}}
    }
end
