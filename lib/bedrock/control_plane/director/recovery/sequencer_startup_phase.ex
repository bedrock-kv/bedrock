defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase do
  @moduledoc """
  Starts the sequencer component that assigns global version numbers to transactions.

  The sequencer is a critical component that provides strict ordering for all
  transactions in the cluster. Only one sequencer runs at a time to ensure
  consistent version assignment across all transaction processing.

  Selects a node and starts the sequencer process with the current durable
  version as the starting point. The sequencer will assign version numbers
  incrementally from this baseline for new transactions.

  Can stall if no suitable nodes are available or if the sequencer fails to
  start. The sequencer must be operational before commit proxies can process
  transactions.

  Transitions to :define_commit_proxies once the sequencer is running and ready.
  """

  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @doc """
  Execute the sequencer definition phase of recovery.

  Starts the sequencer component with the current epoch and version vector.
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
    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.ProxyStartupPhase}
  end

  defp handle_sequencer_result({:error, reason}, recovery_attempt) do
    {recovery_attempt, {:stalled, {:failed_to_start, :sequencer, node(), reason}}}
  end
end
