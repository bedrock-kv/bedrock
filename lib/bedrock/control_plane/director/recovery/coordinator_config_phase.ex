defmodule Bedrock.ControlPlane.Director.Recovery.CoordinatorConfigPhase do
  @moduledoc """
  Handles the :persist_coordinator_config phase of recovery.

  This phase persists the cluster configuration to coordinators via raft consensus,
  ensuring that future cold starts can recover from persistent state instead of
  doing first-time initialization.
  """

  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery

  @spec execute(RecoveryAttempt.t(), Recovery.recovery_context()) :: RecoveryAttempt.t()
  def execute(%RecoveryAttempt{state: :persist_coordinator_config} = recovery_attempt, context) do
    updated_config =
      context.cluster_config
      |> Map.take([:epoch, :coordinators, :parameters, :policies])
      |> Map.put(:transaction_system_layout, recovery_attempt.transaction_system_layout)

    context.coordinator
    |> Coordinator.write_config(updated_config, 5_000)
    |> case do
      :ok ->
        %{recovery_attempt | state: :monitor_components}

      {:error, _reason} ->
        %{recovery_attempt | state: :monitor_components}
    end
  end
end
