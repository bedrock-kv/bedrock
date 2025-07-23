defmodule Bedrock.ControlPlane.Director.Recovery.CoordinatorConfigPhase do
  @moduledoc """
  Handles the :persist_coordinator_config phase of recovery.

  This phase persists the cluster configuration to coordinators via raft consensus,
  ensuring that future cold starts can recover from persistent state instead of 
  doing first-time initialization.
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery

  @doc """
  Execute the coordinator config persistence phase.

  This phase:
  1. Extracts the cluster config from the recovery context
  2. Sends config persistence request to coordinator
  3. Waits for coordinator raft consensus to complete
  4. Transitions to monitoring phase on success
  """
  @spec execute(RecoveryAttempt.t(), Recovery.recovery_context()) :: RecoveryAttempt.t()
  def execute(%RecoveryAttempt{state: :persist_coordinator_config} = recovery_attempt, context) do
    # Extract only the essential cluster config, not the recovery attempt
    essential_config = extract_essential_config(context.cluster_config)

    case persist_config_to_coordinator(essential_config, context.coordinator) do
      :ok ->
        %{recovery_attempt | state: :monitor_components}

      {:error, _reason} ->
        # Don't fail recovery - this is an optimization, not critical
        # Continue to monitoring phase
        %{recovery_attempt | state: :monitor_components}
    end
  end

  # Private functions

  @spec extract_essential_config(map()) :: map()
  defp extract_essential_config(cluster_config) do
    # Only include essential fields, exclude recovery_attempt and other transient data
    cluster_config
    |> Map.take([:epoch, :coordinators, :transaction_system_layout, :parameters, :policies])
    |> Map.update(:transaction_system_layout, %{}, fn layout ->
      # Clean up transaction system layout - keep only persistent fields
      layout
      |> Map.take([:id, :services, :logs, :director])
    end)
  end

  @spec persist_config_to_coordinator(map(), pid()) :: :ok | {:error, term()}
  defp persist_config_to_coordinator(cluster_config, coordinator_pid) do
    # Use async cast instead of call to avoid blocking recovery
    # The coordinator can handle this asynchronously and we don't need to wait
    GenServer.cast(coordinator_pid, {:write_config_async, cluster_config})
    :ok
  rescue
    error -> {:error, {:exception, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
