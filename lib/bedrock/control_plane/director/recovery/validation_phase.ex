defmodule Bedrock.ControlPlane.Director.Recovery.ValidationPhase do
  @moduledoc """
  Performs final validation before Transaction System Layout construction.

  Conducts comprehensive checks to ensure all transaction system components
  are properly configured and ready for TSL construction. This is the last
  opportunity to detect problems before building the critical system blueprint.

  Validates that sequencer, commit proxies, resolvers, logs, and storage teams
  are all operational and properly connected. Ensures all required services
  are available and that the recovery state is consistent and complete.

  The validation phase acts as a safety gate before the critical TSL construction
  phase. Any problems detected here can be addressed through recovery retry
  rather than system transaction failure during persistence.

  Transitions to Transaction System Layout construction to build the TSL and prepare services.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, _context) do
    case validate_recovery_state(recovery_attempt) do
      :ok ->
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase}

      {:error, reason} ->
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  # Validate that recovery state is ready for system transaction
  @spec validate_recovery_state(RecoveryAttempt.t()) ::
          :ok | {:error, {:invalid_recovery_state, atom() | {atom(), [binary()]}}}
  defp validate_recovery_state(recovery_attempt) do
    with :ok <- validate_sequencer(recovery_attempt.sequencer),
         :ok <- validate_commit_proxies(recovery_attempt.proxies),
         :ok <- validate_resolvers(recovery_attempt.resolvers),
         :ok <- validate_logs(recovery_attempt.logs, recovery_attempt.transaction_services) do
      :ok
    else
      {:error, reason} -> {:error, {:invalid_recovery_state, reason}}
    end
  end

  @spec validate_sequencer(nil | pid()) :: :ok | {:error, atom()}
  defp validate_sequencer(nil), do: {:error, :no_sequencer}
  defp validate_sequencer(sequencer) when is_pid(sequencer), do: :ok
  defp validate_sequencer(_), do: {:error, :invalid_sequencer}

  @spec validate_commit_proxies([pid()]) :: :ok | {:error, atom()}
  defp validate_commit_proxies([]), do: {:error, :no_commit_proxies}

  defp validate_commit_proxies(proxies) when is_list(proxies) do
    if Enum.all?(proxies, &is_pid/1) do
      :ok
    else
      {:error, :invalid_commit_proxies}
    end
  end

  defp validate_commit_proxies(_), do: {:error, :invalid_commit_proxies}

  @spec validate_resolvers([{Bedrock.key(), pid()}]) :: :ok | {:error, atom()}
  defp validate_resolvers([]), do: {:error, :no_resolvers}

  defp validate_resolvers(resolvers) when is_list(resolvers) do
    valid_resolvers =
      Enum.all?(resolvers, fn
        {_start_key, pid} when is_pid(pid) -> true
        _ -> false
      end)

    if valid_resolvers do
      :ok
    else
      {:error, :invalid_resolvers}
    end
  end

  @spec validate_logs(%{Log.id() => LogDescriptor.t()}, %{Worker.id() => ServiceDescriptor.t()}) ::
          :ok | {:error, {:missing_log_services, [binary()]}}
  defp validate_logs(logs, transaction_services) when is_map(logs) do
    log_ids = Map.keys(logs)

    trace_recovery_log_validation_started(log_ids, transaction_services)

    # Check that all log IDs have corresponding services in transaction_services
    missing_services =
      log_ids
      |> Enum.reject(fn log_id ->
        case Map.get(transaction_services, log_id) do
          %{status: {:up, pid}} when is_pid(pid) ->
            trace_recovery_log_service_status(log_id, :found, %{status: {:up, pid}})
            true

          _ ->
            trace_recovery_log_service_status(log_id, :missing, nil)
            false
        end
      end)

    case missing_services do
      [] ->
        :ok

      missing ->
        {:error, {:missing_log_services, missing}}
    end
  end
end
