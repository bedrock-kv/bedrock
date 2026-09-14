defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhase do
  @moduledoc """
  Sets up monitoring of all transaction system components and marks recovery as complete.

  Establishes process monitoring for sequencer, commit proxies, resolvers, logs,
  and storage servers. Any failure of these critical components will trigger
  immediate director shutdown and recovery restart.

  This monitoring implements Bedrock's fail-fast philosophy - rather than
  attempting complex error recovery, component failures cause the director
  to exit and let the coordinator restart recovery with a new epoch.

  The monitoring setup represents the final step before the cluster becomes
  operational. Once monitoring is active, the director shifts from recovery
  mode to operational mode.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_monitoring_components()

    monitor_fn = Map.get(context, :monitor_fn, &Process.monitor/1)

    # The refs are kept on the attempt: an attempt that stalls here or
    # later is abandoned wholesale on the next retry, and releasing its
    # monitors is what keeps that retirement from arriving at the
    # director as a component failure.
    monitors =
      recovery_attempt
      |> extract_pids_to_monitor()
      |> Enum.map(monitor_fn)

    {%{recovery_attempt | component_monitors: monitors}, Bedrock.ControlPlane.Director.Recovery.PersistencePhase}
  end

  # Monitored: sequencer, proxies, resolvers, and the epoch's logs (their
  # pids read from the attempt's transaction_services — the TSL carries no
  # membership map). Materializers are deliberately absent: they
  # self-organize from logs and their failure is not epoch-fatal.
  @spec extract_pids_to_monitor(map()) :: [pid()]
  defp extract_pids_to_monitor(recovery_attempt) do
    resolver_pids =
      Enum.map(recovery_attempt.resolvers, fn {_start_key, pid} -> pid end)

    # Fail fast on a log that is missing or not up: an epoch whose log
    # cannot be monitored is an epoch that cannot detect its own failure.
    log_pids =
      Enum.map(Map.keys(recovery_attempt.logs), fn log_id ->
        %{status: {:up, pid}} = Map.fetch!(recovery_attempt.transaction_services, log_id)
        pid
      end)

    Enum.concat([
      [recovery_attempt.sequencer],
      recovery_attempt.proxies,
      resolver_pids,
      log_pids
    ])
  end
end
