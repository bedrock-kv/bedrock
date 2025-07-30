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

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  require Logger

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_monitoring_components()

    monitor_fn = Map.get(context, :monitor_fn, &Process.monitor/1)

    recovery_attempt.transaction_system_layout
    |> extract_pids_to_monitor()
    |> monitor_all_pids(monitor_fn)

    {recovery_attempt, :completed}
  end

  @spec extract_pids_to_monitor(map()) :: [pid()]
  defp extract_pids_to_monitor(layout) do
    resolver_pids =
      layout.resolvers
      |> Enum.map(fn {_start_key, pid} -> pid end)

    service_pids =
      layout.services
      |> Enum.filter(fn {_service_id, service} -> service.kind != :storage end)
      |> Enum.map(fn {_service_id, %{status: {:up, pid}}} -> pid end)

    Enum.concat([
      [layout.sequencer],
      layout.proxies,
      resolver_pids,
      service_pids
    ])
  end

  @spec monitor_all_pids([pid()], (pid() -> reference())) :: [pid()]
  defp monitor_all_pids(pids, monitor_fn) do
    Enum.each(pids, monitor_fn)
    pids
  end
end
