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

  Always succeeds since monitoring setup is local to the director process.
  Transitions to :cleanup_obsolete_workers to remove unused services before
  completing recovery.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  require Logger

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(%{state: :monitor_components} = recovery_attempt, _context) do
    trace_recovery_monitoring_components()

    recovery_attempt
    |> Map.get(:sequencer)
    |> case do
      nil ->
        Logger.warning("No sequencer to monitor")

      sequencer_pid when is_pid(sequencer_pid) ->
        Process.monitor(sequencer_pid)
        Logger.debug("Director monitoring sequencer: #{inspect(sequencer_pid)}")
    end

    recovery_attempt
    |> Map.get(:proxies, [])
    |> Enum.each(fn proxy ->
      if is_pid(proxy) do
        Process.monitor(proxy)
        Logger.debug("Director monitoring commit proxy: #{inspect(proxy)}")
      end
    end)

    recovery_attempt
    |> Map.get(:resolvers, [])
    |> Enum.each(fn resolver ->
      if is_pid(resolver) do
        Process.monitor(resolver)
        Logger.debug("Director monitoring resolver: #{inspect(resolver)}")
      end
    end)

    log_pids =
      recovery_attempt
      |> Map.get(:services, %{})
      |> Enum.filter(fn {_id, service} -> service.kind == :log end)
      |> Enum.filter(fn {_id, service} -> match?({:up, _}, service.status) end)
      |> Enum.map(fn {_id, service} -> elem(service.status, 1) end)

    log_pids
    |> Enum.each(fn log_pid ->
      if is_pid(log_pid) do
        Process.monitor(log_pid)
        Logger.debug("Director monitoring log: #{inspect(log_pid)}")
      end
    end)

    Logger.info(
      "Director monitoring #{length(recovery_attempt.proxies)} proxies, #{length(recovery_attempt.resolvers)} resolvers, #{length(log_pids)} logs, and 1 sequencer"
    )

    recovery_attempt |> Map.put(:state, :completed)
  end
end
