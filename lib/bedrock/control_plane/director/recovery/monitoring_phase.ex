defmodule Bedrock.ControlPlane.Director.Recovery.MonitoringPhase do
  @moduledoc """
  Handles the :monitor_components phase of recovery.

  This phase is responsible for setting up monitoring of all
  transaction system components and marking recovery as complete.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  require Logger

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the monitoring phase of recovery.

  Sets up process monitoring for all transaction system components
  and marks the recovery as completed.
  """

  @impl true
  def execute(%{state: :monitor_components} = recovery_attempt, _context) do
    trace_recovery_monitoring_components()

    # Monitor sequencer
    recovery_attempt
    |> Map.get(:sequencer)
    |> case do
      nil ->
        Logger.warning("No sequencer to monitor")

      sequencer_pid when is_pid(sequencer_pid) ->
        Process.monitor(sequencer_pid)
        Logger.debug("Director monitoring sequencer: #{inspect(sequencer_pid)}")
    end

    # Monitor commit proxies
    recovery_attempt
    |> Map.get(:proxies, [])
    |> Enum.each(fn proxy ->
      if is_pid(proxy) do
        Process.monitor(proxy)
        Logger.debug("Director monitoring commit proxy: #{inspect(proxy)}")
      end
    end)

    # Monitor resolvers
    recovery_attempt
    |> Map.get(:resolvers, [])
    |> Enum.each(fn resolver ->
      if is_pid(resolver) do
        Process.monitor(resolver)
        Logger.debug("Director monitoring resolver: #{inspect(resolver)}")
      end
    end)

    # Monitor logs (get PIDs from services)
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
