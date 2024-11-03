defmodule Bedrock.Internal.Tracing.MonitorTelemetry do
  require Logger

  defp handler_id, do: "bedrock_trace_monitor_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :monitor, :advertise_capabilities],
        [:bedrock, :cluster, :monitor, :searching_for_director],
        [:bedrock, :cluster, :monitor, :found_director],
        [:bedrock, :cluster, :monitor, :lost_director],
        [:bedrock, :cluster, :monitor, :searching_for_coordinator],
        [:bedrock, :cluster, :monitor, :found_coordinator]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :cluster, :monitor, :advertise_capabilities],
        _measurements,
        %{cluster: cluster, capabilities: capabilities, running_services: running_services} =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Advertising to director (#{capabilities |> Enum.join(", ")}): #{inspect(running_services)}"
    )
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :searching_for_director],
        _measurements,
        %{cluster: cluster} = _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Searching for a director")
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :found_director],
        _measurements,
        %{cluster: cluster, director: director} = _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Found director: #{inspect(director)}")
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :lost_director],
        _measurements,
        %{cluster: cluster} = _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Lost director")
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :searching_for_coordinator],
        _measurements,
        %{cluster: cluster} =
          _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Searching for a coordinator")
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :found_coordinator],
        _measurements,
        %{cluster: cluster, coordinator: coordinator} = _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Found coordinator: #{inspect(coordinator)}")
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :missed_pong],
        %{missed_pongs: missed_pongs},
        %{cluster: cluster} = _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Missed #{inspect(missed_pongs)} pong#{if(missed_pongs > 1, do: "s", else: "")} from director"
    )
  end
end
