defmodule Bedrock.Internal.Logging.MonitorTelemetry do
  require Logger

  defp handler_id, do: "bedrock_logging_monitor_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :monitor, :controller_replaced],
        [:bedrock, :cluster, :monitor, :advertise_capabilities]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :cluster, :monitor, :controller_replaced],
        _measurements,
        %{cluster: cluster, controller: controller} = _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Controller changed to: #{inspect(controller)}.",
      ansi_color: :green
    )
  end

  def log_event(
        [:bedrock, :cluster, :monitor, :advertise_capabilities],
        _measurements,
        %{cluster: cluster, capabilities: capabilities, running_services: running_services} =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Advertising to controller (#{capabilities |> Enum.join(", ")}): #{inspect(running_services)}",
      ansi_color: :green
    )
  end
end
