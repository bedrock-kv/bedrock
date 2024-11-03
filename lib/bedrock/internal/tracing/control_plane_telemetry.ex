defmodule Bedrock.Internal.Tracing.ControlPlaneTelemetry do
  require Logger

  defp handler_id, do: "bedrock_trace_controlplane_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :controller, :changed],
        [:bedrock, :cluster, :leadership, :changed]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :cluster, :leadership, :changed],
        _measurements,
        %{cluster: cluster, new_leader: leader} = _metadata,
        _config
      ) do
    if leader == :undecided do
      Logger.info("Bedrock [#{cluster.name()}]: There is no leader",
        ansi_color: :red
      )
    else
      Logger.info(
        "Bedrock [#{cluster.name()}]: #{inspect(leader)} was elected as the cluster leader",
        ansi_color: :green
      )
    end
  end

  def log_event(
        [:bedrock, :cluster, :controller, :changed],
        _measurements,
        %{cluster: cluster, controller: controller} = _metadata,
        _config
      ) do
    if controller == :unavailable do
      Logger.info("Bedrock [#{cluster.name()}]: A quorum of coordinators is not present",
        ansi_color: :red
      )
    else
      Logger.info("Bedrock [#{cluster.name()}]: Controller changed to #{inspect(controller)}",
        ansi_color: :green
      )
    end
  end
end
