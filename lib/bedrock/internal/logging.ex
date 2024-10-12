defmodule Bedrock.Internal.Logging do
  require Logger

  def start do
    :telemetry.attach_many(
      "bedrock_logging",
      [
        [:bedrock, :cluster, :controller, :changed],
        [:bedrock, :cluster, :leadership, :changed]
      ],
      &__MODULE__.handle_cluster_controller_replaced/4,
      nil
    )
  end

  def handle_cluster_controller_replaced(
        [:bedrock, :cluster, :leadership, :changed],
        _measurements,
        %{cluster: cluster, new_leader: leader} = _metadata,
        _config
      ) do
    if leader == :undecided do
      Logger.info("Bedrock [#{cluster.name()}]: A quorum has not been reached to elect a leader")
    else
      Logger.info(
        "Bedrock [#{cluster.name()}]: #{inspect(leader)} was elected as the cluster leader"
      )
    end
  end

  def handle_cluster_controller_replaced(
        [:bedrock, :cluster, :controller, :changed],
        _measurements,
        %{cluster: cluster, controller: controller} = _metadata,
        _config
      ) do
    if controller == :unavailable do
      Logger.info("Bedrock [#{cluster.name()}]: Controller is unavailable")
    else
      Logger.info("Bedrock [#{cluster.name()}]: Controller changed to #{inspect(controller)}")
    end
  end
end
