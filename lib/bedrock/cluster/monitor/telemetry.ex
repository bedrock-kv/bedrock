defmodule Bedrock.Cluster.Monitor.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.ClusterController

  @doc """
  Emits a telemetry event indicating that the cluster controller has been
  replaced. The event includes metadata for the cluster and controller.
  """
  @spec trace_controller_replaced(cluster :: module(), controller :: {Bedrock.epoch(), pid()}) ::
          :ok
  def trace_controller_replaced(cluster, controller) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :controller_replaced], %{}, %{
      cluster: cluster,
      controller: controller
    })
  end

  @spec trace_advertising_capabilities(
          cluster :: module(),
          capabilities :: [Cluster.capability()],
          running_services :: ClusterController.running_service_info_by_id()
        ) ::
          :ok
  def trace_advertising_capabilities(cluster, capabilities, running_services) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :advertise_capabilities], %{}, %{
      cluster: cluster,
      capabilities: capabilities,
      running_services: running_services
    })
  end
end
