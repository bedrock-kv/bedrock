defmodule Bedrock.Cluster.Monitor.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.ClusterController

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

  def trace_searching_for_controller(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :searching_for_controller], %{}, %{
      cluster: cluster
    })
  end

  def trace_found_controller(cluster, controller) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :found_controller], %{}, %{
      cluster: cluster,
      controller: controller
    })
  end

  def trace_searching_for_coordinator(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :searching_for_coordinator], %{}, %{
      cluster: cluster
    })
  end

  def trace_found_coordinator(cluster, coordinator) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :found_coordinator], %{}, %{
      cluster: cluster,
      coordinator: coordinator
    })
  end

  def trace_lost_controller(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :lost_controller], %{}, %{
      cluster: cluster
    })
  end

  def trace_missed_pong(cluster, n_missed) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :missed_pong], %{missed_pongs: n_missed}, %{
      cluster: cluster
    })
  end
end
