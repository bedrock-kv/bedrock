defmodule Bedrock.Cluster.Monitor.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Director

  @spec trace_started(cluster :: module()) :: :ok
  def trace_started(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :started], %{}, %{
      cluster: cluster
    })
  end

  @spec trace_advertising_capabilities(
          cluster :: module(),
          capabilities :: [Cluster.capability()],
          running_services :: Director.running_service_info_by_id()
        ) ::
          :ok
  def trace_advertising_capabilities(cluster, capabilities, running_services) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :advertise_capabilities], %{}, %{
      cluster: cluster,
      capabilities: capabilities,
      running_services: running_services
    })
  end

  def trace_searching_for_director(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :searching_for_director], %{}, %{
      cluster: cluster
    })
  end

  def trace_found_director(cluster, director) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :found_director], %{}, %{
      cluster: cluster,
      director: director
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

  def trace_lost_director(cluster) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :lost_director], %{}, %{
      cluster: cluster
    })
  end

  def trace_missed_pong(cluster, n_missed) do
    Telemetry.execute([:bedrock, :cluster, :monitor, :missed_pong], %{missed_pongs: n_missed}, %{
      cluster: cluster
    })
  end
end
