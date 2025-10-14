defmodule Bedrock.Cluster.CoordinatorClient.Telemetry do
  @moduledoc false
  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Telemetry

  @spec trace_started(cluster :: module()) :: :ok
  def trace_started(cluster) do
    Telemetry.execute([:bedrock, :cluster, :gateway, :started], %{}, %{
      cluster: cluster
    })
  end

  @spec trace_advertising_capabilities(
          cluster :: module(),
          capabilities :: [Cluster.capability()],
          running_services :: map()
        ) ::
          :ok
  def trace_advertising_capabilities(cluster, capabilities, running_services) do
    Telemetry.execute([:bedrock, :cluster, :gateway, :advertise_capabilities], %{}, %{
      cluster: cluster,
      capabilities: capabilities,
      running_services: running_services
    })
  end

  @spec trace_searching_for_coordinator(cluster :: module()) :: :ok
  def trace_searching_for_coordinator(cluster) do
    Telemetry.execute([:bedrock, :cluster, :gateway, :searching_for_coordinator], %{}, %{
      cluster: cluster
    })
  end

  @spec trace_found_coordinator(
          cluster :: module(),
          coordinator :: Coordinator.ref()
        ) :: :ok
  def trace_found_coordinator(cluster, coordinator) do
    Telemetry.execute([:bedrock, :cluster, :gateway, :found_coordinator], %{}, %{
      cluster: cluster,
      coordinator: coordinator
    })
  end

  @spec trace_missed_pong(cluster :: module(), n_missed :: non_neg_integer()) :: :ok
  def trace_missed_pong(cluster, n_missed) do
    Telemetry.execute([:bedrock, :cluster, :gateway, :missed_pong], %{missed_pongs: n_missed}, %{
      cluster: cluster
    })
  end
end
