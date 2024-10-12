defmodule Bedrock.Cluster.Monitor.Telemetry do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.Telemetry

  @doc """
  Emits a telemetry event indicating that the cluster controller has been
  replaced. The event includes metadata for the cluster and controller.
  """
  @spec emit_cluster_controller_replaced(t :: State.t()) :: State.t()
  def emit_cluster_controller_replaced(t) do
    Telemetry.emit([:bedrock, :cluster, :controller_replaced], %{}, %{
      cluster: t.cluster,
      controller: t.controller
    })

    t
  end
end
