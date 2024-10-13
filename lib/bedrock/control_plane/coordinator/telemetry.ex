defmodule Bedrock.ControlPlane.Coordinator.Telemetry do
  alias Bedrock.ControlPlane.Coordinator.State

  @doc """
  """
  @spec emit_cluster_leadership_changed(State.t()) :: State.t()
  def emit_cluster_leadership_changed(t) do
    :telemetry.execute([:bedrock, :cluster, :leadership, :changed], %{}, %{
      cluster: t.cluster,
      new_leader: t.leader_node
    })

    t
  end

  @doc """
  """
  @spec emit_cluster_controller_changed(State.t(), controller :: pid()) :: State.t()
  def emit_cluster_controller_changed(t, controller) do
    :telemetry.execute([:bedrock, :cluster, :controller, :changed], %{}, %{
      cluster: t.cluster,
      controller: controller
    })

    t
  end
end
