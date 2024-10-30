defmodule Bedrock.ControlPlane.Coordinator.Telemetry do
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Telemetry

  @doc """
  """
  @spec emit_cluster_leadership_changed(State.t()) :: State.t()
  def emit_cluster_leadership_changed(t) do
    Telemetry.execute([:bedrock, :cluster, :leadership, :changed], %{}, %{
      cluster: t.cluster,
      new_leader: t.leader_node
    })

    t
  end

  @doc """
  """
  @spec emit_cluster_controller_changed(State.t(), controller :: pid() | :unavailable) ::
          State.t()
  def emit_cluster_controller_changed(t, controller) do
    Telemetry.execute([:bedrock, :cluster, :controller, :changed], %{}, %{
      cluster: t.cluster,
      controller: controller
    })

    t
  end

  def trace_consensus_reached(t, transaction_id) do
    Telemetry.execute([:bedrock, :cluster, :coordination, :consensus_reached], %{}, %{
      cluster: t.cluster,
      transaction_id: transaction_id
    })

    t
  end
end
