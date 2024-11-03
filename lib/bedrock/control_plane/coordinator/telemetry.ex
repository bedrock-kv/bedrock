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
  @spec emit_director_changed(State.t(), director :: pid() | :unavailable) ::
          State.t()
  def emit_director_changed(t, director) do
    Telemetry.execute([:bedrock, :cluster, :director, :changed], %{}, %{
      cluster: t.cluster,
      director: director
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
