defmodule Bedrock.ControlPlane.Coordinator.Tracing do
  require Logger

  defp handler_id, do: "bedrock_trace_controlplane_coordinator"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :control_plane, :coordinator, :started],
        [:bedrock, :control_plane, :coordinator, :election_completed],
        [:bedrock, :control_plane, :coordinator, :director_changed],
        [:bedrock, :control_plane, :coordinator, :consensus_reached]
      ],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def handle_event([:bedrock, :control_plane, :coordinator, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def trace(:started, _, %{cluster: cluster}) do
    Logger.metadata(cluster: cluster)
    info("Coordinator started")
  end

  def trace(:election_completed, _, %{new_leader: :undecided}),
    do: info("There is no leader")

  def trace(:election_completed, _, %{new_leader: leader}),
    do: info("#{inspect(leader)} was elected as the cluster leader")

  def trace(:director_changed, _, %{director: :unavailable}),
    do: info("A quorum of coordinators is not present")

  def trace(:director_changed, _, %{director: director}),
    do: info("Director changed to #{inspect(director)}")

  def trace(:consensus_reached, _, %{transaction_id: tx_id}),
    do: info("Consensus reached at #{inspect(tx_id)}")

  def info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}]: #{message}",
      ansi_color: :green
    )
  end
end
