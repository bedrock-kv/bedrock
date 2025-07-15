defmodule Bedrock.ControlPlane.Coordinator.Tracing do
  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_controlplane_coordinator"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :control_plane, :coordinator, :started],
        [:bedrock, :control_plane, :coordinator, :election_completed],
        [:bedrock, :control_plane, :coordinator, :director_changed],
        [:bedrock, :control_plane, :coordinator, :consensus_reached]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          any()
        ) :: any()
  def handler([:bedrock, :control_plane, :coordinator, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  @spec trace(atom(), map(), map()) :: any()
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

  @spec info(message :: String.t()) :: :ok
  def info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}]: #{message}",
      ansi_color: :green
    )
  end
end
