defmodule Bedrock.Cluster.Monitor.Tracing do
  require Logger

  defp handler_id, do: "bedrock_trace_monitor"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :monitor, :started],
        [:bedrock, :cluster, :monitor, :advertise_capabilities],
        [:bedrock, :cluster, :monitor, :searching_for_director],
        [:bedrock, :cluster, :monitor, :found_director],
        [:bedrock, :cluster, :monitor, :lost_director],
        [:bedrock, :cluster, :monitor, :searching_for_coordinator],
        [:bedrock, :cluster, :monitor, :found_coordinator]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def handler([:bedrock, :cluster, :monitor, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def trace(:started, _, %{cluster: cluster}) do
    Logger.metadata(cluster: cluster)

    info("Monitor started")
  end

  def trace(:advertise_capabilities, _, %{
        capabilities: capabilities,
        running_services: running_services
      }) do
    info(
      "Advertising to director (#{capabilities |> Enum.join(", ")}): #{inspect(running_services, pretty: true)}"
    )
  end

  def trace(:searching_for_director, _, _),
    do: info("Searching for a director")

  def trace(:found_director, _, %{director: director}),
    do: info("Found director: #{inspect(director)}")

  def trace(:lost_director, _, _),
    do: info("Lost director")

  def trace(:searching_for_coordinator, _, _),
    do: info("Searching for a coordinator")

  def trace(:found_coordinator, _, %{coordinator: coordinator}),
    do: info("Found coordinator: #{inspect(coordinator)}")

  def trace(:missed_pong, %{missed_pongs: missed_pongs}, _) when missed_pongs > 1,
    do: info("Missed #{inspect(missed_pongs)} pongs from director")

  def trace(:missed_pong, %{missed_pongs: missed_pongs}, _),
    do: info("Missed #{inspect(missed_pongs)} pong from director")

  def info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}]: #{message}")
  end
end
