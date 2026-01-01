defmodule Bedrock.Cluster.Link.Tracing do
  @moduledoc false

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_link"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :link, :started],
        [:bedrock, :cluster, :link, :advertise_capabilities],
        [:bedrock, :cluster, :link, :searching_for_coordinator],
        [:bedrock, :cluster, :link, :found_coordinator]
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
  def handler([:bedrock, :cluster, :link, event], measurements, metadata, _), do: trace(event, measurements, metadata)

  @spec trace(atom(), map(), map()) :: :ok
  def trace(:started, _, %{cluster: cluster}) do
    Logger.metadata(cluster: cluster)

    info("Link started")
  end

  def trace(:advertise_capabilities, _, %{capabilities: capabilities, running_services: running_services}) do
    info("Advertising capabilities (#{Enum.join(capabilities, ", ")}): #{inspect(running_services, pretty: true)}")
  end

  def trace(:searching_for_coordinator, _, _), do: info("Searching for a coordinator")

  def trace(:found_coordinator, _, %{coordinator: coordinator}), do: info("Found coordinator: #{inspect(coordinator)}")

  def trace(:missed_pong, %{missed_pongs: missed_pongs}, _) when missed_pongs > 1,
    do: info("Missed #{inspect(missed_pongs)} pongs from coordinator")

  def trace(:missed_pong, %{missed_pongs: missed_pongs}, _),
    do: info("Missed #{inspect(missed_pongs)} pong from coordinator")

  @spec info(message :: String.t()) :: :ok
  def info(message) do
    cluster = Keyword.fetch!(Logger.metadata(), :cluster)
    Logger.info("Bedrock [#{cluster.name()}]: #{message}")
  end
end
