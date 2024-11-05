defmodule Bedrock.ControlPlane.Director.Recovery.Tracing do
  @moduledoc false

  require Logger

  defp handler_id, do: "bedrock_trace_director_recovery"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :recovery, :started],
        [:bedrock, :recovery, :services_locked],
        [:bedrock, :recovery, :durable_version_chosen],
        [:bedrock, :recovery, :suitable_logs_chosen],
        [:bedrock, :recovery, :storage_unlocking]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def handler([:bedrock, :recovery, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def trace(:started, _, %{cluster: cluster, epoch: epoch, attempt: attempt}) do
    Logger.metadata(cluster: cluster, epoch: epoch, attempt: attempt)

    info("Recovery attempt ##{attempt} started")
  end

  def trace(:services_locked, %{n_services: n_services, n_reporting: n_reporting}, _),
    do: info("Services #{n_reporting}/#{n_services} reporting")

  def trace(:durable_version_chosen, _, %{degraded_teams: [], durable_version: durable_version}),
    do: info("Durable version chosen: #{durable_version}, all teams healthy.")

  def trace(:durable_version_chosen, _, %{
        degraded_teams: degraded_teams,
        durable_version: durable_version
      }) do
    formatted_degraded_teams = degraded_teams |> Enum.join(", ")

    info(
      "Durable version chosen: #{durable_version} (degraded teams: #{formatted_degraded_teams})"
    )
  end

  def trace(:suitable_logs_chosen, _, %{
        suitable_logs: suitable_logs,
        log_version_vector: log_version_vector
      }) do
    info("Suitable logs chosen: #{suitable_logs |> Enum.join(", ")}")
    info("Version vector: #{inspect(log_version_vector)}")
  end

  def trace(:storage_unlocking, _, %{storage_worker_id: storage_worker_id}),
    do: info("Storage worker #{storage_worker_id} unlocking")

  defp info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}/#{metadata[:epoch]}]: #{message}",
      ansi_color: :magenta
    )
  end
end
