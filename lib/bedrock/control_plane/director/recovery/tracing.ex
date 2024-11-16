defmodule Bedrock.ControlPlane.Director.Recovery.Tracing do
  @moduledoc false

  import Bedrock.Internal.Time.Interval, only: [humanize: 1]

  require Logger

  defp handler_id, do: "bedrock_trace_director_recovery"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :recovery, :started],
        [:bedrock, :recovery, :stalled],
        [:bedrock, :recovery, :completed],
        [:bedrock, :recovery, :services_locked],
        [:bedrock, :recovery, :first_time_initialization],
        [:bedrock, :recovery, :creating_vacancies],
        [:bedrock, :recovery, :durable_version_chosen],
        [:bedrock, :recovery, :team_health],
        [:bedrock, :recovery, :suitable_logs_chosen],
        [:bedrock, :recovery, :all_log_vacancies_filled],
        [:bedrock, :recovery, :all_storage_team_vacancies_filled],
        [:bedrock, :recovery, :replaying_old_logs],
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

  def trace(:stalled, _, %{elapsed: elapsed, reason: reason}),
    do: error("Recovery stalled after #{humanize(elapsed)}: #{inspect(reason)}")

  def trace(:completed, _, %{elapsed: elapsed}),
    do: info("Recovery completed in #{humanize(elapsed)}!")

  def trace(:services_locked, %{n_services: n_services, n_reporting: n_reporting}, _),
    do: info("Services #{n_reporting}/#{n_services} reporting")

  def trace(:first_time_initialization, _, _), do: info("Initializing a brand new system")

  def trace(:creating_vacancies, measurements, _) do
    case {measurements[:n_log_vacancies], measurements[:n_storage_team_vacancies]} do
      {0, 0} ->
        info("No vacancies to create")

      {0, n_storage_team_vacancies} ->
        info("Creating #{n_storage_team_vacancies} storage team vacancies")

      {n_log_vacancies, 0} ->
        info("Creating #{n_log_vacancies} log vacancies")

      {n_log_vacancies, n_storage_team_vacancies} ->
        info(
          "Creating #{n_log_vacancies} log vacancies and #{n_storage_team_vacancies} storage team vacancies"
        )
    end
  end

  def trace(:durable_version_chosen, _, %{durable_version: durable_version}),
    do: info("Durable version chosen: #{durable_version}")

  def trace(:team_health, _, metadata) do
    case {metadata[:healthy_teams], metadata[:degraded_teams]} do
      {[], []} ->
        info("No teams available")

      {healthy, []} ->
        info("All teams healthy (#{healthy |> Enum.sort() |> Enum.join(", ")})")

      {[], degraded} ->
        info("All teams degraded (#{degraded |> Enum.sort() |> Enum.join(", ")})")

      {healthy, degraded} ->
        info(
          "Healthy teams are #{healthy |> Enum.join(", ")}, with some teams degraded (#{degraded |> Enum.join(", ")})"
        )
    end
  end

  def trace(:all_log_vacancies_filled, _, _),
    do: info("All log vacancies filled")

  def trace(:all_storage_team_vacancies_filled, _, _),
    do: info("All storage team vacancies filled")

  def trace(:replaying_old_logs, _, %{
        old_log_ids: old_log_ids,
        new_log_ids: new_log_ids,
        version_vector: version_vector
      }) do
    info("Version vector chosen: #{inspect(version_vector)}")

    case old_log_ids do
      [] ->
        info("No logs to replay")

      _ ->
        info(
          "Replaying logs: {#{old_log_ids |> Enum.join(", ")}} -> {#{new_log_ids |> Enum.join(", ")}}"
        )
    end
  end

  def trace(:suitable_logs_chosen, _, %{
        suitable_logs: suitable_logs,
        log_version_vector: log_version_vector
      }) do
    info("Suitable logs chosen for copying: #{suitable_logs |> Enum.join(", ")}")
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

  defp error(message) do
    metadata = Logger.metadata()

    Logger.error("Bedrock [#{metadata[:cluster].name()}/#{metadata[:epoch]}]: #{message}",
      ansi_color: :red
    )
  end
end
