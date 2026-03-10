defmodule Bedrock.ControlPlane.Director.Recovery.Tracing do
  @moduledoc false

  import Bedrock.Internal.Time.Interval, only: [humanize: 1]

  alias Bedrock.DataPlane.Version

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_director_recovery"

  @spec start() :: :ok | {:error, :already_exists}
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
        [:bedrock, :recovery, :suitable_logs_chosen],
        [:bedrock, :recovery, :all_log_vacancies_filled],
        [:bedrock, :recovery, :replaying_old_logs],
        [:bedrock, :recovery, :log_recruitment_completed],
        [:bedrock, :recovery, :log_validation_started],
        [:bedrock, :recovery, :log_service_status],
        [:bedrock, :recovery, :attempt_persisted],
        [:bedrock, :recovery, :attempt_persist_failed],
        [:bedrock, :recovery, :layout_persisted],
        [:bedrock, :recovery, :layout_persist_failed],
        [:bedrock, :recovery, :tsl_validation_success],
        [:bedrock, :recovery, :tsl_validation_failed],
        [:bedrock, :recovery, :unexpected_state]
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
  def handler([:bedrock, :recovery, event], measurements, metadata, _), do: trace(event, measurements, metadata)

  @spec trace(atom(), map(), map()) :: :ok
  def trace(:started, _, %{cluster: cluster, epoch: epoch, attempt: attempt}) do
    Logger.metadata(cluster: cluster, epoch: epoch, attempt: attempt)

    info("Recovery attempt ##{attempt} started")
  end

  def trace(:stalled, _, %{elapsed: elapsed, reason: reason}),
    do: error("Recovery stalled after #{humanize(elapsed)}: #{inspect(reason)}")

  def trace(:completed, _, %{elapsed: elapsed}), do: info("Recovery completed in #{humanize(elapsed)}!")

  def trace(:services_locked, %{n_services: n_services, n_reporting: n_reporting}, _),
    do: info("Services #{n_reporting}/#{n_services} reporting")

  def trace(:first_time_initialization, _, _), do: info("Initializing a brand new system")

  def trace(:creating_vacancies, measurements, _) do
    case measurements[:n_log_vacancies] do
      0 -> info("No vacancies to create")
      n_log_vacancies -> info("Creating #{n_log_vacancies} log vacancies")
    end
  end

  def trace(:durable_version_chosen, _, %{durable_version: durable_version}),
    do: info("Durable version chosen: #{Version.to_string(durable_version)}")

  def trace(:all_log_vacancies_filled, _, _), do: info("All log vacancies filled")

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
        info("Replaying logs: {#{Enum.join(old_log_ids, ", ")}} -> {#{Enum.join(new_log_ids, ", ")}}")
    end
  end

  def trace(:suitable_logs_chosen, _, %{suitable_logs: suitable_logs, log_version_vector: log_version_vector}) do
    info("Suitable logs chosen for copying: #{Enum.map_join(suitable_logs, ", ", &inspect/1)}")

    info("Version vector: #{inspect(log_version_vector)}")
  end

  def trace(:log_recruitment_completed, _, %{
        log_ids: log_ids,
        service_pids: service_pids,
        available_services: available_services,
        updated_services: updated_services
      }) do
    info("Log recruitment completed: #{length(log_ids)} logs created")

    debug("""
      Created logs: #{inspect(log_ids)}
      Service PIDs: #{inspect(Map.keys(service_pids))}
      Available services: #{inspect(Map.keys(available_services))}
      Updated services: #{inspect(Map.keys(updated_services))}
    """)
  end

  def trace(:log_validation_started, _, %{log_ids: log_ids, available_services: available_services}) do
    debug("""
    Starting log validation: #{length(log_ids)} logs to validate
      Log IDs: #{inspect(log_ids)}
      Available services: #{inspect(Map.keys(available_services))}
    """)
  end

  def trace(:log_service_status, _, %{log_id: log_id, status: status, service: service}) do
    case status do
      :found ->
        debug("  Log #{inspect(log_id)}: #{inspect(service.status)})")

      :missing ->
        debug("  Log #{inspect(log_id)}: NO MATCHING SERVICE (found: #{inspect(service)})")
    end
  end

  def trace(:attempt_persisted, _, %{txn_id: txn_id}),
    do: info("Recovery attempt persisted with txn ID: #{inspect(txn_id)}")

  def trace(:attempt_persist_failed, _, %{reason: reason}),
    do: error("Failed to persist recovery attempt: #{inspect(reason)}")

  def trace(:layout_persisted, _, %{txn_id: txn_id}),
    do: info("New transaction system layout persisted with txn ID: #{inspect(txn_id)}")

  def trace(:layout_persist_failed, _, %{reason: reason}),
    do: error("Failed to persist new transaction system layout: #{inspect(reason)}")

  def trace(:tsl_validation_success, _, _), do: info("TSL type safety validation passed")

  def trace(:tsl_validation_failed, _, %{transaction_system_layout: tsl, validation_error: validation_error}) do
    error("""
    TSL type safety validation failed during recovery - this indicates data corruption.

    Validation Error: #{inspect(validation_error, limit: :infinity)}

    TSL Components being validated:
      - Logs: #{inspect(Map.get(tsl, :logs), limit: 10)}
      - Resolvers: #{inspect(Map.get(tsl, :resolvers), limit: 5)}

    This indicates the TSL data was corrupted, likely due to improper integer-to-binary
    version conversion. Manual intervention may be required to fix the underlying data.
    """)
  end

  def trace(:unexpected_state, _, %{unexpected_state: unexpected_state, full_state: full_state}) do
    error("Recovery attempt in unexpected state: #{inspect(unexpected_state)}")
    debug("Full recovery state: #{inspect(full_state)}")
  end

  @spec info(String.t()) :: :ok
  defp info(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)
    epoch = Keyword.fetch!(metadata, :epoch)

    Logger.info("Bedrock [#{cluster.name()}/#{epoch}]: #{message}",
      ansi_color: :magenta
    )
  end

  @spec error(String.t()) :: :ok
  defp error(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)
    epoch = Keyword.fetch!(metadata, :epoch)

    Logger.error("Bedrock [#{cluster.name()}/#{epoch}]: #{message}",
      ansi_color: :red
    )
  end

  @spec debug(String.t()) :: :ok
  defp debug(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)
    epoch = Keyword.fetch!(metadata, :epoch)

    Logger.debug("Bedrock [#{cluster.name()}/#{epoch}]: #{message}",
      ansi_color: :cyan
    )
  end
end
