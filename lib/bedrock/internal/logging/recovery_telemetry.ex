defmodule Bedrock.Internal.Logging.RecoveryTelemetry do
  require Logger

  defp handler_id, do: "bedrock_logging_recovery_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :cluster, :recovery, :started],
        [:bedrock, :cluster, :recovery, :services_locked],
        [:bedrock, :cluster, :recovery, :durable_version_chosen],
        [:bedrock, :cluster, :recovery, :suitable_logs_chosen]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :cluster, :recovery, :started],
        _measurements,
        %{cluster: cluster, epoch: epoch, attempt: attempt} = _metadata,
        _config
      ) do
    if attempt == 0 do
      Logger.info("Bedrock [#{cluster.name()}/#{epoch}]: Recovery started",
        ansi_color: :magenta
      )
    else
      Logger.info("Bedrock [#{cluster.name()}/#{epoch}]: Recovery try ##{attempt} started",
        ansi_color: :magenta
      )
    end
  end

  def log_event(
        [:bedrock, :cluster, :recovery, :services_locked],
        _measurements,
        %{cluster: cluster, epoch: epoch, n_services: n_services, n_reporting: n_reporting} =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}/#{epoch}]: Services #{n_reporting}/#{n_services} reporting",
      ansi_color: :magenta
    )
  end

  def log_event(
        [:bedrock, :cluster, :recovery, :durable_version_chosen],
        _measurements,
        %{
          cluster: cluster,
          epoch: epoch,
          durable_version: durable_version,
          degraded_teams: degraded_teams
        } =
          _metadata,
        _config
      ) do
    if Enum.empty?(degraded_teams) do
      Logger.info(
        "Bedrock [#{cluster.name()}/#{epoch}]: Durable version chosen: #{durable_version}, all teams healthy.",
        ansi_color: :magenta
      )
    else
      Logger.info(
        "Bedrock [#{cluster.name()}/#{epoch}]: Durable version chosen: #{durable_version} (degraded teams: #{degraded_teams |> Enum.join(", ")})",
        ansi_color: :magenta
      )
    end
  end

  def log_event(
        [:bedrock, :cluster, :recovery, :suitable_logs_chosen],
        _measurements,
        %{
          cluster: cluster,
          epoch: epoch,
          suitable_logs: suitable_logs,
          log_version_vector: log_version_vector
        } =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}/#{epoch}]: Suitable logs chosen: #{suitable_logs |> Enum.join(", ")}",
      ansi_color: :magenta
    )

    Logger.info(
      "Bedrock [#{cluster.name()}/#{epoch}]: Version vector: #{inspect(log_version_vector)}",
      ansi_color: :magenta
    )
  end
end
