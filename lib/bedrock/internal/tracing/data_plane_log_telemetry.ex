defmodule Bedrock.Internal.Tracing.DataPlaneLogTelemetry do
  require Logger

  defp handler_id, do: "bedrock_trace_data_plane_log_telemetry"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :data_plane, :log, :started],
        [:bedrock, :data_plane, :log, :lock_for_recovery],
        [:bedrock, :data_plane, :log, :recover_from],
        [:bedrock, :data_plane, :log, :push],
        [:bedrock, :data_plane, :log, :pull]
      ],
      &__MODULE__.log_event/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def log_event(
        [:bedrock, :data_plane, :log, :started],
        _measurements,
        %{cluster: cluster, id: id, otp_name: otp_name} =
          _metadata,
        _config
      ) do
    Logger.info("Bedrock [#{cluster.name()}]: Log #{id} started with OTP name #{otp_name}")
  end

  def log_event(
        [:bedrock, :data_plane, :log, :lock_for_recovery],
        _measurements,
        %{cluster: cluster, id: id, epoch: epoch} =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Log #{id} attempting to lock for recovery in epoch #{epoch}"
    )
  end

  def log_event(
        [:bedrock, :data_plane, :log, :recover_from],
        _measurements,
        %{
          cluster: cluster,
          id: id,
          source_log: nil,
          first_version: :undefined,
          last_version: last_version = 0
        } =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Log #{id} attempting to reset back to version #{last_version}"
    )
  end

  def log_event(
        [:bedrock, :data_plane, :log, :recover_from],
        _measurements,
        %{
          cluster: cluster,
          id: id,
          source_log: source_log,
          first_version: first_version,
          last_version: last_version
        } =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Log #{id} attempting to recover from #{inspect(source_log)} with versions #{first_version} to #{last_version}"
    )
  end

  def log_event(
        [:bedrock, :data_plane, :log, :push],
        %{n_keys: n_keys},
        %{
          cluster: cluster,
          id: id,
          expected_version: expected_version,
          transaction: transaction
        } = _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Log #{id} attempting to push transaction (#{n_keys} keys) with expected version #{inspect(expected_version)}",
      transaction: transaction
    )
  end

  def log_event(
        [:bedrock, :data_plane, :log, :pull],
        _measurements,
        %{cluster: cluster, id: id, from_version: from_version, opts: opts} =
          _metadata,
        _config
      ) do
    Logger.info(
      "Bedrock [#{cluster.name()}]: Log #{id} attempting to pull transactions from version #{from_version} with options #{inspect(opts)}"
    )
  end
end
