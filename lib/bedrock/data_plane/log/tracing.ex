defmodule Bedrock.DataPlane.Log.Tracing do
  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_log"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :log, :started],
        [:bedrock, :log, :lock_for_recovery],
        [:bedrock, :log, :recover_from],
        [:bedrock, :log, :push],
        [:bedrock, :log, :pull]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :log, event], measurements, metadata, _),
    do: log_event(event, measurements, metadata)

  @spec log_event(atom(), map(), map()) :: :ok
  def log_event(:started, _, %{cluster: cluster, id: id, otp_name: otp_name}) do
    Logger.metadata(
      id: id,
      cluster: cluster,
      otp_name: otp_name
    )

    info("Started log service: #{otp_name}")
  end

  def log_event(:lock_for_recovery, _, %{epoch: epoch}),
    do: info("Lock for recovery in epoch #{epoch}")

  def log_event(:recover_from, _, %{source_log: :none}),
    do: info("Reset to initial version")

  def log_event(:recover_from, _, %{
        source_log: source_log,
        first_version: first_version,
        last_version: last_version
      }) do
    info("Recover from #{inspect(source_log)} with versions #{first_version} to #{last_version}")
  end

  def log_event(:push, %{n_keys: n_keys}, %{expected_version: expected_version}) do
    info("Push transaction (#{n_keys} keys) with expected version #{inspect(expected_version)}")
  end

  def log_event(:pull, _, %{from_version: from_version, opts: opts}),
    do: info("Pull transactions from version #{from_version} with options #{inspect(opts)}")

  defp info(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)
    id = Keyword.fetch!(metadata, :id)
    Logger.info("Bedrock [#{cluster.name()}/#{id}]: #{message}")
  end
end
