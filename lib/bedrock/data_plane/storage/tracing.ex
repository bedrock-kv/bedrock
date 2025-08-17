defmodule Bedrock.DataPlane.Storage.Tracing do
  @moduledoc false

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_storage"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :storage, :pull_start],
        [:bedrock, :storage, :pull_succeeded],
        [:bedrock, :storage, :pull_failed],
        [:bedrock, :storage, :log_marked_as_failed],
        [:bedrock, :storage, :log_pull_circuit_breaker_tripped],
        [:bedrock, :storage, :log_pull_circuit_breaker_reset]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :storage, event], measurements, metadata, _), do: log_event(event, measurements, metadata)

  @spec log_event(atom(), map(), map()) :: :ok
  def log_event(:pull_start, _, %{timestamp: timestamp, next_version: next_version}),
    do: debug("Log pull started at #{timestamp} for version #{Bedrock.DataPlane.Version.to_string(next_version)}")

  def log_event(:pull_succeeded, _, %{timestamp: timestamp, n_transactions: n_transactions}),
    do: debug("Log pull succeeded at #{timestamp} with #{n_transactions} transactions")

  def log_event(:pull_failed, _, %{timestamp: timestamp, reason: reason}),
    do: warn("Log pull failed at #{timestamp}: #{inspect(reason)}")

  def log_event(:log_marked_as_failed, _, %{timestamp: timestamp, log_id: log_id}),
    do: warn("Log #{log_id} marked as failed at #{timestamp}")

  def log_event(:log_pull_circuit_breaker_tripped, _, %{timestamp: timestamp, ms_to_wait: ms_to_wait}),
    do: warn("Log pull circuit breaker tripped at #{timestamp}, waiting #{ms_to_wait}ms")

  def log_event(:log_pull_circuit_breaker_reset, _, %{timestamp: timestamp}),
    do: info("Log pull circuit breaker reset at #{timestamp}")

  @spec debug(String.t()) :: :ok
  defp debug(message) do
    Logger.debug("Bedrock Storage: #{message}", ansi_color: :cyan)
  end

  @spec info(String.t()) :: :ok
  defp info(message) do
    Logger.info("Bedrock Storage: #{message}", ansi_color: :cyan)
  end

  @spec warn(String.t()) :: :ok
  defp warn(message) do
    Logger.warning("Bedrock Storage: #{message}", ansi_color: :yellow)
  end
end
