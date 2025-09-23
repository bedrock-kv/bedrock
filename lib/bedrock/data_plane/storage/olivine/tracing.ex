defmodule Bedrock.DataPlane.Storage.Olivine.Tracing do
  @moduledoc false

  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.Time

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_storage_olivine"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :storage, :transactions_queued],
        [:bedrock, :storage, :batch_processing_start],
        [:bedrock, :storage, :batch_processing_complete],
        [:bedrock, :storage, :transaction_timeout_scheduled],
        [:bedrock, :storage, :read_request_waitlisted],
        [:bedrock, :storage, :read_task_spawned],
        [:bedrock, :storage, :read_task_complete],
        [:bedrock, :storage, :window_advancement_no_eviction],
        [:bedrock, :storage, :window_advancement_evicting],
        [:bedrock, :storage, :window_advancement_complete],
        [:bedrock, :storage, :dets_tx_build_complete],
        [:bedrock, :storage, :dets_insert_complete],
        [:bedrock, :storage, :dets_sync_complete],
        [:bedrock, :storage, :dets_cleanup_complete]
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
  def log_event(:transactions_queued, measurements, metadata) do
    info(
      "#{metadata.otp_name}: Queued #{measurements.transaction_count} transactions (queue size: #{measurements.queue_size})"
    )
  end

  def log_event(:batch_processing_start, measurements, metadata) do
    batch_size_bytes = Map.get(measurements, :batch_size_bytes, 0)

    debug(
      "#{metadata.otp_name}: Starting batch processing (batch size: #{measurements.batch_size}, #{format_bytes(batch_size_bytes)})"
    )
  end

  def log_event(:batch_processing_complete, measurements, metadata) do
    duration_ms = measurements.duration_us / 1000
    batch_size_bytes = Map.get(measurements, :batch_size_bytes, 0)

    info(
      "#{metadata.otp_name}: Completed batch processing (batch size: #{measurements.batch_size}, #{format_bytes(batch_size_bytes)}, duration: #{Float.round(duration_ms, 2)}ms)"
    )
  end

  def log_event(:transaction_timeout_scheduled, _measurements, metadata) do
    debug("#{metadata.otp_name}: Scheduled timeout for transaction processing")
  end

  def log_event(:read_request_waitlisted, _measurements, metadata) do
    key_str = format_key(metadata.key)
    info("#{metadata.otp_name}: Read request waitlisted (operation: #{metadata.operation}, key: #{key_str})")
  end

  def log_event(:read_task_spawned, _measurements, metadata) do
    key_str = format_key(metadata.key)
    debug("#{metadata.otp_name}: Read task spawned (operation: #{metadata.operation}, key: #{key_str})")
  end

  def log_event(:read_task_complete, _measurements, metadata) do
    debug("#{metadata.otp_name}: Read task completed (pid: #{inspect(metadata.task_pid)})")
  end

  def log_event(:window_advancement_no_eviction, _, %{storage_id: storage_id}) do
    debug("Window advancement considered for #{storage_id}, no eviction needed")
  end

  def log_event(:window_advancement_evicting, measurements, %{storage_id: storage_id}) do
    evicted_count = Map.get(measurements, :evicted_count, 0)
    version = Map.get(measurements, :new_durable_version)
    target_version = Map.get(measurements, :window_target_version)
    lag_microseconds = Map.get(measurements, :lag_microseconds, 0)

    info(
      "Window advancement for #{storage_id}: evicting #{evicted_count} versions, new durable version #{Version.to_string(version)}, target #{Version.to_string(target_version)}, lag #{Time.Interval.humanize({:microsecond, lag_microseconds})}"
    )
  end

  def log_event(:window_advancement_complete, _measurements, %{storage_id: storage_id}) do
    info("Window advancement complete for #{storage_id}")
  end

  def log_event(:dets_tx_build_complete, measurements, %{durable_version: version}) do
    duration_ms = measurements.duration_us / 1000
    tx_size_mb = measurements.tx_size_bytes / (1024 * 1024)
    tx_count = measurements.tx_count

    info(
      "DETS transaction built: #{tx_count} entries, #{Float.round(tx_size_mb, 2)}MB, #{Float.round(duration_ms, 2)}ms (version: #{Version.to_string(version)})"
    )
  end

  def log_event(:dets_insert_complete, measurements, %{durable_version: version}) do
    duration_ms = measurements.duration_us / 1000
    tx_size_mb = measurements.tx_size_bytes / (1024 * 1024)
    tx_count = measurements.tx_count

    info(
      "DETS insert complete: #{tx_count} entries, #{Float.round(tx_size_mb, 2)}MB, #{Float.round(duration_ms, 2)}ms (version: #{Version.to_string(version)})"
    )
  end

  def log_event(:dets_sync_complete, measurements, %{durable_version: version}) do
    duration_ms = measurements.duration_us / 1000

    info("DETS sync complete: #{Float.round(duration_ms, 2)}ms (version: #{Version.to_string(version)})")
  end

  def log_event(:dets_cleanup_complete, measurements, %{durable_version: version}) do
    duration_ms = measurements.duration_us / 1000

    info("DETS buffer cleanup complete: #{Float.round(duration_ms, 2)}ms (version: #{Version.to_string(version)})")
  end

  defp format_key(key) when is_binary(key) do
    if String.printable?(key) and byte_size(key) <= 50 do
      "\"#{key}\""
    else
      "<<#{byte_size(key)} bytes>>"
    end
  end

  defp format_key({start_key, end_key}) do
    start_str = format_key(start_key)
    end_str = format_key(end_key)
    "{#{start_str}, #{end_str}}"
  end

  defp format_key(key), do: inspect(key)

  defp format_bytes(bytes) when bytes < 1024 do
    "#{bytes}B"
  end

  defp format_bytes(bytes) when bytes < 1024 * 1024 do
    "#{Float.round(bytes / 1024, 1)}KB"
  end

  defp format_bytes(bytes) do
    "#{Float.round(bytes / (1024 * 1024), 1)}MB"
  end

  @spec debug(String.t()) :: :ok
  defp debug(message) do
    Logger.debug("Bedrock Storage Olivine: #{message}", ansi_color: :magenta)
  end

  @spec info(String.t()) :: :ok
  defp info(message) do
    Logger.info("Bedrock Storage Olivine: #{message}", ansi_color: :magenta)
  end
end
