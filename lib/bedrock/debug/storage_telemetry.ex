defmodule Bedrock.Debug.StorageTelemetry do
  @moduledoc """
  Helper module for attaching to storage telemetry events for debugging purposes.

  ## Usage

  In your livebook or debugging session:

      # Attach the debug handler
      Bedrock.Debug.StorageTelemetry.attach()
      
      # Run your operations - you'll see telemetry logs
      
      # Detach when done
      Bedrock.Debug.StorageTelemetry.detach()
  """

  require Logger

  @handler_id {__MODULE__, :debug_handler}

  @doc """
  Attach telemetry handlers for debugging storage operations.
  """
  def attach do
    :telemetry.attach_many(
      @handler_id,
      [
        [:bedrock, :storage, :get_success],
        [:bedrock, :storage, :get_timeout],
        [:bedrock, :storage, :get_unavailable]
      ],
      &handle_storage_event/4,
      %{}
    )

    Logger.info("Storage telemetry debug handler attached")
  end

  @doc """
  Detach the telemetry handlers.
  """
  def detach do
    :telemetry.detach(@handler_id)
    Logger.info("Storage telemetry debug handler detached")
  end

  defp handle_storage_event([:bedrock, :storage, :get_success], measurements, metadata, _config) do
    duration_ms = measurements.duration / 1_000_000
    key_info = get_key_info(metadata)

    Logger.info(
      "Storage GET SUCCESS: #{key_info}, duration=#{Float.round(duration_ms, 2)}ms, storage=#{inspect(metadata.storage_id)}, version=#{inspect(metadata.version)}, result=#{inspect(metadata.result)}"
    )
  end

  defp handle_storage_event([:bedrock, :storage, :get_timeout], measurements, metadata, _config) do
    duration_ms = measurements.duration / 1_000_000
    key_info = get_key_info(metadata)

    Logger.error(
      "Storage GET TIMEOUT: #{key_info}, duration=#{Float.round(duration_ms, 2)}ms, timeout=#{inspect(measurements.timeout_ms)}ms, storage=#{inspect(metadata.storage_id)}, version=#{inspect(metadata.version)}"
    )
  end

  defp handle_storage_event([:bedrock, :storage, :get_unavailable], measurements, metadata, _config) do
    duration_ms = measurements.duration / 1_000_000
    key_info = get_key_info(metadata)

    Logger.error(
      "Storage GET UNAVAILABLE: #{key_info}, duration=#{Float.round(duration_ms, 2)}ms, storage=#{inspect(metadata.storage_id)}, version=#{inspect(metadata.version)}, exit_reason=#{inspect(metadata.exit_reason)}"
    )
  end

  defp get_key_info(metadata) do
    cond do
      Map.has_key?(metadata, :key) -> "key=#{inspect(metadata.key)}"
      Map.has_key?(metadata, :key_selector) -> "key_selector=#{inspect(metadata.key_selector)}"
      true -> "key=unknown"
    end
  end
end
