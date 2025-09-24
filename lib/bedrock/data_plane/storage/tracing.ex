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
        [:bedrock, :storage, :log_pull_circuit_breaker_reset],
        [:bedrock, :storage, :window_advancement_no_eviction],
        [:bedrock, :storage, :window_advancement_evicting],
        [:bedrock, :storage, :window_advancement_complete],
        [:bedrock, :storage, :startup_start],
        [:bedrock, :storage, :startup_complete],
        [:bedrock, :storage, :startup_failed],
        [:bedrock, :storage, :shutdown_start],
        [:bedrock, :storage, :shutdown_complete],
        [:bedrock, :storage, :shutdown_waiting],
        [:bedrock, :storage, :shutdown_timeout],
        [:bedrock, :storage, :read_request_start],
        [:bedrock, :storage, :read_request_complete]
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
    do:
      debug(
        "Log pull started at #{Bedrock.DataPlane.Version.to_string(timestamp)} for version #{Bedrock.DataPlane.Version.to_string(next_version)}"
      )

  def log_event(:pull_start, _, %{start_after: start_after}),
    do: debug("Log pull started after #{inspect(start_after)}")

  def log_event(:pull_succeeded, _, %{timestamp: timestamp, n_transactions: n_transactions}),
    do:
      debug(
        "Log pull succeeded at #{Bedrock.DataPlane.Version.to_string(timestamp)} with #{n_transactions} transactions"
      )

  def log_event(:pull_failed, _, %{timestamp: timestamp, reason: reason}),
    do: warn("Log pull failed at #{Bedrock.DataPlane.Version.to_string(timestamp)}: #{inspect(reason)}")

  def log_event(:log_marked_as_failed, _, %{timestamp: timestamp, log_id: log_id}),
    do: warn("Log #{log_id} marked as failed at #{Bedrock.DataPlane.Version.to_string(timestamp)}")

  def log_event(:log_pull_circuit_breaker_tripped, _, %{timestamp: timestamp, ms_to_wait: ms_to_wait}),
    do:
      warn(
        "Log pull circuit breaker tripped at #{Bedrock.DataPlane.Version.to_string(timestamp)}, waiting #{ms_to_wait}ms"
      )

  def log_event(:log_pull_circuit_breaker_reset, _, %{timestamp: timestamp}),
    do: info("Log pull circuit breaker reset at #{Bedrock.DataPlane.Version.to_string(timestamp)}")

  def log_event(:fetch_start, _, %{key: key, version: version}),
    do: debug("Fetch started for key #{inspect(key)} at version #{Bedrock.DataPlane.Version.to_string(version)}")

  def log_event(:transaction_applied, _, %{version: version, n_keys: n_keys}),
    do: debug("Transaction applied at version #{Bedrock.DataPlane.Version.to_string(version)} (#{n_keys} keys)")

  def log_event(:window_advancement_no_eviction, _, %{worker_id: worker_id}),
    do: debug("Window advancement considered for #{worker_id}, no eviction needed")

  def log_event(:window_advancement_evicting, _measurements, %{
        worker_id: worker_id,
        new_durable_version: version,
        n_evicted: n_evicted,
        window_target_version: target_version
      }) do
    lag_microseconds = calculate_lag(target_version, version)

    info(
      "Window advancement for #{worker_id}: evicting #{n_evicted} versions, new durable version #{Bedrock.DataPlane.Version.to_string(version)}, target #{Bedrock.DataPlane.Version.to_string(target_version)}, lag #{format_lag(lag_microseconds)}"
    )
  end

  def log_event(:window_advancement_complete, _, %{worker_id: worker_id, new_durable_version: version}),
    do: info("Window advancement complete for #{worker_id} at version #{Bedrock.DataPlane.Version.to_string(version)}")

  def log_event(:startup_start, _, %{otp_name: otp_name}), do: info("Storage startup initiated: #{otp_name}")

  def log_event(:startup_complete, _, %{otp_name: otp_name}), do: info("Storage startup complete: #{otp_name}")

  def log_event(:startup_failed, _, %{otp_name: otp_name, reason: reason}),
    do: warn("Storage startup failed for #{otp_name}: #{inspect(reason)}")

  def log_event(:shutdown_start, _, %{otp_name: otp_name, reason: reason}),
    do: info("Storage shutdown initiated for #{otp_name}: #{inspect(reason)}")

  def log_event(:shutdown_complete, _, %{otp_name: otp_name}), do: info("Storage shutdown complete: #{otp_name}")

  def log_event(:shutdown_waiting, _, %{otp_name: otp_name, n_tasks: n_tasks}),
    do: info("Storage #{otp_name} waiting for #{n_tasks} tasks to complete")

  def log_event(:shutdown_timeout, _, %{n_tasks: n_tasks}),
    do: warn("Storage shutdown timeout with #{n_tasks} tasks still running")

  def log_event(:read_request_start, _measurements, metadata) do
    key_str = format_key(metadata.key)
    debug("#{metadata.otp_name}: Read request started (operation: #{metadata.operation}, key: #{key_str})")
  end

  def log_event(:read_request_complete, measurements, metadata) do
    duration_ms = measurements.duration_us / 1000
    key_str = format_key(metadata.key)

    info(
      "#{metadata.otp_name}: Read request completed (operation: #{metadata.operation}, key: #{key_str}, duration: #{Float.round(duration_ms, 2)}ms)"
    )
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

  defp format_lag(lag_microseconds) when lag_microseconds < 1000 do
    "#{lag_microseconds}μs"
  end

  defp format_lag(lag_microseconds) when lag_microseconds < 1_000_000 do
    "#{Float.round(lag_microseconds / 1000, 1)}ms"
  end

  defp format_lag(lag_microseconds) do
    "#{Float.round(lag_microseconds / 1_000_000, 2)}s"
  end

  defp calculate_lag(target_version, actual_version) do
    case {target_version, actual_version} do
      {target, actual} when target >= actual ->
        target - actual

      _ ->
        0
    end
  rescue
    _ -> 0
  end

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
