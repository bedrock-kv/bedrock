defmodule Bedrock.DataPlane.Storage.Telemetry do
  @moduledoc """
  Telemetry utilities for storage operations.
  """

  @spec emit_storage_operation(atom(), map(), map()) :: :ok
  def emit_storage_operation(operation, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(
      [:bedrock, :storage, operation],
      measurements,
      metadata
    )
  end

  @spec emit_pull_started(String.t()) :: :ok
  def emit_pull_started(log_id) do
    emit_storage_operation(:pull_started, %{}, %{log_id: log_id})
  end

  @spec emit_pull_completed(String.t(), integer()) :: :ok
  def emit_pull_completed(log_id, duration_ms) do
    emit_storage_operation(:pull_completed, %{duration: duration_ms}, %{log_id: log_id})
  end

  @spec emit_pull_failed(String.t(), term()) :: :ok
  def emit_pull_failed(log_id, reason) do
    emit_storage_operation(:pull_failed, %{}, %{log_id: log_id, reason: reason})
  end

  @spec emit_transaction_stored(String.t(), integer()) :: :ok
  def emit_transaction_stored(log_id, transaction_count) do
    emit_storage_operation(:transaction_stored, %{count: transaction_count}, %{log_id: log_id})
  end

  # Trace functions used by pulling modules
  @spec trace_log_pull_start(term(), term()) :: :ok
  def trace_log_pull_start(start_after, _start_after) do
    emit_storage_operation(:pull_start, %{}, %{start_after: start_after})
  end

  @spec trace_log_pull_succeeded(term(), integer()) :: :ok
  def trace_log_pull_succeeded(start_after, count) do
    emit_storage_operation(:pull_succeeded, %{count: count}, %{start_after: start_after})
  end

  @spec trace_log_pull_failed(term(), term()) :: :ok
  def trace_log_pull_failed(start_after, reason) do
    emit_storage_operation(:pull_failed, %{}, %{start_after: start_after, reason: reason})
  end

  @spec trace_log_pull_circuit_breaker_tripped(term(), integer()) :: :ok
  def trace_log_pull_circuit_breaker_tripped(start_after, ms_to_wait) do
    emit_storage_operation(:circuit_breaker_tripped, %{wait_ms: ms_to_wait}, %{start_after: start_after})
  end

  @spec trace_log_marked_as_failed(term(), String.t()) :: :ok
  def trace_log_marked_as_failed(start_after, log_id) do
    emit_storage_operation(:log_marked_failed, %{}, %{start_after: start_after, log_id: log_id})
  end

  @spec trace_log_pull_circuit_breaker_reset(term()) :: :ok
  def trace_log_pull_circuit_breaker_reset(start_after) do
    emit_storage_operation(:circuit_breaker_reset, %{}, %{start_after: start_after})
  end

  # Server lifecycle trace functions
  @spec trace_startup_start(term()) :: :ok
  def trace_startup_start(otp_name) do
    emit_storage_operation(:startup_start, %{}, %{otp_name: otp_name})
  end

  @spec trace_startup_complete(term()) :: :ok
  def trace_startup_complete(otp_name) do
    emit_storage_operation(:startup_complete, %{}, %{otp_name: otp_name})
  end

  @spec trace_startup_failed(term(), term()) :: :ok
  def trace_startup_failed(otp_name, reason) do
    emit_storage_operation(:startup_failed, %{}, %{otp_name: otp_name, reason: reason})
  end

  @spec trace_shutdown_start(term(), term()) :: :ok
  def trace_shutdown_start(otp_name, reason) do
    emit_storage_operation(:shutdown_start, %{}, %{otp_name: otp_name, reason: reason})
  end

  @spec trace_shutdown_complete(term()) :: :ok
  def trace_shutdown_complete(otp_name) do
    emit_storage_operation(:shutdown_complete, %{}, %{otp_name: otp_name})
  end

  @spec trace_shutdown_waiting(term(), integer()) :: :ok
  def trace_shutdown_waiting(otp_name, task_count) do
    emit_storage_operation(:shutdown_waiting, %{task_count: task_count}, %{otp_name: otp_name})
  end

  @spec trace_shutdown_timeout(integer()) :: :ok
  def trace_shutdown_timeout(task_count) do
    emit_storage_operation(:shutdown_timeout, %{task_count: task_count}, %{})
  end

  # Window advancement trace functions
  @spec trace_window_advancement_no_eviction(term()) :: :ok
  def trace_window_advancement_no_eviction(storage_id) do
    emit_storage_operation(:window_advancement_no_eviction, %{}, %{storage_id: storage_id})
  end

  @spec trace_window_advancement_evicting(term(), term(), integer(), term(), integer()) :: :ok
  def trace_window_advancement_evicting(
        storage_id,
        new_durable_version,
        evicted_count,
        window_target_version,
        lag_microseconds
      ) do
    emit_storage_operation(
      :window_advancement_evicting,
      %{
        evicted_count: evicted_count,
        window_target_version: window_target_version,
        new_durable_version: new_durable_version,
        lag_microseconds: lag_microseconds
      },
      %{
        storage_id: storage_id
      }
    )
  end

  @spec trace_window_advancement_complete(term(), term()) :: :ok
  def trace_window_advancement_complete(storage_id, new_durable_version) do
    emit_storage_operation(:window_advancement_complete, %{}, %{
      storage_id: storage_id,
      new_durable_version: new_durable_version
    })
  end

  # Transaction processing trace functions
  @spec trace_transactions_queued(term(), integer(), integer()) :: :ok
  def trace_transactions_queued(otp_name, transaction_count, queue_size) do
    emit_storage_operation(:transactions_queued, %{transaction_count: transaction_count, queue_size: queue_size}, %{
      otp_name: otp_name
    })
  end

  @spec trace_batch_processing_start(term(), integer()) :: :ok
  def trace_batch_processing_start(otp_name, batch_size) do
    trace_batch_processing_start(otp_name, batch_size, 0)
  end

  @spec trace_batch_processing_start(term(), integer(), integer()) :: :ok
  def trace_batch_processing_start(otp_name, batch_size, batch_size_bytes) do
    emit_storage_operation(:batch_processing_start, %{batch_size: batch_size, batch_size_bytes: batch_size_bytes}, %{
      otp_name: otp_name
    })
  end

  @spec trace_batch_processing_complete(term(), integer(), integer()) :: :ok
  def trace_batch_processing_complete(otp_name, batch_size, duration_microseconds) do
    trace_batch_processing_complete(otp_name, batch_size, duration_microseconds, 0)
  end

  @spec trace_batch_processing_complete(term(), integer(), integer(), integer()) :: :ok
  def trace_batch_processing_complete(otp_name, batch_size, duration_microseconds, batch_size_bytes) do
    emit_storage_operation(
      :batch_processing_complete,
      %{batch_size: batch_size, duration_us: duration_microseconds, batch_size_bytes: batch_size_bytes},
      %{
        otp_name: otp_name
      }
    )
  end

  @spec trace_transaction_timeout_scheduled(term()) :: :ok
  def trace_transaction_timeout_scheduled(otp_name) do
    emit_storage_operation(:transaction_timeout_scheduled, %{}, %{otp_name: otp_name})
  end

  # Read operation trace functions
  @spec trace_read_request_start(term(), term(), term()) :: :ok
  def trace_read_request_start(otp_name, operation, key) do
    emit_storage_operation(:read_request_start, %{}, %{otp_name: otp_name, operation: operation, key: key})
  end

  @spec trace_read_request_complete(term(), term(), term(), integer()) :: :ok
  def trace_read_request_complete(otp_name, operation, key, duration_microseconds) do
    emit_storage_operation(:read_request_complete, %{duration_us: duration_microseconds}, %{
      otp_name: otp_name,
      operation: operation,
      key: key
    })
  end

  @spec trace_read_request_waitlisted(term(), term(), term()) :: :ok
  def trace_read_request_waitlisted(otp_name, operation, key) do
    emit_storage_operation(:read_request_waitlisted, %{}, %{otp_name: otp_name, operation: operation, key: key})
  end

  @spec trace_read_task_spawned(term(), term(), term()) :: :ok
  def trace_read_task_spawned(otp_name, operation, key) do
    emit_storage_operation(:read_task_spawned, %{}, %{otp_name: otp_name, operation: operation, key: key})
  end

  @spec trace_read_task_complete(term(), pid()) :: :ok
  def trace_read_task_complete(otp_name, task_pid) do
    emit_storage_operation(:read_task_complete, %{}, %{otp_name: otp_name, task_pid: task_pid})
  end
end
