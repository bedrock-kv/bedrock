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

  @spec trace_window_advancement_evicting(term(), term(), integer()) :: :ok
  def trace_window_advancement_evicting(storage_id, new_durable_version, evicted_count) do
    emit_storage_operation(:window_advancement_evicting, %{evicted_count: evicted_count}, %{
      storage_id: storage_id,
      new_durable_version: new_durable_version
    })
  end

  @spec trace_window_advancement_complete(term(), term()) :: :ok
  def trace_window_advancement_complete(storage_id, new_durable_version) do
    emit_storage_operation(:window_advancement_complete, %{}, %{
      storage_id: storage_id,
      new_durable_version: new_durable_version
    })
  end
end
