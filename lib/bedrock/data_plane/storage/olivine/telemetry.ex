defmodule Bedrock.DataPlane.Storage.Olivine.Telemetry do
  @moduledoc """
  Telemetry utilities specifically for Olivine storage operations.
  """

  alias Bedrock.DataPlane.Storage.Telemetry

  # Read operation trace functions (delegate to general storage telemetry)
  @spec trace_read_operation_complete(term(), term(), keyword()) :: :ok
  def trace_read_operation_complete(operation, key, opts) do
    Telemetry.trace_read_operation_complete(operation, key, opts)
  end

  # Index update trace functions
  @spec trace_index_update_complete(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def trace_index_update_complete(keys_added, keys_removed, keys_changed, total_keys) do
    total_keys_affected = keys_added + keys_removed + keys_changed

    Telemetry.emit_storage_operation(
      :index_update_complete,
      %{
        keys_added: keys_added,
        keys_removed: keys_removed,
        keys_changed: keys_changed,
        total_keys_affected: total_keys_affected,
        total_keys: total_keys
      },
      %{}
    )
  end

  # Database operation trace functions
  @spec emit_dets_sync_complete(integer(), term()) :: :ok
  def emit_dets_sync_complete(sync_time_μs, durable_version) do
    Telemetry.emit_storage_operation(:dets_sync_complete, %{duration_μs: sync_time_μs}, %{
      durable_version: durable_version
    })
  end

  @spec emit_dets_tx_build_complete(integer(), integer(), integer(), term()) :: :ok
  def emit_dets_tx_build_complete(build_time_μs, tx_size_bytes, tx_count, durable_version) do
    Telemetry.emit_storage_operation(
      :dets_tx_build_complete,
      %{duration_μs: build_time_μs, tx_size_bytes: tx_size_bytes, tx_count: tx_count},
      %{durable_version: durable_version}
    )
  end

  @spec emit_dets_insert_complete(integer(), integer(), integer(), term()) :: :ok
  def emit_dets_insert_complete(insert_time_μs, tx_size_bytes, tx_count, durable_version) do
    Telemetry.emit_storage_operation(
      :dets_insert_complete,
      %{duration_μs: insert_time_μs, tx_size_bytes: tx_size_bytes, tx_count: tx_count},
      %{durable_version: durable_version}
    )
  end

  @spec emit_file_write_complete(integer(), integer(), term()) :: :ok
  def emit_file_write_complete(write_time_μs, tx_size_bytes, data_size_in_bytes) do
    Telemetry.emit_storage_operation(
      :file_write_complete,
      %{duration_μs: write_time_μs, tx_size_bytes: tx_size_bytes},
      %{data_size_in_bytes: data_size_in_bytes}
    )
  end

  @spec emit_dets_cleanup_complete(integer(), term()) :: :ok
  def emit_dets_cleanup_complete(cleanup_time_μs, durable_version) do
    Telemetry.emit_storage_operation(:dets_cleanup_complete, %{duration_μs: cleanup_time_μs}, %{
      durable_version: durable_version
    })
  end

  # Window advancement trace functions (Olivine-specific with database metrics)
  @spec trace_window_advanced(atom(), term(), keyword()) :: :ok
  def trace_window_advanced(result, new_durable_version, opts \\ []) do
    measurements = %{
      duration_μs: Keyword.fetch!(opts, :duration_μs),
      evicted_count: Keyword.get(opts, :evicted_count, 0),
      lag_time_μs: Keyword.get(opts, :lag_time_μs, 0),
      # Database operation metrics
      durable_version_duration_μs: Keyword.get(opts, :durable_version_duration_μs, 0),
      tx_size_bytes: Keyword.get(opts, :tx_size_bytes, 0),
      tx_count: Keyword.get(opts, :tx_count, 0),
      # Database operation breakdown
      db_build_time_μs: Keyword.get(opts, :db_build_time_μs, 0),
      db_insert_time_μs: Keyword.get(opts, :db_insert_time_μs, 0),
      db_write_time_μs: Keyword.get(opts, :db_write_time_μs, 0),
      db_sync_time_μs: Keyword.get(opts, :db_sync_time_μs, 0),
      db_cleanup_time_μs: Keyword.get(opts, :db_cleanup_time_μs, 0)
    }

    metadata = %{
      result: result,
      new_durable_version: new_durable_version,
      window_target_version: Keyword.get(opts, :window_target_version),
      data_size_in_bytes: Keyword.get(opts, :data_size_in_bytes)
    }

    Telemetry.emit_storage_operation(:window_advanced, measurements, metadata)
  end
end
