defmodule Bedrock.DataPlane.Materializer.Olivine.Telemetry do
  @moduledoc """
  Telemetry utilities specifically for Olivine materializer operations.
  """

  alias Bedrock.DataPlane.Materializer.Telemetry

  @spec trace_read_operation_complete(term(), term(), keyword()) :: :ok
  def trace_read_operation_complete(operation, key, opts) do
    Telemetry.trace_read_operation_complete(operation, key, opts)
  end

  @spec trace_index_update_complete(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def trace_index_update_complete(keys_added, keys_removed, keys_changed, total_keys) do
    total_keys_affected = keys_added + keys_removed + keys_changed

    Telemetry.emit_materializer_operation(
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

  @spec trace_window_advanced(atom(), term(), keyword()) :: :ok
  def trace_window_advanced(result, new_durable_version, opts \\ []) do
    measurements = %{
      duration_μs: Keyword.fetch!(opts, :duration_μs),
      evicted_count: Keyword.get(opts, :evicted_count, 0),
      lag_time_μs: Keyword.get(opts, :lag_time_μs, 0),
      # Database operation metrics
      durable_version_duration_μs: Keyword.get(opts, :durable_version_duration_μs, 0),
      # Database operation breakdown
      db_insert_time_μs: Keyword.get(opts, :db_insert_time_μs, 0),
      db_write_time_μs: Keyword.get(opts, :db_write_time_μs, 0)
    }

    metadata = %{
      result: result,
      new_durable_version: new_durable_version,
      window_target_version: Keyword.get(opts, :window_target_version),
      data_size_in_bytes: Keyword.get(opts, :data_size_in_bytes)
    }

    Telemetry.emit_materializer_operation(:window_advanced, measurements, metadata)
  end

  @spec trace_compaction_started(Bedrock.version(), keyword()) :: :ok
  def trace_compaction_started(durable_version, opts \\ []) do
    measurements = %{
      data_size_before: Keyword.get(opts, :data_size_before, 0),
      index_size_before: Keyword.get(opts, :index_size_before, 0)
    }

    metadata = %{
      durable_version: durable_version
    }

    Telemetry.emit_materializer_operation(:compaction_started, measurements, metadata)
  end

  @spec trace_compaction_complete(Bedrock.version(), keyword()) :: :ok
  def trace_compaction_complete(durable_version, opts \\ []) do
    measurements = %{
      duration_μs: Keyword.fetch!(opts, :duration_μs),
      data_size_before: Keyword.get(opts, :data_size_before, 0),
      data_size_after: Keyword.get(opts, :data_size_after, 0),
      index_size_before: Keyword.get(opts, :index_size_before, 0),
      index_size_after: Keyword.get(opts, :index_size_after, 0),
      values_compacted: Keyword.get(opts, :values_compacted, 0)
    }

    metadata = %{
      durable_version: durable_version,
      space_reclaimed_bytes: measurements.data_size_before - measurements.data_size_after,
      compaction_ratio:
        if measurements.data_size_before > 0 do
          Float.round(measurements.data_size_after / measurements.data_size_before, 2)
        else
          1.0
        end
    }

    Telemetry.emit_materializer_operation(:compaction_complete, measurements, metadata)
  end

  @spec trace_compaction_failed(term()) :: :ok
  def trace_compaction_failed(reason) do
    Telemetry.emit_materializer_operation(
      :compaction_failed,
      %{},
      %{reason: reason}
    )
  end
end
