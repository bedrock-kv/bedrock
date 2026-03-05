defmodule Bedrock.DataPlane.Demux.PersistenceTelemetry do
  @moduledoc false

  @spec emit_write_ok(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def emit_write_ok(shard_id, batch_size, duration_us) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence, :write, :ok],
      %{
        batch_size: batch_size,
        duration_us: duration_us
      },
      %{shard_id: shard_id}
    )
  end

  @spec emit_write_error(non_neg_integer(), non_neg_integer(), non_neg_integer(), term()) :: :ok
  def emit_write_error(shard_id, batch_size, duration_us, reason) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence, :write, :error],
      %{
        batch_size: batch_size,
        duration_us: duration_us
      },
      %{
        shard_id: shard_id,
        reason: reason
      }
    )
  end

  @spec emit_watermark_advanced(non_neg_integer(), Bedrock.version(), non_neg_integer()) :: :ok
  def emit_watermark_advanced(shard_id, durable_version, buffered_transactions) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence, :watermark, :advanced],
      %{
        buffered_transactions: buffered_transactions
      },
      %{
        shard_id: shard_id,
        durable_version: durable_version
      }
    )
  end
end
