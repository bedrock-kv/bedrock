defmodule Bedrock.DataPlane.Demux.PersistenceQueueTelemetry do
  @moduledoc false

  @type queue_counts :: %{
          pending: non_neg_integer(),
          scheduled: non_neg_integer(),
          in_flight: non_neg_integer(),
          lag: non_neg_integer()
        }

  @spec emit_enqueue(queue_counts(), pos_integer()) :: :ok
  def emit_enqueue(counts, capacity) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence_queue, :enqueue],
      %{
        pending: counts.pending,
        scheduled: counts.scheduled,
        in_flight: counts.in_flight,
        lag: counts.lag,
        capacity: capacity
      },
      %{}
    )
  end

  @spec emit_dequeue(queue_counts(), pos_integer()) :: :ok
  def emit_dequeue(counts, capacity) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence_queue, :dequeue],
      %{
        pending: counts.pending,
        scheduled: counts.scheduled,
        in_flight: counts.in_flight,
        lag: counts.lag,
        capacity: capacity
      },
      %{}
    )
  end

  @spec emit_backpressure(queue_counts(), pos_integer()) :: :ok
  def emit_backpressure(counts, capacity) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence_queue, :backpressure],
      %{
        pending: counts.pending,
        scheduled: counts.scheduled,
        in_flight: counts.in_flight,
        lag: counts.lag,
        capacity: capacity
      },
      %{}
    )
  end

  @spec emit_retry_scheduled(queue_counts(), pos_integer(), non_neg_integer(), term()) :: :ok
  def emit_retry_scheduled(counts, delay_ms, attempt, reason) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence_queue, :retry_scheduled],
      %{
        pending: counts.pending,
        scheduled: counts.scheduled,
        in_flight: counts.in_flight,
        lag: counts.lag,
        delay_ms: delay_ms,
        attempt: attempt
      },
      %{reason: reason}
    )
  end

  @spec emit_retry_dropped(queue_counts(), non_neg_integer(), term()) :: :ok
  def emit_retry_dropped(counts, attempt, reason) do
    :telemetry.execute(
      [:bedrock, :demux, :persistence_queue, :retry_dropped],
      %{
        pending: counts.pending,
        scheduled: counts.scheduled,
        in_flight: counts.in_flight,
        lag: counts.lag,
        attempt: attempt
      },
      %{reason: reason}
    )
  end
end
