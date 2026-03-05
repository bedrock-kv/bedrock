defmodule Bedrock.DataPlane.Demux.PersistenceQueue do
  @moduledoc """
  Bounded queue primitives for async shard persistence.

  This queue tracks pending, in-flight, and retry-scheduled entries.
  It is intentionally side-effect free aside from telemetry emission.
  """

  alias Bedrock.DataPlane.Demux.PersistenceQueueTelemetry, as: Telemetry

  @default_capacity 1_024
  @default_max_retries 5
  @default_retry_base_backoff_ms 25

  @type token :: pos_integer()
  @type payload :: term()
  @type entry :: %{payload: payload(), attempt: non_neg_integer(), enqueued_at_ms: non_neg_integer()}
  @type scheduled_entry :: %{
          payload: payload(),
          attempt: non_neg_integer(),
          enqueued_at_ms: non_neg_integer(),
          due_at_ms: non_neg_integer()
        }
  @type counts :: %{
          pending: non_neg_integer(),
          scheduled: non_neg_integer(),
          in_flight: non_neg_integer(),
          lag: non_neg_integer()
        }

  @type t :: %__MODULE__{
          capacity: pos_integer(),
          max_retries: non_neg_integer(),
          retry_base_backoff_ms: pos_integer(),
          pending: :queue.queue(entry()),
          scheduled: [scheduled_entry()],
          in_flight: %{token() => entry()},
          next_token: token()
        }

  defstruct capacity: @default_capacity,
            max_retries: @default_max_retries,
            retry_base_backoff_ms: @default_retry_base_backoff_ms,
            pending: :queue.new(),
            scheduled: [],
            in_flight: %{},
            next_token: 1

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, @default_capacity)
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    retry_base_backoff_ms = Keyword.get(opts, :retry_base_backoff_ms, @default_retry_base_backoff_ms)

    if !(is_integer(capacity) and capacity > 0) do
      raise ArgumentError, "capacity must be a positive integer"
    end

    if !(is_integer(max_retries) and max_retries >= 0) do
      raise ArgumentError, "max_retries must be a non-negative integer"
    end

    if !(is_integer(retry_base_backoff_ms) and retry_base_backoff_ms > 0) do
      raise ArgumentError, "retry_base_backoff_ms must be a positive integer"
    end

    %__MODULE__{
      capacity: capacity,
      max_retries: max_retries,
      retry_base_backoff_ms: retry_base_backoff_ms
    }
  end

  @spec enqueue(t(), payload(), keyword()) :: {:ok, t()} | {:error, :full, t()}
  def enqueue(queue, payload, opts \\ []) do
    now_ms = now_ms(opts)
    queue = promote_retries(queue, now_ms)

    if occupied(queue) >= queue.capacity do
      Telemetry.emit_backpressure(counts(queue), queue.capacity)
      {:error, :full, queue}
    else
      entry = %{payload: payload, attempt: 0, enqueued_at_ms: now_ms}
      queue = %{queue | pending: :queue.in(entry, queue.pending)}
      Telemetry.emit_enqueue(counts(queue), queue.capacity)
      {:ok, queue}
    end
  end

  @spec dequeue(t(), keyword()) :: {:ok, token(), payload(), t()} | :empty
  def dequeue(queue, opts \\ []) do
    now_ms = now_ms(opts)
    queue = promote_retries(queue, now_ms)

    case :queue.out(queue.pending) do
      {{:value, entry}, pending} ->
        token = queue.next_token

        queue =
          queue
          |> Map.put(:pending, pending)
          |> Map.update!(:in_flight, &Map.put(&1, token, Map.delete(entry, :due_at_ms)))
          |> Map.put(:next_token, token + 1)

        Telemetry.emit_dequeue(counts(queue), queue.capacity)
        {:ok, token, entry.payload, queue}

      {:empty, _pending} ->
        :empty
    end
  end

  @spec ack(t(), token()) :: {:ok, t()} | {:error, :unknown_token, t()}
  def ack(queue, token) do
    case Map.pop(queue.in_flight, token) do
      {nil, _in_flight} ->
        {:error, :unknown_token, queue}

      {_entry, in_flight} ->
        {:ok, %{queue | in_flight: in_flight}}
    end
  end

  @spec nack(t(), token(), term(), keyword()) ::
          {:rescheduled, t()} | {:dropped, t()} | {:error, :unknown_token, t()}
  def nack(queue, token, reason, opts \\ []) do
    now_ms = now_ms(opts)
    queue = promote_retries(queue, now_ms)

    case Map.pop(queue.in_flight, token) do
      {nil, _in_flight} ->
        {:error, :unknown_token, queue}

      {entry, in_flight} ->
        queue = %{queue | in_flight: in_flight}
        attempt = entry.attempt + 1

        if attempt > queue.max_retries do
          Telemetry.emit_retry_dropped(counts(queue), attempt, reason)
          {:dropped, queue}
        else
          delay_ms = Keyword.get(opts, :backoff_ms, retry_backoff_ms(queue, attempt))

          scheduled_entry =
            entry
            |> Map.put(:attempt, attempt)
            |> Map.put(:due_at_ms, now_ms + delay_ms)

          queue = %{queue | scheduled: insert_scheduled(queue.scheduled, scheduled_entry)}
          Telemetry.emit_retry_scheduled(counts(queue), delay_ms, attempt, reason)
          {:rescheduled, queue}
        end
    end
  end

  @spec counts(t()) :: counts()
  def counts(queue) do
    pending = :queue.len(queue.pending)
    scheduled = length(queue.scheduled)
    in_flight = map_size(queue.in_flight)

    %{
      pending: pending,
      scheduled: scheduled,
      in_flight: in_flight,
      lag: pending + scheduled
    }
  end

  @spec lag(t()) :: non_neg_integer()
  def lag(queue), do: counts(queue).lag

  @spec next_retry_due_ms(t()) :: non_neg_integer() | nil
  def next_retry_due_ms(%__MODULE__{scheduled: [%{due_at_ms: due_at_ms} | _]}), do: due_at_ms
  def next_retry_due_ms(_queue), do: nil

  defp occupied(queue) do
    :queue.len(queue.pending) + length(queue.scheduled) + map_size(queue.in_flight)
  end

  defp retry_backoff_ms(queue, attempt) do
    multiplier = trunc(:math.pow(2, max(attempt - 1, 0)))
    queue.retry_base_backoff_ms * multiplier
  end

  defp promote_retries(queue, now_ms) do
    {ready, waiting} = Enum.split_while(queue.scheduled, &(&1.due_at_ms <= now_ms))
    pending = Enum.reduce(ready, queue.pending, fn entry, acc -> :queue.in(Map.delete(entry, :due_at_ms), acc) end)
    %{queue | scheduled: waiting, pending: pending}
  end

  defp insert_scheduled([], entry), do: [entry]

  defp insert_scheduled([head | tail] = scheduled, entry) do
    if entry.due_at_ms < head.due_at_ms do
      [entry | scheduled]
    else
      [head | insert_scheduled(tail, entry)]
    end
  end

  defp now_ms(opts), do: Keyword.get(opts, :now, System.monotonic_time(:millisecond))
end
