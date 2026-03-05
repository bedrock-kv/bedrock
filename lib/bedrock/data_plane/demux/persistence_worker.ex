defmodule Bedrock.DataPlane.Demux.PersistenceWorker do
  @moduledoc """
  Minimal async persistence worker scaffolding built on PersistenceQueue.

  The worker is intentionally generic in this foundational slice. It processes
  payloads with an injected `:perform` function and handles bounded retries.
  """

  use GenServer

  alias Bedrock.DataPlane.Demux.PersistenceQueue

  @default_retry_tick_ms 25

  defstruct [
    :perform,
    :queue,
    :retry_tick_ms
  ]

  @type state :: %__MODULE__{
          perform: (term() -> :ok | {:error, term()}),
          queue: PersistenceQueue.t(),
          retry_tick_ms: pos_integer()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @spec enqueue(GenServer.server(), term()) :: :ok | {:error, :queue_full}
  def enqueue(server, payload) do
    GenServer.call(server, {:enqueue, payload})
  end

  @spec stats(GenServer.server()) :: PersistenceQueue.counts()
  def stats(server) do
    GenServer.call(server, :stats)
  end

  @impl true
  def init(opts) do
    perform = Keyword.fetch!(opts, :perform)

    queue =
      PersistenceQueue.new(
        capacity: Keyword.get(opts, :capacity, 1_024),
        max_retries: Keyword.get(opts, :max_retries, 5),
        retry_base_backoff_ms: Keyword.get(opts, :retry_base_backoff_ms, 25)
      )

    retry_tick_ms = Keyword.get(opts, :retry_tick_ms, @default_retry_tick_ms)

    if !is_function(perform, 1) do
      raise ArgumentError, ":perform must be a function with arity 1"
    end

    if !(is_integer(retry_tick_ms) and retry_tick_ms > 0) do
      raise ArgumentError, "retry_tick_ms must be a positive integer"
    end

    {:ok, %__MODULE__{perform: perform, queue: queue, retry_tick_ms: retry_tick_ms}}
  end

  @impl true
  def handle_call({:enqueue, payload}, _from, state) do
    case PersistenceQueue.enqueue(state.queue, payload) do
      {:ok, queue} ->
        send(self(), :drain)
        {:reply, :ok, %{state | queue: queue}}

      {:error, :full, queue} ->
        {:reply, {:error, :queue_full}, %{state | queue: queue}}
    end
  end

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply, PersistenceQueue.counts(state.queue), state}
  end

  @impl true
  def handle_info(:drain, state) do
    now_ms = System.monotonic_time(:millisecond)

    case PersistenceQueue.dequeue(state.queue, now: now_ms) do
      :empty ->
        maybe_schedule_retry_tick(state.queue, state.retry_tick_ms, now_ms)
        {:noreply, state}

      {:ok, token, payload, queue} ->
        state = %{state | queue: queue}
        {:noreply, perform_payload(state, token, payload, now_ms)}
    end
  end

  defp perform_payload(state, token, payload, now_ms) do
    case state.perform.(payload) do
      :ok ->
        {:ok, queue} = PersistenceQueue.ack(state.queue, token)
        send(self(), :drain)
        %{state | queue: queue}

      {:error, reason} ->
        case PersistenceQueue.nack(state.queue, token, reason, now: now_ms) do
          {:rescheduled, queue} ->
            maybe_schedule_retry_tick(queue, state.retry_tick_ms, now_ms)
            send(self(), :drain)
            %{state | queue: queue}

          {:dropped, queue} ->
            send(self(), :drain)
            %{state | queue: queue}

          {:error, :unknown_token, queue} ->
            %{state | queue: queue}
        end
    end
  end

  defp maybe_schedule_retry_tick(queue, retry_tick_ms, now_ms) do
    case PersistenceQueue.next_retry_due_ms(queue) do
      nil ->
        :ok

      due_at_ms ->
        delay_ms = max(due_at_ms - now_ms, retry_tick_ms)
        Process.send_after(self(), :drain, delay_ms)
    end
  end
end
