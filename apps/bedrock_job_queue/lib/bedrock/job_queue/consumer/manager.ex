defmodule Bedrock.JobQueue.Consumer.Manager do
  @moduledoc """
  Manages job processing by dequeuing items and dispatching to workers.

  Per QuiCK paper: The Manager receives queue notifications from the Scanner,
  batch dequeues items, obtains leases, and dispatches to the Worker pool.
  """

  use GenServer

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Consumer.WorkerPool
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [
    :repo,
    :root,
    :registry,
    :worker_pool,
    :batch_size,
    :lease_duration,
    :holder_id,
    :backoff_fn,
    pending_queues: MapSet.new()
  ]

  @default_batch_size 10
  @default_lease_duration 30_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      root: Keyword.get(opts, :root, Keyspace.new("job_queue/")),
      registry: Keyword.fetch!(opts, :registry),
      worker_pool: Keyword.fetch!(opts, :worker_pool),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      lease_duration: Keyword.get(opts, :lease_duration, @default_lease_duration),
      holder_id: :crypto.strong_rand_bytes(16),
      backoff_fn: Keyword.get(opts, :backoff_fn, &Config.default_backoff/1)
    }

    {:ok, state}
  end

  @impl true
  def handle_info({:queue_ready, queue_id}, state) do
    state = %{state | pending_queues: MapSet.put(state.pending_queues, queue_id)}
    {:noreply, process_pending(state)}
  end

  def handle_info({:worker_done, lease, result}, state) do
    handle_worker_result(state, lease, result)
    {:noreply, process_pending(state)}
  end

  defp process_pending(%{pending_queues: queues} = state) when map_size(queues) == 0 do
    state
  end

  defp process_pending(state) do
    case WorkerPool.available_workers(state.worker_pool) do
      0 ->
        state

      available ->
        {queue_id, remaining} = pop_queue(state.pending_queues)

        if queue_id do
          state = %{state | pending_queues: remaining}
          process_queue(state, queue_id, available)
        else
          state
        end
    end
  end

  defp pop_queue(queues) do
    case Enum.take(queues, 1) do
      [queue_id] -> {queue_id, MapSet.delete(queues, queue_id)}
      [] -> {nil, queues}
    end
  end

  defp process_queue(state, queue_id, max_items) do
    limit = min(max_items, state.batch_size)

    result =
      state.repo.transact(fn ->
        items = Store.peek(state.repo, state.root, queue_id, limit: limit)

        leases =
          for item <- items, reduce: [] do
            acc ->
              case Store.obtain_lease(
                     state.repo,
                     state.root,
                     item,
                     state.holder_id,
                     state.lease_duration
                   ) do
                {:ok, lease} -> [lease | acc]
                {:error, _} -> acc
              end
          end

        {:ok, {items, leases}}
      end)

    case result do
      {:ok, {items, leases}} ->
        dispatch_jobs(state, items, leases)
        state

      {:error, reason} ->
        Logger.warning("Failed to process queue #{queue_id}: #{inspect(reason)}")
        state
    end
  end

  defp dispatch_jobs(state, items, leases) do
    items_by_id = Map.new(items, &{&1.id, &1})

    for lease <- leases do
      item = Map.get(items_by_id, lease.item_id)

      if item do
        case WorkerPool.dispatch(state.worker_pool, %{
               item: item,
               lease: lease,
               registry: state.registry,
               reply_to: self()
             }) do
          {:ok, _pid} ->
            :ok

          {:error, reason} ->
            # Dispatch failed (e.g., max_children exceeded).
            # Log warning - lease will expire and item will become visible again.
            Logger.warning(
              "Failed to dispatch job #{inspect(item.id)}: #{inspect(reason)}. " <>
                "Job will be retried after lease expires."
            )
        end
      end
    end
  end

  defp handle_worker_result(state, lease, result) do
    case result do
      success when success in [:ok] or (is_tuple(success) and elem(success, 0) == :ok) ->
        run_job_action(state, lease, :complete)

      {:error, _reason} ->
        run_job_action(state, lease, :requeue)

      {:discard, reason} ->
        Logger.info("Discarding job #{lease.item_id}: #{inspect(reason)}")
        run_job_action(state, lease, :complete)

      {:snooze, delay_ms} ->
        run_job_action(state, lease, {:snooze, delay_ms})
    end
  end

  defp run_job_action(state, lease, action) do
    state.repo.transact(fn ->
      case action do
        :complete ->
          Store.complete(state.repo, state.root, lease)

        :requeue ->
          Store.requeue(state.repo, state.root, lease, backoff_fn: state.backoff_fn)

        {:snooze, delay_ms} ->
          # Snooze uses explicit delay, bypassing backoff_fn
          Store.requeue(state.repo, state.root, lease, base_delay: delay_ms, max_delay: delay_ms)
      end
    end)
  end
end
