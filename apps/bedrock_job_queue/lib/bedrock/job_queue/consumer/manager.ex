defmodule Bedrock.JobQueue.Consumer.Manager do
  @moduledoc """
  Manages job processing by dequeuing items and dispatching to workers.

  Per QuiCK paper: The Manager receives queue notifications from the Scanner,
  batch dequeues items, obtains leases, and dispatches to the Worker pool.
  """

  use GenServer

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Consumer.LeaseExtender
  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [
    :repo,
    :root,
    :workers,
    :worker_pool,
    :concurrency,
    :batch_size,
    :lease_duration,
    :queue_lease_duration,
    :holder_id,
    :backoff_fn,
    pending_queues: MapSet.new(),
    # Maps task ref -> {lease, extender_pid} for tracking in-flight jobs
    task_info: %{}
  ]

  @default_batch_size 10
  @default_lease_duration 30_000
  # Queue lease duration is short - just enough to dequeue items
  # Per QuiCK paper: prevents thundering herd on hot queues
  @default_queue_lease_duration 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      root: Keyword.get(opts, :root, Keyspace.new("job_queue/")),
      workers: Keyword.fetch!(opts, :workers),
      worker_pool: Keyword.fetch!(opts, :worker_pool),
      concurrency: Keyword.get(opts, :concurrency, System.schedulers_online()),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      lease_duration: Keyword.get(opts, :lease_duration, @default_lease_duration),
      queue_lease_duration: Keyword.get(opts, :queue_lease_duration, @default_queue_lease_duration),
      holder_id: Keyword.get(opts, :holder_id, :crypto.strong_rand_bytes(16)),
      backoff_fn: Keyword.get(opts, :backoff_fn, &Config.default_backoff/1)
    }

    {:ok, state}
  end

  @impl true
  def handle_info({:queue_ready, queue_id}, state) do
    state = %{state | pending_queues: MapSet.put(state.pending_queues, queue_id)}
    {:noreply, process_pending(state)}
  end

  # Task completed successfully
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.task_info, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {{lease, extender_pid}, task_info} ->
        # Stop the lease extender
        LeaseExtender.stop(extender_pid)
        handle_worker_result(state, lease, result)
        state = %{state | task_info: task_info}
        {:noreply, process_pending(state)}
    end
  end

  # Task crashed
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.task_info, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {{lease, extender_pid}, task_info} ->
        # Stop the lease extender
        LeaseExtender.stop(extender_pid)
        Logger.error("Job task crashed: #{inspect(reason)}")
        handle_worker_result(state, lease, {:error, {:crash, reason}})
        state = %{state | task_info: task_info}
        {:noreply, process_pending(state)}
    end
  end

  defp process_pending(%{pending_queues: queues} = state) when map_size(queues) == 0 do
    state
  end

  defp process_pending(state) do
    case available_workers(state) do
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

  defp available_workers(state) do
    # Task.Supervisor uses :workers instead of :active
    children = Task.Supervisor.children(state.worker_pool)
    max(0, state.concurrency - length(children))
  end

  defp pop_queue(queues) do
    case Enum.take(queues, 1) do
      [queue_id] -> {queue_id, MapSet.delete(queues, queue_id)}
      [] -> {nil, queues}
    end
  end

  defp process_queue(state, queue_id, max_items) do
    limit = min(max_items, state.batch_size)

    # Per QuiCK Algorithm 2: First obtain queue lease to prevent thundering herd
    result = state.repo.transact(fn -> dequeue_with_lease(state, queue_id, limit) end)

    handle_dequeue_result(state, result)
  end

  defp dequeue_with_lease(state, queue_id, limit) do
    case Store.obtain_queue_lease(state.repo, state.root, queue_id, state.holder_id, state.queue_lease_duration) do
      {:ok, queue_lease} ->
        result = do_dequeue(state, queue_id, limit)
        # Release the queue lease after dequeuing to allow subsequent dequeue attempts
        Store.release_queue_lease(state.repo, state.root, queue_lease)
        result

      {:error, :queue_leased} ->
        {:skip, :queue_leased}
    end
  end

  defp do_dequeue(state, queue_id, limit) do
    items = Store.peek(state.repo, state.root, queue_id, limit: limit)
    leases = obtain_item_leases(state, items)
    update_pointer_for_remaining(state, queue_id)
    {:ok, {items, leases}}
  end

  defp obtain_item_leases(state, items) do
    Enum.reduce(items, [], fn item, acc ->
      case Store.obtain_lease(state.repo, state.root, item, state.holder_id, state.lease_duration) do
        {:ok, lease} -> [lease | acc]
        {:error, _} -> acc
      end
    end)
  end

  # Per QuiCK Algorithm 2 lines 6-9: After dequeuing, update pointer to min vesting_time
  defp update_pointer_for_remaining(state, queue_id) do
    case Store.min_vesting_time(state.repo, state.root, queue_id) do
      nil -> :ok
      min_vesting -> Store.update_queue_pointer(state.repo, state.root, queue_id, min_vesting)
    end
  end

  defp handle_dequeue_result(state, {:ok, {items, leases}}), do: dispatch_jobs(state, items, leases)
  defp handle_dequeue_result(state, {:skip, :queue_leased}), do: state

  defp handle_dequeue_result(state, {:error, reason}) do
    Logger.warning("Failed to process queue: #{inspect(reason)}")
    state
  end

  defp dispatch_jobs(state, items, leases) do
    items_by_id = Map.new(items, &{&1.id, &1})

    Enum.reduce(leases, state, fn lease, acc_state ->
      item = Map.get(items_by_id, lease.item_id)

      if item do
        # Start lease extender to periodically extend the lease during job execution
        # Per QuiCK Algorithm 3: extend_lease runs in parallel with job processing
        extender_pid =
          LeaseExtender.start(
            acc_state.repo,
            acc_state.root,
            lease,
            acc_state.lease_duration
          )

        task =
          Task.Supervisor.async_nolink(
            acc_state.worker_pool,
            Worker,
            :execute,
            [item, acc_state.workers]
          )

        # Track the task ref -> {lease, extender_pid} mapping
        %{acc_state | task_info: Map.put(acc_state.task_info, task.ref, {lease, extender_pid})}
      else
        acc_state
      end
    end)
  end

  defp handle_worker_result(state, lease, result) do
    case result do
      success when success in [:ok] or (is_tuple(success) and elem(success, 0) == :ok) ->
        run_job_action(state, lease, :complete)

      {:error, _reason} ->
        run_job_action(state, lease, :requeue)

      {:discard, reason} ->
        Logger.info("Discarding job #{Base.encode16(lease.item_id, case: :lower)}: #{inspect(reason)}")
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
