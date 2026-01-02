defmodule Bedrock.JobQueue.Consumer.Manager do
  @moduledoc """
  Manages job processing by dequeuing items and dispatching to workers.

  Per QuiCK paper: The Manager receives queue notifications from the Scanner,
  batch dequeues items, obtains leases, and dispatches to the Worker pool.
  """

  use GenServer

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [
    :repo,
    :root,
    :registry,
    :worker_pool,
    :concurrency,
    :batch_size,
    :lease_duration,
    :holder_id,
    :backoff_fn,
    pending_queues: MapSet.new(),
    # Maps task ref -> lease for tracking in-flight jobs
    task_leases: %{}
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
      concurrency: Keyword.get(opts, :concurrency, System.schedulers_online()),
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

  # Task completed successfully
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.task_leases, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {lease, task_leases} ->
        handle_worker_result(state, lease, result)
        state = %{state | task_leases: task_leases}
        {:noreply, process_pending(state)}
    end
  end

  # Task crashed
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.task_leases, ref) do
      {nil, _} ->
        # Unknown task, ignore
        {:noreply, state}

      {lease, task_leases} ->
        Logger.error("Job task crashed: #{inspect(reason)}")
        handle_worker_result(state, lease, {:error, {:crash, reason}})
        state = %{state | task_leases: task_leases}
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

      {:error, reason} ->
        Logger.warning("Failed to process queue #{queue_id}: #{inspect(reason)}")
        state
    end
  end

  defp dispatch_jobs(state, items, leases) do
    items_by_id = Map.new(items, &{&1.id, &1})

    Enum.reduce(leases, state, fn lease, acc_state ->
      item = Map.get(items_by_id, lease.item_id)

      if item do
        task =
          Task.Supervisor.async_nolink(
            acc_state.worker_pool,
            Worker,
            :execute,
            [item, acc_state.registry]
          )

        # Track the task ref -> lease mapping
        %{acc_state | task_leases: Map.put(acc_state.task_leases, task.ref, lease)}
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
