defmodule Bedrock.JobQueue.Consumer.Scanner do
  @moduledoc """
  Scans the pointer index for queues with visible items.

  Per QuiCK paper: The Scanner continuously scans the pointer index to find
  queues that have items ready to process.

  ## Fairness (QuiCK-inspired)

  To prevent unfair prioritization of queues that happen to appear first:
  - Queue IDs are shuffled before notification
  - Last-notified tracking ensures round-robin across scans
  - Jittered scan intervals prevent thundering herd

  ## Pointer GC

  The Scanner also periodically garbage collects stale pointers from empty queues.
  This is done as part of the scan cycle to avoid an extra process.

  ## Configuration

  - `:repo` - Required. The Bedrock Repo module
  - `:manager` - Required. The Manager process name/pid to notify
  - `:name` - Process name (default: `Bedrock.JobQueue.Consumer.Scanner`)
  - `:root` - Root keyspace (default: `Keyspace.new("job_queue/")`)
  - `:interval` - Base scan interval in ms (default: 100)
  - `:batch_size` - Max pointers to scan per cycle (default: 100)
  - `:jitter_percent` - Random jitter as percentage of interval (default: 20)
  - `:selection_frac` - Fraction of visible queues to notify (default: 0.5)
  - `:selection_max` - Maximum queues to notify per scan (default: 10)
  - `:gc_interval` - How often to run pointer GC in ms (default: 60_000)
  - `:gc_grace_period` - Grace period before considering pointer stale in ms (default: 60_000)
  - `:gc_batch_size` - Max stale pointers to GC per cycle (default: 100)
  - `:now_fn` - Function returning current time in ms (default: `fn -> System.system_time(:millisecond) end`)
  - `:worker_pool` - Task.Supervisor for checking worker availability (optional)
  - `:concurrency` - Max workers for availability check (default: `System.schedulers_online()`)
  """

  use GenServer

  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [
    :repo,
    :root,
    :manager,
    :worker_pool,
    :concurrency,
    :interval,
    :batch_size,
    :jitter_percent,
    :selection_frac,
    :selection_max,
    :gc_interval,
    :gc_grace_period,
    :gc_batch_size,
    :now_fn,
    last_notified: [],
    gc_last_run: 0
  ]

  @default_interval 100
  @default_batch_size 100
  @default_jitter_percent 20
  # Per QuiCK Algorithm 1: select a random fraction of visible pointers
  # to reduce contention between multiple scanners
  @default_selection_frac 0.5
  @default_selection_max 10
  @default_gc_interval 60_000
  @default_gc_grace_period 60_000
  @default_gc_batch_size 100

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      root: Keyword.get(opts, :root, Keyspace.new("job_queue/")),
      manager: Keyword.fetch!(opts, :manager),
      worker_pool: Keyword.get(opts, :worker_pool),
      concurrency: Keyword.get(opts, :concurrency, System.schedulers_online()),
      interval: Keyword.get(opts, :interval, @default_interval),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      jitter_percent: Keyword.get(opts, :jitter_percent, @default_jitter_percent),
      selection_frac: Keyword.get(opts, :selection_frac, @default_selection_frac),
      selection_max: Keyword.get(opts, :selection_max, @default_selection_max),
      gc_interval: Keyword.get(opts, :gc_interval, @default_gc_interval),
      gc_grace_period: Keyword.get(opts, :gc_grace_period, @default_gc_grace_period),
      gc_batch_size: Keyword.get(opts, :gc_batch_size, @default_gc_batch_size),
      now_fn: Keyword.get(opts, :now_fn, fn -> System.system_time(:millisecond) end)
    }

    schedule_scan(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:scan, state) do
    # Per QuiCK Algorithm 1 line 5: wait until at least one worker has no task
    state =
      if workers_available?(state) do
        scan_and_notify(state)
      else
        state
      end

    state = maybe_run_gc(state)
    schedule_scan(state)
    {:noreply, state}
  end

  defp scan_and_notify(state) do
    result =
      state.repo.transact(fn ->
        queue_ids = Store.scan_visible_queues(state.repo, state.root, limit: state.batch_size)
        {:ok, queue_ids}
      end)

    case result do
      {:ok, queue_ids} ->
        # Shuffle for fairness, prioritizing queues not recently notified
        ordered = prioritize_queues(queue_ids, state.last_notified)

        # Per QuiCK Algorithm 1: select a random fraction of pointers
        # to reduce contention between multiple scanners
        selected = select_random_fraction(ordered, state.selection_frac, state.selection_max)

        for queue_id <- selected do
          send(state.manager, {:queue_ready, queue_id})
        end

        # Track recently notified for round-robin fairness
        %{state | last_notified: selected}

      {:error, _} ->
        state
    end
  end

  # Prioritize queues that weren't notified in the last scan
  defp prioritize_queues(queue_ids, last_notified) do
    {fresh, stale} = Enum.split_with(queue_ids, &(&1 not in last_notified))

    # Shuffle each group, then concatenate (fresh first)
    Enum.shuffle(fresh) ++ Enum.shuffle(stale)
  end

  # Per QuiCK Algorithm 1: select min(selection_max, ceil(size * selection_frac)) randomly
  # The list is already shuffled by prioritize_queues, so we just take the first N
  defp select_random_fraction(queue_ids, selection_frac, selection_max) do
    size = length(queue_ids)
    count = selection_max |> min(ceil(size * selection_frac)) |> trunc()
    Enum.take(queue_ids, max(1, count))
  end

  defp schedule_scan(state) do
    jittered_interval = add_jitter(state.interval, state.jitter_percent)
    Process.send_after(self(), :scan, jittered_interval)
  end

  # Per QuiCK Algorithm 1 line 5: wait until at least one worker has no task
  # Returns true if worker_pool is not configured (backward compat) or has available workers
  defp workers_available?(%{worker_pool: nil}), do: true

  defp workers_available?(%{worker_pool: pool, concurrency: concurrency}) do
    active = length(Task.Supervisor.children(pool))
    active < concurrency
  end

  # Add random jitter to prevent synchronized scans across consumers
  defp add_jitter(interval, jitter_percent) do
    jitter_range = div(interval * jitter_percent, 100)

    if jitter_range > 0 do
      interval + :rand.uniform(jitter_range * 2) - jitter_range
    else
      interval
    end
  end

  # Run GC if gc_interval has passed since last run
  defp maybe_run_gc(state) do
    now = state.now_fn.()

    if now - state.gc_last_run >= state.gc_interval do
      run_gc(state, now)
      %{state | gc_last_run: now}
    else
      state
    end
  end

  defp run_gc(state, now) do
    result =
      state.repo.transact(fn ->
        deleted =
          Store.gc_stale_pointers(state.repo, state.root,
            grace_period: state.gc_grace_period,
            limit: state.gc_batch_size,
            now: now
          )

        {:ok, deleted}
      end)

    case result do
      {:ok, deleted} when deleted > 0 ->
        Logger.debug("Pointer GC: deleted #{deleted} stale pointers")

      _ ->
        :ok
    end
  end
end
