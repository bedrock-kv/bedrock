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
  """

  use GenServer

  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [
    :repo,
    :root,
    :manager,
    :interval,
    :batch_size,
    :jitter_percent,
    :gc_interval,
    :gc_grace_period,
    :gc_batch_size,
    last_notified: [],
    gc_last_run: 0
  ]

  @default_interval 100
  @default_batch_size 100
  @default_jitter_percent 20
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
      interval: Keyword.get(opts, :interval, @default_interval),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      jitter_percent: Keyword.get(opts, :jitter_percent, @default_jitter_percent),
      gc_interval: Keyword.get(opts, :gc_interval, @default_gc_interval),
      gc_grace_period: Keyword.get(opts, :gc_grace_period, @default_gc_grace_period),
      gc_batch_size: Keyword.get(opts, :gc_batch_size, @default_gc_batch_size)
    }

    schedule_scan(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:scan, state) do
    state = scan_and_notify(state)
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

        for queue_id <- ordered do
          send(state.manager, {:queue_ready, queue_id})
        end

        # Track recently notified for round-robin fairness
        %{state | last_notified: ordered}

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

  defp schedule_scan(state) do
    jittered_interval = add_jitter(state.interval, state.jitter_percent)
    Process.send_after(self(), :scan, jittered_interval)
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
    now = System.system_time(:millisecond)

    if now - state.gc_last_run >= state.gc_interval do
      run_gc(state)
      %{state | gc_last_run: now}
    else
      state
    end
  end

  defp run_gc(state) do
    result =
      state.repo.transact(fn ->
        deleted =
          Store.gc_stale_pointers(state.repo, state.root,
            grace_period: state.gc_grace_period,
            limit: state.gc_batch_size
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
