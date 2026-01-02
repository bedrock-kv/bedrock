defmodule Bedrock.JobQueue.Consumer.PointerGC do
  @moduledoc """
  Garbage collects stale pointers from the pointer index.

  Per QuiCK paper: Pointers accumulate as queues become empty. This process
  periodically scans for and removes stale pointers that point to empty queues.

  ## Configuration

  - `:interval` - How often to run GC (default: 60 seconds)
  - `:grace_period` - Time after vesting before a pointer is eligible for GC (default: 60 seconds)
  - `:batch_size` - Maximum pointers to process per GC cycle (default: 100)
  """

  use GenServer

  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  require Logger

  defstruct [:repo, :root, :interval, :grace_period, :batch_size]

  @default_interval 60_000
  @default_grace_period 60_000
  @default_batch_size 100

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      repo: Keyword.fetch!(opts, :repo),
      root: Keyword.get(opts, :root, Keyspace.new("job_queue/")),
      interval: Keyword.get(opts, :interval, @default_interval),
      grace_period: Keyword.get(opts, :grace_period, @default_grace_period),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    schedule_gc(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:gc, state) do
    run_gc(state)
    schedule_gc(state.interval)
    {:noreply, state}
  end

  defp run_gc(state) do
    result =
      state.repo.transact(fn ->
        deleted =
          Store.gc_stale_pointers(state.repo, state.root,
            grace_period: state.grace_period,
            limit: state.batch_size
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

  defp schedule_gc(interval) do
    Process.send_after(self(), :gc, interval)
  end
end
