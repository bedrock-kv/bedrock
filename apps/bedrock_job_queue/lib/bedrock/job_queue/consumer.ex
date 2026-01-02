defmodule Bedrock.JobQueue.Consumer do
  @moduledoc """
  Consumer supervision tree.

  Supervises the Scanner, Manager, and Worker Pool for processing jobs,
  following the QuiCK paper's consumer architecture.

  ## Architecture

  ```
  Consumer (Supervisor)
  ├── Task.Supervisor (worker pool)
  │   └── Task* (job execution tasks)
  ├── Manager (GenServer)
  └── Scanner (GenServer)
  ```

  - **Scanner**: Continuously scans the pointer index for queues with visible items,
    also periodically garbage collects stale pointers
  - **Manager**: Receives queue notifications, dequeues items, obtains leases, dispatches to workers
  - **Task.Supervisor**: Dynamic pool of task workers up to concurrency limit
  - **Worker**: Module providing job execution logic with timeout protection

  ## Usage

      {:ok, _pid} = Bedrock.JobQueue.Consumer.start_link(
        repo: MyApp.Repo,
        registry: MyApp.JobRegistry,
        concurrency: 10
      )
  """

  use Supervisor

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Consumer.Scanner
  alias Bedrock.Keyspace

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    registry = Keyword.fetch!(opts, :registry)
    root = Keyword.get(opts, :root, Keyspace.new("job_queue/"))
    concurrency = Keyword.get(opts, :concurrency, System.schedulers_online())
    batch_size = Keyword.get(opts, :batch_size, 10)
    scan_interval = Keyword.get(opts, :scan_interval, 100)
    backoff_fn = Keyword.get(opts, :backoff_fn, &Config.default_backoff/1)

    # Generate unique names for child processes
    id = 4 |> :crypto.strong_rand_bytes() |> Base.encode16()
    pool_name = :"#{__MODULE__}.WorkerPool.#{id}"
    manager_name = :"#{__MODULE__}.Manager.#{id}"
    scanner_name = :"#{__MODULE__}.Scanner.#{id}"

    # GC options (passed to Scanner)
    gc_interval = Keyword.get(opts, :gc_interval, 60_000)
    gc_grace_period = Keyword.get(opts, :gc_grace_period, 60_000)

    children = [
      {Task.Supervisor, name: pool_name, max_children: concurrency},
      {Manager,
       name: manager_name,
       repo: repo,
       root: root,
       registry: registry,
       worker_pool: pool_name,
       concurrency: concurrency,
       batch_size: batch_size,
       backoff_fn: backoff_fn},
      {Scanner,
       name: scanner_name,
       repo: repo,
       root: root,
       manager: manager_name,
       interval: scan_interval,
       batch_size: batch_size,
       gc_interval: gc_interval,
       gc_grace_period: gc_grace_period}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
