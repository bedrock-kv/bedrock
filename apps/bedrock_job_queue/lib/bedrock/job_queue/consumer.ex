defmodule Bedrock.JobQueue.Consumer do
  @moduledoc """
  Consumer supervision tree.

  Supervises the Scanner, Manager, and Worker Pool for processing jobs,
  following the QuiCK paper's consumer architecture.

  ## Architecture

  ```
  Consumer (Supervisor)
  ├── WorkerPool (DynamicSupervisor)
  │   └── Worker* (temporary GenServers)
  ├── Manager (GenServer)
  ├── Scanner (GenServer)
  └── PointerGC (GenServer)
  ```

  - **Scanner**: Continuously scans the pointer index for queues with visible items
  - **Manager**: Receives queue notifications, dequeues items, obtains leases, dispatches to workers
  - **WorkerPool**: Dynamic pool of workers up to concurrency limit
  - **Worker**: Executes individual jobs with timeout protection
  - **PointerGC**: Garbage collects stale pointers from empty queues

  ## Usage

      {:ok, _pid} = Bedrock.JobQueue.Consumer.start_link(
        repo: MyApp.Repo,
        registry: MyApp.JobRegistry,
        concurrency: 10
      )
  """

  use Supervisor

  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Consumer.PointerGC
  alias Bedrock.JobQueue.Consumer.Scanner
  alias Bedrock.JobQueue.Consumer.WorkerPool
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

    # Generate unique names for child processes
    id = 4 |> :crypto.strong_rand_bytes() |> Base.encode16()
    pool_name = :"#{__MODULE__}.WorkerPool.#{id}"
    manager_name = :"#{__MODULE__}.Manager.#{id}"
    scanner_name = :"#{__MODULE__}.Scanner.#{id}"
    gc_name = :"#{__MODULE__}.PointerGC.#{id}"

    # GC options
    gc_interval = Keyword.get(opts, :gc_interval, 60_000)
    gc_grace_period = Keyword.get(opts, :gc_grace_period, 60_000)

    children = [
      {WorkerPool, name: pool_name, concurrency: concurrency},
      {Manager,
       name: manager_name, repo: repo, root: root, registry: registry, worker_pool: pool_name, batch_size: batch_size},
      {Scanner,
       name: scanner_name,
       repo: repo,
       root: root,
       manager: manager_name,
       interval: scan_interval,
       batch_size: batch_size},
      {PointerGC, name: gc_name, repo: repo, root: root, interval: gc_interval, grace_period: gc_grace_period}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
