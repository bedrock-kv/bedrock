defmodule Bedrock.JobQueue do
  @moduledoc """
  A durable job queue system for Elixir, built on Bedrock.

  Modeled after Apple's QuiCK paper, this system provides:
  - Topic-based routing to worker modules via static config
  - Two-level sharding (per-queue zones + pointer index)
  - Priority ordering and scheduled/delayed jobs
  - Fault-tolerant leasing via vesting time
  - Scanner/Manager/Worker consumer architecture

  ## Quick Start

      # Configure workers (in config.exs)
      config :bedrock_job_queue, :workers, %{
        "user:created" => MyApp.Jobs.UserCreated,
        "email:send" => MyApp.Jobs.SendEmail
      }

      # Define a job module
      defmodule MyApp.Jobs.UserCreated do
        use Bedrock.JobQueue.Job, topic: "user:created"

        @impl true
        def perform(%{user_id: user_id}, meta) do
          # meta.topic, meta.queue_id, meta.item_id, meta.attempt available
          :ok
        end
      end

      # Start consumer
      Bedrock.JobQueue.start_consumer(repo: MyRepo, concurrency: 10)

      # Enqueue jobs
      Bedrock.JobQueue.enqueue("tenant_1", "user:created", %{user_id: 123})

  ## Scheduling Jobs

      # Schedule for a specific time
      Bedrock.JobQueue.enqueue_at("tenant_1", "email:send", payload, ~U[2024-01-15 10:00:00Z])

      # Schedule with a delay
      Bedrock.JobQueue.enqueue_in("tenant_1", "cleanup", payload, :timer.hours(1))

  ## Priority

  Lower priority values are processed first:

      Bedrock.JobQueue.enqueue("tenant_1", "urgent", payload, priority: 0)   # Processed first
      Bedrock.JobQueue.enqueue("tenant_1", "normal", payload, priority: 100) # Default
      Bedrock.JobQueue.enqueue("tenant_1", "batch", payload, priority: 200)  # Processed last
  """

  alias Bedrock.JobQueue.Config
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  @type queue_id :: String.t()
  @type topic :: String.t()
  @type payload :: map() | binary()

  @type enqueue_opts :: [
          priority: integer(),
          scheduled_at: DateTime.t(),
          max_retries: non_neg_integer(),
          id: binary(),
          repo: module(),
          root: Keyspace.t()
        ]

  @doc """
  Enqueues a job for processing.

  ## Options

  - `:priority` - Integer priority (lower = higher priority, default: 100)
  - `:max_retries` - Maximum retry attempts (default: 3)
  - `:id` - Custom job ID (default: auto-generated UUID)
  - `:repo` - The Bedrock repo module
  - `:root` - Root keyspace for the job queue

  ## Examples

      Bedrock.JobQueue.enqueue("tenant_1", "user:created", %{user_id: 123})
      Bedrock.JobQueue.enqueue("tenant_1", "email:send", %{to: "user@example.com"}, priority: 10)
  """
  @spec enqueue(queue_id(), topic(), payload(), enqueue_opts()) ::
          {:ok, Item.t()} | {:error, term()}
  def enqueue(queue_id, topic, payload, opts \\ []) do
    repo = Keyword.get(opts, :repo) || default_repo()
    root = Keyword.get(opts, :root) || default_root()
    item = Item.new(queue_id, topic, payload, opts)

    repo.transact(fn ->
      Store.enqueue(repo, root, item)
      {:ok, item}
    end)
  end

  @doc """
  Enqueues a job scheduled for a specific time.

  ## Examples

      Bedrock.JobQueue.enqueue_at("tenant_1", "reminder", payload, ~U[2024-01-15 10:00:00Z])
  """
  @spec enqueue_at(queue_id(), topic(), payload(), DateTime.t(), enqueue_opts()) ::
          {:ok, Item.t()} | {:error, term()}
  def enqueue_at(queue_id, topic, payload, %DateTime{} = scheduled_at, opts \\ []) do
    vesting_time = DateTime.to_unix(scheduled_at, :millisecond)
    enqueue(queue_id, topic, payload, Keyword.put(opts, :vesting_time, vesting_time))
  end

  @doc """
  Enqueues a job with a delay.

  ## Examples

      # Run in 1 hour
      Bedrock.JobQueue.enqueue_in("tenant_1", "cleanup", payload, :timer.hours(1))

      # Run in 30 seconds
      Bedrock.JobQueue.enqueue_in("tenant_1", "retry", payload, 30_000)
  """
  @spec enqueue_in(queue_id(), topic(), payload(), non_neg_integer(), enqueue_opts()) ::
          {:ok, Item.t()} | {:error, term()}
  def enqueue_in(queue_id, topic, payload, delay_ms, opts \\ []) when is_integer(delay_ms) do
    vesting_time = System.system_time(:millisecond) + delay_ms
    enqueue(queue_id, topic, payload, Keyword.put(opts, :vesting_time, vesting_time))
  end

  @doc """
  Starts a consumer for processing jobs.

  ## Options

  - `:repo` - The Bedrock repo module (required)
  - `:concurrency` - Number of concurrent workers (default: System.schedulers_online())
  - `:batch_size` - Items to dequeue per batch (default: 10)

  ## Examples

      Bedrock.JobQueue.start_consumer(repo: MyApp.Repo, concurrency: 10)
  """
  @spec start_consumer(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_consumer(opts) do
    config = Config.new(opts)

    child_spec =
      {Bedrock.JobQueue.Consumer, repo: config.repo, concurrency: config.concurrency, batch_size: config.batch_size}

    DynamicSupervisor.start_child(Bedrock.JobQueue.ConsumerSupervisor, child_spec)
  end

  @doc """
  Gets queue statistics.

  Returns a map with `:pending_count` and `:processing_count`.
  """
  @spec stats(queue_id(), keyword()) :: map()
  def stats(queue_id, opts \\ []) do
    repo = Keyword.get(opts, :repo) || default_repo()
    root = Keyword.get(opts, :root) || default_root()

    fn ->
      {:ok, Store.stats(repo, root, queue_id)}
    end
    |> repo.transact()
    |> case do
      {:ok, stats} -> stats
      error -> error
    end
  end

  defp default_repo do
    Application.get_env(:bedrock_job_queue, :repo) ||
      raise "No repo configured. Set :repo in options or configure :bedrock_job_queue, :repo"
  end

  defp default_root do
    Application.get_env(:bedrock_job_queue, :root, Keyspace.new("job_queue/"))
  end
end
