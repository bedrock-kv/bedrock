# Bedrock.JobQueue Basic Usage Example
#
# This example demonstrates how to use Bedrock.JobQueue for background job processing.
# The job queue is modeled after Apple's QuiCK paper and provides:
# - Priority-based job ordering
# - Scheduled/delayed job execution
# - Automatic retries with exponential backoff
# - Per-tenant/queue isolation
#
# To run this example:
#   mix run apps/bedrock_job_queue/examples/basic_usage.exs

alias Bedrock.JobQueue

# =============================================================================
# Step 1: Define Job Modules
# =============================================================================
#
# Jobs are defined as modules that implement the Bedrock.JobQueue.Job behaviour.
# Each job module specifies its topic pattern and default options.

defmodule ExampleJobs.SendEmail do
  @moduledoc """
  Job for sending emails to users.

  This job demonstrates basic success handling.
  """
  use Bedrock.JobQueue.Job,
    topic: "email:send",
    max_retries: 3,
    priority: 50

  @impl true
  def perform(%{to: to, subject: subject, body: body} = _args) do
    IO.puts("[SendEmail] Sending email to #{to}: #{subject}")

    # Simulate sending email
    Process.sleep(100)

    # Return :ok to mark the job as completed
    :ok
  end
end

defmodule ExampleJobs.ProcessWebhook do
  @moduledoc """
  Job for processing incoming webhooks.

  This job demonstrates returning {:ok, result} with a value.
  """
  use Bedrock.JobQueue.Job,
    topic: "webhook:*",
    priority: 10  # Higher priority (lower number = processed first)

  @impl true
  def perform(%{event: event, payload: payload}) do
    IO.puts("[ProcessWebhook] Processing #{event} event")

    # Process the webhook
    result = %{processed_at: DateTime.utc_now(), event: event}

    # Return {:ok, result} - the result is logged but otherwise ignored
    {:ok, result}
  end
end

defmodule ExampleJobs.SyncInventory do
  @moduledoc """
  Job for syncing inventory with an external system.

  This job demonstrates error handling and retries.
  """
  use Bedrock.JobQueue.Job,
    topic: "inventory:sync",
    max_retries: 5,
    timeout: 60_000

  @impl true
  def perform(%{product_id: product_id}) do
    IO.puts("[SyncInventory] Syncing product #{product_id}")

    # Simulate occasional failures
    if :rand.uniform(10) > 7 do
      IO.puts("[SyncInventory] Sync failed, will retry...")
      # Return {:error, reason} to trigger a retry
      {:error, :external_api_timeout}
    else
      :ok
    end
  end
end

defmodule ExampleJobs.CleanupOldData do
  @moduledoc """
  Job for cleaning up old data.

  This job demonstrates discarding jobs that shouldn't be retried.
  """
  use Bedrock.JobQueue.Job,
    topic: "cleanup:*",
    priority: 200  # Low priority (processed after higher priority jobs)

  @impl true
  def perform(%{table: table, older_than: older_than}) do
    IO.puts("[CleanupOldData] Cleaning #{table} older than #{older_than}")

    case validate_table(table) do
      :ok ->
        # Perform cleanup
        :ok

      {:error, :invalid_table} ->
        # Return {:discard, reason} to mark as failed without retry
        {:discard, "Table #{table} does not exist"}
    end
  end

  defp validate_table(table) do
    if table in ["users", "orders", "logs"] do
      :ok
    else
      {:error, :invalid_table}
    end
  end
end

defmodule ExampleJobs.RateLimitedJob do
  @moduledoc """
  Job that may need to be snoozed due to rate limiting.

  This job demonstrates the snooze feature.
  """
  use Bedrock.JobQueue.Job,
    topic: "api:call",
    max_retries: 3

  @impl true
  def perform(%{endpoint: endpoint}) do
    IO.puts("[RateLimitedJob] Calling #{endpoint}")

    case check_rate_limit() do
      :ok ->
        # Make the API call
        :ok

      {:rate_limited, retry_after_ms} ->
        IO.puts("[RateLimitedJob] Rate limited, snoozing for #{retry_after_ms}ms")
        # Return {:snooze, delay_ms} to reschedule without counting as a retry
        {:snooze, retry_after_ms}
    end
  end

  defp check_rate_limit do
    # Simulate rate limiting
    if :rand.uniform(10) > 5 do
      {:rate_limited, 5000}
    else
      :ok
    end
  end
end

# =============================================================================
# Step 2: Register Job Handlers
# =============================================================================
#
# Job handlers are registered with topic patterns. The pattern supports wildcards:
# - "email:send" matches only "email:send"
# - "webhook:*" matches "webhook:github", "webhook:stripe", etc.

IO.puts("\n=== Registering Job Handlers ===\n")

JobQueue.register("email:send", ExampleJobs.SendEmail)
JobQueue.register("webhook:*", ExampleJobs.ProcessWebhook)
JobQueue.register("inventory:sync", ExampleJobs.SyncInventory)
JobQueue.register("cleanup:*", ExampleJobs.CleanupOldData)
JobQueue.register("api:call", ExampleJobs.RateLimitedJob)

IO.puts("Registered 5 job handlers")

# =============================================================================
# Step 3: Enqueue Jobs
# =============================================================================
#
# Jobs are enqueued with a queue_id (tenant), topic, and payload.
# The queue_id provides isolation between tenants.

IO.puts("\n=== Enqueuing Jobs ===\n")

# Basic enqueue
{:ok, job1} = JobQueue.enqueue("tenant_1", "email:send", %{
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up."
}, repo: MockRepo)

IO.puts("Enqueued email job: #{inspect(job1.id)}")

# Enqueue with priority
{:ok, job2} = JobQueue.enqueue("tenant_1", "webhook:github", %{
  event: "push",
  payload: %{ref: "refs/heads/main"}
}, priority: 5, repo: MockRepo)

IO.puts("Enqueued webhook job with priority 5: #{inspect(job2.id)}")

# Scheduled enqueue (run at a specific time)
scheduled_time = DateTime.utc_now() |> DateTime.add(3600, :second)
{:ok, job3} = JobQueue.enqueue_at("tenant_1", "cleanup:logs", %{
  table: "logs",
  older_than: "30 days"
}, scheduled_time, repo: MockRepo)

IO.puts("Scheduled cleanup job for #{scheduled_time}: #{inspect(job3.id)}")

# Delayed enqueue (run after a delay)
{:ok, job4} = JobQueue.enqueue_in("tenant_1", "inventory:sync", %{
  product_id: 12345
}, :timer.minutes(5), repo: MockRepo)

IO.puts("Enqueued inventory sync with 5 minute delay: #{inspect(job4.id)}")

# =============================================================================
# Step 4: Creating Jobs from Job Modules
# =============================================================================
#
# Job modules provide a new/2 function for creating jobs with defaults.

IO.puts("\n=== Creating Jobs from Modules ===\n")

# Using the job module's new/2 function
email_job = ExampleJobs.SendEmail.new(
  %{to: "admin@example.com", subject: "Alert", body: "Check this out"},
  queue_id: "tenant_1"
)

IO.puts("Created email job with defaults:")
IO.puts("  Priority: #{email_job.priority}")
IO.puts("  Max retries: #{email_job.max_retries}")
IO.puts("  Topic: #{email_job.topic}")

# =============================================================================
# Step 5: Job Lifecycle
# =============================================================================
#
# Jobs go through the following lifecycle:
#
# 1. PENDING    - Job is waiting in the queue
# 2. PROCESSING - Job is being executed by a worker
# 3. COMPLETED  - Job finished successfully (:ok or {:ok, result})
# 4. REQUEUED   - Job failed and will be retried ({:error, reason})
# 5. DISCARDED  - Job failed permanently ({:discard, reason})
#
# Jobs can also be snoozed ({:snooze, delay_ms}) which reschedules them
# without counting as a retry attempt.

IO.puts("\n=== Job Return Values ===\n")

IO.puts("""
Return Value          | Result
----------------------|-------------------------------------------
:ok                   | Job completed successfully
{:ok, result}         | Job completed with result (logged)
{:error, reason}      | Job failed, will retry if attempts remain
{:discard, reason}    | Job failed permanently, no retry
{:snooze, delay_ms}   | Reschedule without counting as retry
""")

# =============================================================================
# Step 6: Starting the Consumer
# =============================================================================
#
# The consumer processes jobs from the queue. It includes:
# - Scanner: Monitors pointers for ready queues
# - Manager: Dequeues items and dispatches to workers
# - WorkerPool: Pool of workers for parallel processing

IO.puts("\n=== Consumer Configuration ===\n")

IO.puts("""
To start a consumer in your application:

    # In your application.ex or supervisor
    children = [
      # ... other children ...
      {Bedrock.JobQueue.ConsumerSupervisor, []},
    ]

    # Start the consumer
    Bedrock.JobQueue.start_consumer(
      repo: MyApp.Repo,
      concurrency: 10,        # Number of parallel workers
      batch_size: 20          # Jobs to dequeue per batch
    )

Configuration options:
  - :repo        - Bedrock repo module (required)
  - :concurrency - Number of concurrent workers (default: schedulers)
  - :batch_size  - Items per dequeue batch (default: 10)
  - :registry    - Job handler registry (default: Registry.Default)
""")

# =============================================================================
# Summary
# =============================================================================

IO.puts("\n=== Summary ===\n")

IO.puts("""
Bedrock.JobQueue provides a robust job queue system with:

Features:
  - Priority-based processing (lower = higher priority)
  - Scheduled and delayed jobs
  - Automatic retries with exponential backoff
  - Per-tenant queue isolation
  - Wildcard topic patterns for job routing

Key Components:
  - Job        - Behaviour for defining job handlers
  - Store      - Persistent storage operations
  - Consumer   - Scanner/Manager/Worker architecture
  - Registry   - Topic pattern matching for job dispatch

For more details, see the module documentation:
  - Bedrock.JobQueue
  - Bedrock.JobQueue.Job
  - Bedrock.JobQueue.Store
""")
