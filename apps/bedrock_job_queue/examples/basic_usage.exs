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

# =============================================================================
# Step 1: Define Job Modules
# =============================================================================
#
# Jobs are defined as modules that implement the Bedrock.JobQueue.Job behaviour.
# Each job module specifies its topic and default options.

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
  def perform(%{to: to, subject: subject, body: _body}, _meta) do
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
    topic: "webhook:process",
    priority: 10  # Higher priority (lower number = processed first)

  @impl true
  def perform(%{event: event, payload: _payload}, _meta) do
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
  def perform(%{product_id: product_id}, _meta) do
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
    topic: "cleanup:data",
    priority: 200  # Low priority (processed after higher priority jobs)

  @impl true
  def perform(%{table: table, older_than: older_than}, _meta) do
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
  def perform(%{endpoint: endpoint}, _meta) do
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
# Step 2: Define Your JobQueue Module
# =============================================================================
#
# Define a JobQueue module with `use Bedrock.JobQueue`. This configures the
# repo, workers, and provides enqueue functions.

IO.puts("\n=== Defining JobQueue Module ===\n")

defmodule ExampleApp.JobQueue do
  use Bedrock.JobQueue,
    otp_app: :example_app,
    repo: MockRepo,
    workers: %{
      "email:send" => ExampleJobs.SendEmail,
      "webhook:process" => ExampleJobs.ProcessWebhook,
      "inventory:sync" => ExampleJobs.SyncInventory,
      "cleanup:data" => ExampleJobs.CleanupOldData,
      "api:call" => ExampleJobs.RateLimitedJob
    }
end

IO.puts("Defined ExampleApp.JobQueue with 5 workers")

# =============================================================================
# Step 3: Job Lifecycle
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
# Step 4: Supervision Tree Setup
# =============================================================================
#
# Add your JobQueue module to your application's supervision tree.

IO.puts("\n=== Supervision Tree Configuration ===\n")

IO.puts("""
To start the consumer in your application:

    # In your application.ex
    def start(_type, _args) do
      children = [
        # ... your cluster and repo ...
        MyApp.Cluster,
        MyApp.Repo,

        # Add your JobQueue module
        {MyApp.JobQueue, concurrency: 10, batch_size: 5}
      ]

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]
      Supervisor.start_link(children, opts)
    end

Configuration options:
  - :concurrency - Number of concurrent workers (default: schedulers)
  - :batch_size  - Items per dequeue batch (default: 10)
""")

# =============================================================================
# Step 5: Enqueue Jobs
# =============================================================================
#
# Jobs are enqueued with a queue_id (tenant), topic, and payload.
# The queue_id provides isolation between tenants.

IO.puts("\n=== Enqueue API ===\n")

IO.puts("""
Enqueue jobs using your JobQueue module:

    # Basic enqueue
    MyApp.JobQueue.enqueue("tenant_1", "email:send", %{
      to: "user@example.com",
      subject: "Welcome!",
      body: "Thanks for signing up."
    })

    # With priority override
    MyApp.JobQueue.enqueue("tenant_1", "webhook:process", %{
      event: "push",
      payload: %{ref: "refs/heads/main"}
    }, priority: 5)

    # Scheduled for a specific time
    scheduled_time = DateTime.utc_now() |> DateTime.add(3600, :second)
    MyApp.JobQueue.enqueue_at("tenant_1", "cleanup:data", %{
      table: "logs",
      older_than: "30 days"
    }, scheduled_time)

    # Delayed by duration
    MyApp.JobQueue.enqueue_in("tenant_1", "inventory:sync", %{
      product_id: 12345
    }, :timer.minutes(5))
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
  - Module-based worker configuration

Key Components:
  - use Bedrock.JobQueue      - Define your JobQueue module
  - use Bedrock.JobQueue.Job  - Define job handlers
  - Store                     - Persistent storage operations
  - Consumer                  - Scanner/Manager/Worker architecture

For more details, see the module documentation:
  - Bedrock.JobQueue
  - Bedrock.JobQueue.Job
  - Bedrock.JobQueue.Store
""")
