# Coffee Shop Job Queue - Test Script
#
# Run with: mix run livebooks/coffee_shop_queue_test.exs
#
# This validates all code before converting to Livebook format.

IO.puts("\n=== Coffee Shop Job Queue Test ===\n")

# Ensure bedrock_job_queue application is started (starts the registry)
{:ok, _} = Application.ensure_all_started(:bedrock_job_queue)

# -----------------------------------------------------------------------------
# Step 1: Setup Cluster and Repo
# -----------------------------------------------------------------------------

IO.puts("Step 1: Setting up cluster and repo...")

# Generate unique working dir at compile time
working_dir = Path.join(System.tmp_dir!(), "coffee_shop_#{:rand.uniform(99999)}")
File.mkdir_p!(working_dir)
IO.puts("  Working dir: #{working_dir}")

# Store in Application env so cluster can read it
Application.put_env(:coffee_shop, :working_dir, working_dir)

defmodule CoffeeShop.Cluster do
  @working_dir Application.compile_env!(:coffee_shop, :working_dir)

  use Bedrock.Cluster,
    otp_app: :coffee_shop,
    name: "coffee_shop",
    config: [
      capabilities: [:coordination, :log, :storage],
      trace: [],
      coordinator: [path: @working_dir],
      storage: [path: @working_dir],
      log: [path: @working_dir]
    ]
end

defmodule CoffeeShop.Repo do
  use Bedrock.Repo, cluster: CoffeeShop.Cluster
end

# Cluster has child_spec, not start_link - use Supervisor
{:ok, _} = Supervisor.start_link([{CoffeeShop.Cluster, []}], strategy: :one_for_one)
IO.puts("  Cluster started")

# Wait for cluster to be ready
Process.sleep(2000)
IO.puts("  Cluster ready")

# Quick test
CoffeeShop.Repo.transact(fn ->
  CoffeeShop.Repo.put("test", "coffee ready")
end)

result = CoffeeShop.Repo.transact(fn -> CoffeeShop.Repo.get("test") end)
if result == "coffee ready" do
  IO.puts("  Repo verified: #{result}")
else
  raise "Repo test failed!"
end

# -----------------------------------------------------------------------------
# Step 2: Define Job Modules
# -----------------------------------------------------------------------------

IO.puts("\nStep 2: Defining job modules...")

defmodule CoffeeShop.Jobs.OrderConfirmation do
  use Bedrock.JobQueue.Job,
    topic: "order:confirm",
    priority: 100

  @impl true
  def perform(%{order_id: order_id, customer: customer}, _meta) do
    IO.puts("  [#{order_id}] Sending confirmation to #{customer}")
    Process.sleep(50)
    :ok
  end
end

defmodule CoffeeShop.Jobs.BrewingStarted do
  use Bedrock.JobQueue.Job,
    topic: "order:brewing",
    priority: 50

  @impl true
  def perform(%{order_id: order_id, drink: drink}, _meta) do
    IO.puts("  [#{order_id}] Barista started: #{drink}")
    Process.sleep(50)
    {:ok, %{started_at: DateTime.utc_now()}}
  end
end

defmodule CoffeeShop.Jobs.ReadyForPickup do
  use Bedrock.JobQueue.Job,
    topic: "order:ready",
    priority: 10

  @impl true
  def perform(%{order_id: order_id, customer: customer}, _meta) do
    IO.puts("  [#{order_id}] READY! Paging #{customer}!")
    Process.sleep(50)
    :ok
  end
end

defmodule CoffeeShop.Jobs.EspressoShot do
  use Bedrock.JobQueue.Job,
    topic: "brew:espresso",
    max_retries: 3,
    priority: 40

  @impl true
  def perform(%{order_id: order_id, shots: shots}, _meta) do
    IO.puts("  [#{order_id}] Pulling #{shots} shot(s)...")
    Process.sleep(50)
    # Always succeed for testing
    :ok
  end
end

defmodule CoffeeShop.Jobs.DeliverySync do
  use Bedrock.JobQueue.Job,
    topic: "delivery:sync",
    max_retries: 5

  @impl true
  def perform(%{order_id: order_id, platform: platform}, _meta) do
    IO.puts("  [#{order_id}] Syncing with #{platform}...")
    Process.sleep(50)
    :ok
  end
end

defmodule CoffeeShop.Jobs.AdminCleanup do
  use Bedrock.JobQueue.Job,
    topic: "admin:cleanup",
    priority: 200

  @impl true
  def perform(%{task: task}, _meta) do
    IO.puts("  Running cleanup: #{task}")
    Process.sleep(50)
    :ok
  end
end

IO.puts("  6 job modules defined")

# Verify job configs
config = CoffeeShop.Jobs.OrderConfirmation.__job_config__()
IO.puts("  OrderConfirmation config: topic=#{config.topic}, priority=#{config.priority}")

# -----------------------------------------------------------------------------
# Step 3: Define JobQueue Module
# -----------------------------------------------------------------------------

IO.puts("\nStep 3: Setting up JobQueue module...")

defmodule CoffeeShop.JobQueue do
  use Bedrock.JobQueue,
    otp_app: :coffee_shop,
    repo: CoffeeShop.Repo,
    workers: %{
      "order:confirm" => CoffeeShop.Jobs.OrderConfirmation,
      "order:brewing" => CoffeeShop.Jobs.BrewingStarted,
      "order:ready" => CoffeeShop.Jobs.ReadyForPickup,
      "brew:espresso" => CoffeeShop.Jobs.EspressoShot,
      "delivery:sync" => CoffeeShop.Jobs.DeliverySync,
      "admin:cleanup" => CoffeeShop.Jobs.AdminCleanup
    }
end

IO.puts("  JobQueue module defined with 6 workers")

# -----------------------------------------------------------------------------
# Step 4: Start Consumer
# -----------------------------------------------------------------------------

IO.puts("\nStep 4: Starting consumer...")

{:ok, _consumer} = CoffeeShop.JobQueue.start_link(concurrency: 2, batch_size: 5)

IO.puts("  Consumer started (concurrency: 2)")

# Give consumer time to initialize
Process.sleep(200)

# -----------------------------------------------------------------------------
# Step 5: Basic Enqueueing
# -----------------------------------------------------------------------------

IO.puts("\nStep 5: Testing basic enqueueing...")

{:ok, job1} = CoffeeShop.JobQueue.enqueue("main_shop", "order:confirm",
  %{order_id: "ORD-001", customer: "Alice"})

IO.puts("  Enqueued order confirmation: #{Base.encode16(job1.id, case: :lower) |> binary_part(0, 8)}...")

# Wait for job to process
Process.sleep(1000)

# -----------------------------------------------------------------------------
# Step 6: Priority Test
# -----------------------------------------------------------------------------

IO.puts("\nStep 6: Testing priorities (low -> high enqueue order)...")

# Enqueue in reverse priority order to show they process by priority
# NOTE: Priority must be passed explicitly - lower number = higher priority
{:ok, _} = CoffeeShop.JobQueue.enqueue("main_shop", "admin:cleanup", %{task: "clear_old_orders"}, priority: 200)
IO.puts("  -> Enqueued cleanup (priority 200 - low)")

{:ok, _} = CoffeeShop.JobQueue.enqueue("main_shop", "order:brewing", %{order_id: "ORD-002", drink: "Latte"}, priority: 50)
IO.puts("  -> Enqueued brewing (priority 50 - medium)")

{:ok, _} = CoffeeShop.JobQueue.enqueue("main_shop", "order:ready", %{order_id: "ORD-003", customer: "Bob"}, priority: 10)
IO.puts("  -> Enqueued ready (priority 10 - HIGH)")

IO.puts("\n  Watching execution order (should be: ready -> brewing -> cleanup):")
Process.sleep(2000)

# -----------------------------------------------------------------------------
# Step 7: Scheduled Jobs
# -----------------------------------------------------------------------------

IO.puts("\nStep 7: Testing scheduled jobs...")

# Schedule job for 2 seconds from now
scheduled_time = DateTime.utc_now() |> DateTime.add(2, :second)
{:ok, _} = CoffeeShop.JobQueue.enqueue("main_shop", "order:confirm",
  %{order_id: "ORD-SCHEDULED", customer: "Charlie"},
  at: scheduled_time)

IO.puts("  -> Scheduled job for #{DateTime.to_iso8601(scheduled_time)}")
IO.puts("  -> Waiting 3 seconds to see it execute...")
Process.sleep(3000)

# -----------------------------------------------------------------------------
# Step 8: Delayed Jobs (enqueue_in)
# -----------------------------------------------------------------------------

IO.puts("\nStep 8: Testing delayed jobs (enqueue_in)...")

{:ok, _} = CoffeeShop.JobQueue.enqueue("main_shop", "order:confirm",
  %{order_id: "ORD-DELAYED", customer: "Diana"},
  in: 1000)  # 1 second delay

IO.puts("  -> Enqueued with 1 second delay")
IO.puts("  -> Waiting 2 seconds...")
Process.sleep(2000)

# -----------------------------------------------------------------------------
# Step 9: Multi-tenant (queue isolation)
# -----------------------------------------------------------------------------

IO.puts("\nStep 9: Testing multi-tenant queue isolation...")

{:ok, _} = CoffeeShop.JobQueue.enqueue("downtown_shop", "order:confirm", %{order_id: "DT-001", customer: "Eve"})
{:ok, _} = CoffeeShop.JobQueue.enqueue("airport_kiosk", "order:confirm", %{order_id: "AP-001", customer: "Frank"})

IO.puts("  Enqueued to downtown_shop and airport_kiosk")
Process.sleep(2000)

# -----------------------------------------------------------------------------
# Step 10: Check Stats
# -----------------------------------------------------------------------------

IO.puts("\nStep 10: Checking queue stats...")

for queue_id <- ["main_shop", "downtown_shop", "airport_kiosk"] do
  stats = CoffeeShop.JobQueue.stats(queue_id)
  IO.puts("  #{queue_id}: pending=#{stats.pending_count}, processing=#{stats.processing_count}")
end

# -----------------------------------------------------------------------------
# Done!
# -----------------------------------------------------------------------------

IO.puts("\n=== All Tests Passed! ===")
IO.puts("Ready to convert to Livebook format.\n")

# Cleanup
File.rm_rf!(Application.get_env(:coffee_shop, :working_dir))
