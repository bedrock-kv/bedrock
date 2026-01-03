defmodule Bedrock.JobQueue do
  @moduledoc """
  A durable job queue system for Elixir, built on Bedrock.

  Modeled after Apple's [QuiCK paper](https://www.foundationdb.org/files/QuiCK.pdf),
  this system provides:
  - Topic-based routing to worker modules
  - Two-level sharding (per-queue zones + pointer index)
  - Priority ordering and scheduled/delayed jobs
  - Fault-tolerant leasing via vesting time
  - Scanner/Manager/Worker consumer architecture

  ## Quick Start

  Define a JobQueue module for your application:

      defmodule MyApp.JobQueue do
        use Bedrock.JobQueue,
          otp_app: :my_app,
          repo: MyApp.Repo,
          workers: %{
            "user:created" => MyApp.Jobs.UserCreated,
            "email:send" => MyApp.Jobs.SendEmail
          }
      end

  Define job modules:

      defmodule MyApp.Jobs.UserCreated do
        use Bedrock.JobQueue.Job, topic: "user:created"

        @impl true
        def perform(%{user_id: user_id}, meta) do
          # meta.topic, meta.queue_id, meta.item_id, meta.attempt available
          :ok
        end
      end

  Add to your supervision tree:

      children = [
        MyApp.Cluster,
        MyApp.Repo,
        {MyApp.JobQueue, concurrency: 10, batch_size: 5}
      ]

  Enqueue jobs:

      # Immediate processing
      MyApp.JobQueue.enqueue("tenant_1", "user:created", %{user_id: 123})

      # Schedule for a specific time
      MyApp.JobQueue.enqueue("tenant_1", "email:send", payload, at: ~U[2024-01-15 10:00:00Z])

      # Schedule with a delay
      MyApp.JobQueue.enqueue("tenant_1", "cleanup", payload, in: :timer.hours(1))

      # With priority (lower = higher priority)
      MyApp.JobQueue.enqueue("tenant_1", "urgent", payload, priority: 0)
  """

  @type queue_id :: term()
  @type topic :: String.t()
  @type payload :: map() | binary()

  @type enqueue_opts :: [
          at: DateTime.t(),
          in: non_neg_integer(),
          priority: integer(),
          max_retries: non_neg_integer(),
          id: binary()
        ]

  @type config :: %{
          otp_app: atom(),
          repo: module(),
          workers: %{String.t() => module()}
        }

  @doc """
  Defines a JobQueue module.

  ## Options

  - `:otp_app` - The OTP application name (required)
  - `:repo` - The Bedrock Repo module (required)
  - `:workers` - Map of topic strings to job modules (default: %{})

  ## Example

      defmodule MyApp.JobQueue do
        use Bedrock.JobQueue,
          otp_app: :my_app,
          repo: MyApp.Repo,
          workers: %{
            "email:send" => MyApp.Jobs.SendEmail
          }
      end
  """
  defmacro __using__(opts) do
    quote location: :keep do
      alias Bedrock.JobQueue.Internal
      alias Bedrock.JobQueue.Supervisor, as: JobQueueSupervisor

      @otp_app Keyword.fetch!(unquote(opts), :otp_app)
      @repo Keyword.fetch!(unquote(opts), :repo)
      @workers Keyword.get(unquote(opts), :workers, %{})

      @doc """
      Returns a child specification for this JobQueue.

      ## Options

      - `:concurrency` - Number of concurrent workers (default: System.schedulers_online())
      - `:batch_size` - Items to dequeue per batch (default: 10)
      """
      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      @doc """
      Starts the JobQueue consumer supervisor.
      """
      def start_link(opts \\ []) do
        JobQueueSupervisor.start_link(__MODULE__, opts)
      end

      @doc """
      Enqueues a job for processing.

      ## Options

      - `:at` - Schedule for a specific DateTime
      - `:in` - Delay in milliseconds before processing
      - `:priority` - Integer priority (lower = higher priority, default: 100)
      - `:max_retries` - Maximum retry attempts (default: 3)
      - `:id` - Custom job ID (default: auto-generated UUID)

      ## Examples

          # Immediate processing
          MyApp.JobQueue.enqueue("tenant_1", "email:send", %{to: "user@example.com"})

          # Schedule for specific time
          MyApp.JobQueue.enqueue("tenant_1", "reminder", payload, at: ~U[2024-01-15 10:00:00Z])

          # Delay by duration
          MyApp.JobQueue.enqueue("tenant_1", "cleanup", payload, in: :timer.hours(1))

          # With priority
          MyApp.JobQueue.enqueue("tenant_1", "urgent", payload, priority: 0)
      """
      def enqueue(queue_id, topic, payload, opts \\ []) do
        Internal.enqueue(__MODULE__, queue_id, topic, payload, opts)
      end

      @doc """
      Gets queue statistics.

      Returns a map with `:pending_count` and `:processing_count`.
      """
      def stats(queue_id, opts \\ []) do
        Internal.stats(__MODULE__, queue_id, opts)
      end

      @doc false
      def __config__ do
        %{
          otp_app: @otp_app,
          repo: @repo,
          workers: @workers
        }
      end
    end
  end
end
