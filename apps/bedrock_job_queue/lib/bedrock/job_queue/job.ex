defmodule Bedrock.JobQueue.Job do
  @moduledoc """
  Behaviour for job modules (similar to Oban.Worker).

  ## Usage

      defmodule MyApp.EmailJob do
        use Bedrock.JobQueue.Job,
          topic: "email:*",
          max_retries: 5,
          priority: 10

        @impl true
        def perform(%{to: to, subject: subject, body: body}) do
          # Send email
          :ok
        end
      end

  ## Options

  - `:topic` - The topic pattern this job handles (optional, used for auto-registration)
  - `:max_retries` - Maximum retry attempts (default: 3)
  - `:priority` - Default priority for jobs created by this module (default: 100)
  - `:timeout` - Job execution timeout in milliseconds (default: 30_000)

  ## Return Values

  The `perform/1` callback should return:

  - `:ok` - Job completed successfully
  - `{:ok, result}` - Job completed with a result (logged but otherwise ignored)
  - `{:error, reason}` - Job failed, will be retried if attempts remain
  - `{:discard, reason}` - Job failed permanently, won't be retried
  - `{:snooze, delay_ms}` - Reschedule job for later without counting as a retry
  """

  @type result ::
          :ok
          | {:ok, term()}
          | {:error, term()}
          | {:discard, term()}
          | {:snooze, non_neg_integer()}

  @doc """
  Performs the job with the given arguments.
  """
  @callback perform(args :: map()) :: result()

  @doc """
  Returns the job execution timeout in milliseconds.
  """
  @callback timeout() :: pos_integer()

  @optional_callbacks [timeout: 0]

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour Bedrock.JobQueue.Job

      @__job_topic__ Keyword.get(unquote(opts), :topic)
      @__job_max_retries__ Keyword.get(unquote(opts), :max_retries, 3)
      @__job_priority__ Keyword.get(unquote(opts), :priority, 100)
      @__job_timeout__ Keyword.get(unquote(opts), :timeout, 30_000)

      @doc false
      def __job_config__ do
        %{
          topic: @__job_topic__,
          max_retries: @__job_max_retries__,
          priority: @__job_priority__,
          timeout: @__job_timeout__
        }
      end

      @doc false
      def timeout, do: @__job_timeout__

      defoverridable timeout: 0

      @doc """
      Creates a new job to be enqueued.

      ## Examples

          MyApp.EmailJob.new(%{to: "user@example.com", subject: "Hello"})
          |> Bedrock.JobQueue.enqueue("tenant_1")
      """
      @spec new(map(), keyword()) :: Bedrock.JobQueue.Item.t()
      def new(args, opts \\ []) when is_map(args) do
        alias Bedrock.JobQueue.Item

        opts =
          opts
          |> Keyword.put_new(:priority, @__job_priority__)
          |> Keyword.put_new(:max_retries, @__job_max_retries__)

        topic = Keyword.get(opts, :topic, @__job_topic__) || raise "No topic specified"
        queue_id = Keyword.fetch!(opts, :queue_id)

        Item.new(queue_id, topic, args, opts)
      end
    end
  end
end
