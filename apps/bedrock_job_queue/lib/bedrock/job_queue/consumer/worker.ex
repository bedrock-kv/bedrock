defmodule Bedrock.JobQueue.Consumer.Worker do
  @moduledoc """
  Job execution logic.

  Provides the execute/2 function that runs job modules' perform/2 callbacks
  with timeout protection. Used directly by Manager via Task.Supervisor.

  Workers are configured via the JobQueue module's workers map:

      defmodule MyApp.JobQueue do
        use Bedrock.JobQueue,
          otp_app: :my_app,
          repo: MyApp.Repo,
          workers: %{
            "email:send" => MyApp.Jobs.SendEmail,
            "order:process" => MyApp.Jobs.ProcessOrder
          }
      end
  """

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Payload

  require Logger

  @doc """
  Executes a job and returns the result.

  Called from a Task spawned by Manager. Looks up the handler for the item's
  topic from the workers map and executes it with timeout protection.

  ## Return Values

  Returns the result from the job module's `perform/2` callback, or an error:

  - `:ok` - Job completed successfully, will be removed from queue
  - `{:ok, result}` - Job completed with result (logged but otherwise same as `:ok`)
  - `{:error, reason}` - Job failed, will be requeued with backoff
  - `{:discard, reason}` - Job failed permanently, removed without retry
  - `{:snooze, delay_ms}` - Reschedule for later without counting as failure
  - `{:error, :timeout}` - Job exceeded timeout, will be requeued
  - `{:discard, :no_handler}` - No worker configured for this topic

  ## Timeout

  Jobs are executed with a timeout (default 30 seconds). If the job module
  implements `timeout/0`, that value is used instead. On timeout, the job
  is killed and `{:error, :timeout}` is returned.
  """
  @spec execute(Item.t(), map()) :: term()
  def execute(%Item{} = item, workers) when is_map(workers) do
    case Map.get(workers, item.topic) do
      nil ->
        Logger.warning("No worker configured for topic: #{item.topic}")
        {:discard, :no_handler}

      job_module ->
        execute_with_timeout(job_module, item)
    end
  end

  defp execute_with_timeout(job_module, item) do
    timeout = get_timeout(job_module)
    payload = Payload.decode(item.payload)

    meta = %{
      topic: item.topic,
      queue_id: item.queue_id,
      item_id: item.id,
      attempt: item.error_count + 1
    }

    task =
      Task.async(fn ->
        job_module.perform(payload, meta)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        result

      nil ->
        {:error, :timeout}

      {:exit, reason} ->
        {:error, {:exit, reason}}
    end
  rescue
    e ->
      Logger.error("Job execution failed: #{Exception.message(e)}")
      {:error, {:exception, e}}
  end

  defp get_timeout(job_module) do
    if function_exported?(job_module, :timeout, 0) do
      job_module.timeout()
    else
      30_000
    end
  end
end
