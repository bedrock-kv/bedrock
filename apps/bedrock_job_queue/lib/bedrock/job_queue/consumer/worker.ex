defmodule Bedrock.JobQueue.Consumer.Worker do
  @moduledoc """
  Job execution logic.

  Provides the execute/2 function that runs job modules' perform/2 callbacks
  with timeout protection. Used directly by Manager via Task.Supervisor.

  Workers are configured via static mapping in config:

      config :bedrock_job_queue, :workers, %{
        "email:send" => MyApp.Jobs.SendEmail,
        "order:process" => MyApp.Jobs.ProcessOrder
      }
  """

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Payload

  require Logger

  @doc """
  Executes a job and returns the result.

  Called from a Task spawned by Manager. Looks up the handler for the item's
  topic from static config and executes it with timeout protection.
  """
  @spec execute(Item.t()) :: term()
  def execute(%Item{} = item) do
    case lookup_handler(item.topic) do
      {:ok, job_module} ->
        execute_with_timeout(job_module, item)

      :error ->
        Logger.warning("No worker configured for topic: #{item.topic}")
        {:discard, :no_handler}
    end
  end

  # Looks up worker module from static config
  defp lookup_handler(topic) do
    workers = Application.get_env(:bedrock_job_queue, :workers, %{})

    case Map.get(workers, topic) do
      nil -> :error
      module -> {:ok, module}
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
