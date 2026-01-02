defmodule Bedrock.JobQueue.Consumer.Worker do
  @moduledoc """
  Job execution logic.

  Provides the execute/3 function that runs job modules' perform/1 callbacks
  with timeout protection. Used directly by Manager via Task.Supervisor.
  """

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Payload

  require Logger

  @doc """
  Executes a job and returns the result.

  Called from a Task spawned by Manager. Looks up the handler for the item's
  topic and executes it with timeout protection.
  """
  @spec execute(Item.t(), module() | atom()) :: term()
  def execute(%Item{} = item, registry) do
    case lookup_handler(registry, item.topic) do
      {:ok, job_module} ->
        execute_with_timeout(job_module, item)

      :error ->
        Logger.warning("No handler registered for topic: #{item.topic}")
        {:discard, :no_handler}
    end
  end

  # Finds the job module registered for a topic pattern
  defp lookup_handler(registry, topic) do
    registry
    |> Registry.select([{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.find_value(:error, fn {pattern, module} ->
      if matches_pattern?(pattern, topic), do: {:ok, module}
    end)
  end

  # Checks if a topic matches a pattern (supports wildcards)
  defp matches_pattern?(pattern, topic) do
    pattern_parts = String.split(pattern, ":")
    topic_parts = String.split(topic, ":")
    match_parts(pattern_parts, topic_parts)
  end

  defp match_parts([], []), do: true
  defp match_parts(["*"], _rest), do: true
  defp match_parts(["*" | _], _), do: true
  defp match_parts([p | pattern_rest], [t | topic_rest]) when p == t, do: match_parts(pattern_rest, topic_rest)
  defp match_parts(_, _), do: false

  defp execute_with_timeout(job_module, item) do
    timeout = get_timeout(job_module)
    args = Payload.decode(item.payload)

    task =
      Task.async(fn ->
        job_module.perform(args)
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
