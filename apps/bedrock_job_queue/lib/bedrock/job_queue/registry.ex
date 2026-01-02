defmodule Bedrock.JobQueue.Registry do
  @moduledoc """
  Registry for mapping topic patterns to job modules.

  Supports wildcard patterns: `"user:*"` matches `"user:created"`, `"user:deleted"`, etc.
  """

  @doc """
  Registers a job module for a topic pattern.
  """
  @spec register(atom(), String.t(), module()) :: :ok | {:error, term()}
  def register(registry, topic_pattern, job_module) do
    Registry.register(registry, topic_pattern, job_module)
    :ok
  rescue
    e -> {:error, e}
  end

  @doc """
  Finds the job module for a given topic.
  """
  @spec lookup(atom(), String.t()) :: {:ok, module()} | :error
  def lookup(registry, topic) do
    registry
    |> Registry.select([{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.find_value(:error, fn {pattern, module} ->
      if matches_pattern?(pattern, topic) do
        {:ok, module}
      end
    end)
  end

  @doc """
  Lists all registered patterns and their job modules.
  """
  @spec list(atom()) :: [{String.t(), module()}]
  def list(registry) do
    Registry.select(registry, [{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
  end

  @doc """
  Checks if a topic matches a pattern.

  Supports wildcards:
  - `"user:*"` matches `"user:created"`, `"user:deleted"`
  - `"*"` matches everything
  - Exact patterns match exactly
  """
  @spec matches_pattern?(String.t(), String.t()) :: boolean()
  def matches_pattern?(pattern, topic) do
    pattern_parts = String.split(pattern, ":")
    topic_parts = String.split(topic, ":")

    match_parts(pattern_parts, topic_parts)
  end

  defp match_parts([], []), do: true
  defp match_parts(["*"], _rest), do: true
  defp match_parts(["*" | _], _), do: true

  defp match_parts([p | pattern_rest], [t | topic_rest]) when p == t do
    match_parts(pattern_rest, topic_rest)
  end

  defp match_parts(_, _), do: false
end
