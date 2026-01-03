defmodule Bedrock.JobQueue.Internal do
  @moduledoc false

  # Internal implementation for JobQueue operations.
  # All functions accept a job_queue_module as the first argument
  # and use its __config__ to get repo, workers, etc.

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

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
      enqueue(MyQueue, "tenant", "topic", payload)

      # Schedule for specific time
      enqueue(MyQueue, "tenant", "topic", payload, at: ~U[2024-01-15 10:00:00Z])

      # Delay by duration
      enqueue(MyQueue, "tenant", "topic", payload, in: :timer.hours(1))

      # With priority
      enqueue(MyQueue, "tenant", "topic", payload, priority: 0)
  """
  def enqueue(job_queue_module, queue_id, topic, payload, opts) do
    config = job_queue_module.__config__()
    root = root_keyspace(job_queue_module)
    opts = process_scheduling_opts(opts)
    item = Item.new(queue_id, topic, payload, opts)

    config.repo.transact(fn ->
      Store.enqueue(config.repo, root, item)
      {:ok, item}
    end)
  end

  defp process_scheduling_opts(opts) do
    cond do
      scheduled_at = Keyword.get(opts, :at) ->
        vesting_time = DateTime.to_unix(scheduled_at, :millisecond)
        opts |> Keyword.delete(:at) |> Keyword.put(:vesting_time, vesting_time)

      delay_ms = Keyword.get(opts, :in) ->
        vesting_time = System.system_time(:millisecond) + delay_ms
        opts |> Keyword.delete(:in) |> Keyword.put(:vesting_time, vesting_time)

      true ->
        opts
    end
  end

  @doc """
  Gets queue statistics.
  """
  def stats(job_queue_module, queue_id, _opts) do
    config = job_queue_module.__config__()
    root = root_keyspace(job_queue_module)

    fn ->
      {:ok, Store.stats(config.repo, root, queue_id)}
    end
    |> config.repo.transact()
    |> case do
      {:ok, stats} -> stats
      error -> error
    end
  end

  @doc """
  Returns the root keyspace for the given JobQueue module.

  Used by both Internal (for enqueue/stats) and Supervisor (for Consumer).
  """
  def root_keyspace(job_queue_module) do
    # Use the module name as part of the keyspace to isolate different JobQueue modules
    module_name = job_queue_module |> Module.split() |> Enum.join("_") |> String.downcase()
    Keyspace.new("job_queue/#{module_name}/")
  end
end
