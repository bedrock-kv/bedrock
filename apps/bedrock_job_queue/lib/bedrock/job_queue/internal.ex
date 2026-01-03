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
  """
  def enqueue(job_queue_module, queue_id, topic, payload, opts) do
    config = job_queue_module.__config__()
    root = root_keyspace(job_queue_module)
    item = Item.new(queue_id, topic, payload, opts)

    config.repo.transact(fn ->
      Store.enqueue(config.repo, root, item)
      {:ok, item}
    end)
  end

  @doc """
  Enqueues a job scheduled for a specific time.
  """
  def enqueue_at(job_queue_module, queue_id, topic, payload, %DateTime{} = scheduled_at, opts) do
    vesting_time = DateTime.to_unix(scheduled_at, :millisecond)
    enqueue(job_queue_module, queue_id, topic, payload, Keyword.put(opts, :vesting_time, vesting_time))
  end

  @doc """
  Enqueues a job with a delay.
  """
  def enqueue_in(job_queue_module, queue_id, topic, payload, delay_ms, opts) when is_integer(delay_ms) do
    vesting_time = System.system_time(:millisecond) + delay_ms
    enqueue(job_queue_module, queue_id, topic, payload, Keyword.put(opts, :vesting_time, vesting_time))
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

  # For now, use a simple string-based keyspace.
  # TODO: Migrate to Directory layer for tighter key sizes.
  defp root_keyspace(job_queue_module) do
    # Use the module name as part of the keyspace to isolate different JobQueue modules
    module_name = job_queue_module |> Module.split() |> Enum.join("_") |> String.downcase()
    Keyspace.new("job_queue/#{module_name}/")
  end
end
