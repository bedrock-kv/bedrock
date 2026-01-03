defmodule Bedrock.JobQueue.Internal do
  @moduledoc """
  Internal implementation for JobQueue operations.

  This module provides the backing implementation for functions defined by
  `use Bedrock.JobQueue`. All functions accept a job_queue_module as the first
  argument and use its `__config__/0` to get repo, workers, etc.

  While technically public, these functions are meant to be called through
  the generated JobQueue module rather than directly.
  """

  alias Bedrock.Directory
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace
  alias Bedrock.ToKeyspace

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
    now = Keyword.get(opts, :now) || System.system_time(:millisecond)
    opts = process_scheduling_opts(opts, now)
    item = Item.new(queue_id, topic, payload, opts)

    config.repo.transact(fn ->
      Store.enqueue(config.repo, root, item, now: now)
      {:ok, item}
    end)
  end

  defp process_scheduling_opts(opts, now) do
    cond do
      scheduled_at = Keyword.get(opts, :at) ->
        vesting_time = DateTime.to_unix(scheduled_at, :millisecond)
        opts |> Keyword.delete(:at) |> Keyword.put(:vesting_time, vesting_time)

      delay_ms = Keyword.get(opts, :in) ->
        vesting_time = now + delay_ms
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
  Initializes the root directory for the JobQueue module.

  Creates or opens the directory and caches the resulting keyspace in persistent_term.
  Must be called within a transaction (or will create one).

  Returns `{:ok, keyspace}` on success.
  """
  def init_root(repo, job_queue_module) do
    module_name = job_queue_module |> Module.split() |> Enum.join("_") |> String.downcase()

    repo.transact(fn ->
      root = Directory.root(repo)

      with {:ok, job_queue_dir} <- Directory.create_or_open(root, ["job_queue"]),
           {:ok, node} <- Directory.create_or_open(job_queue_dir, [module_name]) do
        keyspace = ToKeyspace.to_keyspace(node)
        :persistent_term.put({__MODULE__, job_queue_module}, keyspace)
        {:ok, keyspace}
      end
    end)
  end

  @doc """
  Returns the root keyspace for the given JobQueue module.

  Reads from cache (populated by init_root/2 during startup).
  Falls back to creating a Keyspace from directory path if not cached.
  """
  def root_keyspace(job_queue_module) do
    case :persistent_term.get({__MODULE__, job_queue_module}, nil) do
      nil ->
        # Fallback for cases where init_root wasn't called (e.g., direct tests)
        module_name = job_queue_module |> Module.split() |> Enum.join("_") |> String.downcase()
        Keyspace.new("job_queue/#{module_name}/")

      keyspace ->
        keyspace
    end
  end
end
