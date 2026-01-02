defmodule Bedrock.JobQueue.Consumer.WorkerPool do
  @moduledoc """
  Dynamic worker pool for job processing.

  Manages a pool of Worker processes up to the configured concurrency limit.
  Workers are started on-demand and terminate after processing each job.
  """

  use DynamicSupervisor

  alias Bedrock.JobQueue.Consumer.Worker

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    max_children = Keyword.get(opts, :concurrency, System.schedulers_online())

    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_children: max_children
    )
  end

  @doc """
  Dispatches a job to a worker.
  """
  @spec dispatch(GenServer.server(), map()) :: {:ok, pid()} | {:error, term()}
  def dispatch(pool, job) do
    child_spec = {Worker, job}
    DynamicSupervisor.start_child(pool, child_spec)
  end

  @doc """
  Returns the number of available worker slots.
  """
  @spec available_workers(GenServer.server()) :: non_neg_integer()
  def available_workers(pool) do
    %{specs: max, active: active} = DynamicSupervisor.count_children(pool)
    max(0, max - active)
  end

  @doc """
  Returns the number of active workers.
  """
  @spec active_workers(GenServer.server()) :: non_neg_integer()
  def active_workers(pool) do
    %{active: active} = DynamicSupervisor.count_children(pool)
    active
  end
end
