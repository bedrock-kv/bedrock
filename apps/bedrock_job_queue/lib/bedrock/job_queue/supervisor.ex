defmodule Bedrock.JobQueue.Supervisor do
  @moduledoc """
  Supervises the consumer tree for a JobQueue module.

  This supervisor is started by the JobQueue module's `start_link/1` function
  and manages the Consumer supervision tree (Scanner, Manager, Worker pool).
  """

  use Supervisor

  @doc """
  Starts the supervisor for the given JobQueue module.

  ## Options

  - `:concurrency` - Number of concurrent workers (default: System.schedulers_online())
  - `:batch_size` - Items to dequeue per batch (default: 10)
  """
  def start_link(job_queue_module, opts \\ []) do
    Supervisor.start_link(__MODULE__, {job_queue_module, opts}, name: job_queue_module)
  end

  @impl true
  def init({job_queue_module, opts}) do
    config = job_queue_module.__config__()

    children = [
      {Bedrock.JobQueue.Consumer,
       job_queue: job_queue_module,
       repo: config.repo,
       workers: config.workers,
       concurrency: Keyword.get(opts, :concurrency, System.schedulers_online()),
       batch_size: Keyword.get(opts, :batch_size, 10)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
