defmodule Bedrock.Service.Foreman do
  @moduledoc """
  Manages worker processes and service lifecycle operations.
  """
  use Bedrock.Internal.GenServerApi, for: Bedrock.Service.Foreman.Supervisor

  alias Bedrock.Service.Worker

  @type ref :: pid() | atom() | {atom(), node()}

  @type health :: :ok | {:failed_to_start, :at_least_one_failed_to_start} | :unknown | :starting

  @spec config_key() :: atom()
  def config_key, do: :worker

  @doc """
  Return a list of running workers.
  """
  @spec all(foreman :: ref(), opts :: [timeout: timeout()]) ::
          {:ok, [Worker.ref()]} | {:error, :unavailable | :timeout | :unknown}
  def all(foreman, opts \\ []), do: call(foreman, :workers, opts[:timeout] || :infinity)

  @doc """
  Create a new worker.
  """
  @spec new_worker(
          foreman :: ref(),
          id :: Worker.id(),
          kind :: :log | :materializer,
          opts :: [timeout: timeout()]
        ) ::
          {:ok, Worker.ref()} | {:error, :timeout}
  def new_worker(foreman, id, kind, opts \\ []), do: call(foreman, {:new_worker, id, kind}, opts[:timeout] || :infinity)

  @doc """
  Return a list of running materializer workers only.
  """
  @spec materializer_workers(foreman :: ref(), opts :: [timeout: timeout()]) ::
          {:ok, [Worker.ref()]} | {:error, :unavailable | :timeout | :unknown}
  def materializer_workers(foreman, opts \\ []), do: call(foreman, :materializer_workers, opts[:timeout] || :infinity)

  @doc """
  Wait until the foreman signals that it (and all of it's workers) are
  reporting that they are healthy, or the timeout happens... whichever comes
  first.
  """
  @spec wait_for_healthy(foreman :: ref(), opts :: [timeout: timeout()]) ::
          :ok | {:error, :unavailable | :timeout | :unknown}
  def wait_for_healthy(foreman, opts \\ []), do: call(foreman, :wait_for_healthy, opts[:timeout] || :infinity)

  @doc """
  Remove a worker and clean up its resources.

  This will:
  1. Terminate the worker process
  2. Remove it from the supervisor
  3. Clean up its working directory
  4. Remove it from foreman state
  """
  @spec remove_worker(
          foreman :: ref(),
          Worker.id(),
          opts :: [{:timeout, timeout()}]
        ) ::
          :ok
          | {:error, :worker_not_found}
          | {:error, {:failed_to_remove_directory, File.posix(), Path.t()}}
          | {:error, :unavailable | :timeout | :unknown}
  def remove_worker(foreman, worker_id, opts \\ []),
    do: call(foreman, {:remove_worker, worker_id}, opts[:timeout] || 5_000)

  @doc """
  Remove multiple workers in a single batch operation.

  This is more efficient than calling remove_worker/3 multiple times
  as it processes all workers in one foreman call.

  Returns a map of results where successful removals are `:ok` and
  failures include the error reason.
  """
  @spec remove_workers(
          foreman_ref :: ref(),
          worker_ids :: [Worker.id()],
          opts :: [{:timeout, timeout()}]
        ) ::
          %{Worker.id() => :ok | {:error, term()}}
          | {:error, :unavailable | :timeout | :unknown}
  def remove_workers(foreman, worker_ids, opts \\ []),
    do: call(foreman, {:remove_workers, worker_ids}, opts[:timeout] || 30_000)

  @doc """
  Called by a worker to report it's health to the foreman.
  """
  @spec report_health(
          foreman :: ref(),
          Worker.id(),
          Worker.health()
        ) :: :ok
  def report_health(foreman, worker_id, health), do: cast(foreman, {:worker_health, worker_id, health})

  @doc """
  Return a list of all running services with information needed for coordinator registration.

  Each service is returned as a compact tuple of {service_id, kind, name} where:
  - service_id is the real service identifier from the worker's manifest
  - kind is :log or :materializer
  - name is the OTP name atom for the worker

  The coordinator will expand this to full service info using node(from) and the provided service_id.
  """
  @spec get_all_running_services(foreman :: ref(), opts :: [timeout: timeout()]) ::
          {:ok, [{service_id :: String.t(), kind :: :log | :materializer, name :: atom()}]}
          | {:error, :unavailable | :timeout | :unknown}
  def get_all_running_services(foreman, opts \\ []),
    do: call(foreman, :get_all_running_services, opts[:timeout] || :infinity)
end
