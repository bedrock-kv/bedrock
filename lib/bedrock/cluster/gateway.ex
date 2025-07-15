defmodule Bedrock.Cluster.Gateway do
  @moduledoc """
  The `Bedrock.Cluster.Gateway` is the interface to a GenServer that is
  responsible for finding and holding the current coordinator and director for
  a cluster.
  """

  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.server()

  @spec begin_transaction(
          gateway_ref :: ref(),
          opts :: [
            retry_count: pos_integer(),
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: {:ok, transaction_pid :: pid()} | {:error, :timeout}
  def begin_transaction(gateway, opts \\ []),
    do: gateway |> call({:begin_transaction, opts}, opts[:timeout_in_ms] || :infinity)

  @spec next_read_version(
          gateway_ref :: ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, read_version :: Bedrock.version(), lease_deadline_ms :: Bedrock.interval_in_ms()}
          | {:error, :unavailable}
  def next_read_version(gateway, opts \\ []),
    do: gateway |> call(:next_read_version, opts[:timeout_in_ms] || :infinity)

  @doc """
  Renew the lease for a transaction based on the read version.
  """
  @spec renew_read_version_lease(
          gateway_ref :: ref(),
          read_version :: Bedrock.version(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, lease_deadline_ms :: Bedrock.interval_in_ms()} | {:error, :lease_expired}
  def renew_read_version_lease(t, read_version, opts \\ []),
    do: t |> call({:renew_read_version_lease, read_version}, opts[:timeout_in_ms] || :infinity)

  @doc """
  Get a coordinator for the cluster. We ask the running instance of the cluster
  gateway to find one for us.
  """
  @spec fetch_coordinator(gateway_ref :: ref(), timeout_ms :: Bedrock.timeout_in_ms()) ::
          {:ok, coordinator_ref :: Coordinator.ref()} | {:error, :unavailable}
  def fetch_coordinator(gateway, timeout_in_ms \\ 5_000),
    do: gateway |> call(:fetch_coordinator, timeout_in_ms)

  @doc """
  Get the current director for the cluster.
  """
  @spec fetch_director(gateway :: ref(), Bedrock.timeout_in_ms()) ::
          {:ok, Director.ref()} | {:error, :unavailable}
  def fetch_director(gateway, timeout_in_ms \\ 5_000),
    do: gateway |> call(:fetch_director, timeout_in_ms)

  @doc """
  Get one of the current commit proxies
  """
  @spec fetch_commit_proxy(gateway :: ref(), Bedrock.timeout_in_ms()) ::
          {:ok, Director.ref()} | {:error, :unavailable}
  def fetch_commit_proxy(gateway, timeout_in_ms \\ 5_000),
    do: gateway |> call(:fetch_commit_proxy, timeout_in_ms)

  @doc """
  Retrieve the list of nodes currently running the coordinator process for the
  cluster. If successful, it returns a tuple with `:ok` and the list of nodes;
  otherwise, it returns `{:error, :unavailable}` if unable to retrieve the
  information.

  ## Parameters
    - `gateway`: The reference to the GenServer instance of the cluster gateway.

  ## Returns
    - `{:ok, [node()]}`: A tuple containing `:ok` and a list of nodes running the coordinators.
    - `{:error, :unavailable}`: An error tuple indicating the information is not accessible at the moment.
  """
  @spec fetch_coordinator_nodes(gateway :: ref(), Bedrock.timeout_in_ms()) ::
          {:ok, [node()]} | {:error, :unavailable}
  def fetch_coordinator_nodes(gateway, timeout_in_ms \\ 5_000),
    do: gateway |> call(:fetch_coordinator_nodes, timeout_in_ms)

  @doc """
  Report the addition of a new worker to the cluster director. It does so by
  sending an asynchronous message to the specified gateway process. The gateway
  process will then gather some information from the worker and pass it to the
  cluster director.

  ## Parameters
    - `gateway`: The GenServer name or PID of the gateway that will handle the
      new worker information.
    - `worker`: The PID of the new worker process that has been added. It will
      be interrogated for details before passing it to the cluster director.

  ## Returns
    - `:ok`: Always returns `:ok` as the message is sent asynchronously.
  """
  @spec advertise_worker(gateway :: ref(), worker :: pid()) :: :ok
  def advertise_worker(gateway, worker),
    do: gateway |> cast({:advertise_worker, worker})
end
