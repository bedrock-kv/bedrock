defmodule Bedrock.Cluster.Monitor do
  @moduledoc """
  The `Bedrock.Cluster.Monitor` is the interface to a GenServer that is
  responsible for finding and holding the current coordinator and director for
  a cluster.
  """

  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()
  @type director :: Director.ref()
  @type timeout_in_ms :: Bedrock.timeout_in_ms()

  @doc """
  Ping all of the nodes in the cluster.
  """
  @spec ping_nodes(monitor_otp_name :: atom(), nodes :: [node()], director, Bedrock.epoch()) ::
          :ok
  def ping_nodes(monitor_otp_name, nodes, director, epoch),
    do: nodes |> broadcast(monitor_otp_name, {:ping, director, epoch})

  @doc """
  Get a coordinator for the cluster. We ask the running instance of the cluster
  monitor to find one for us.
  """
  @spec fetch_coordinator(monitor :: ref(), timeout_in_ms()) ::
          {:ok, Coordinator.ref()} | {:error, :unavailable}
  def fetch_coordinator(monitor, timeout_in_ms \\ 5_000),
    do: monitor |> call(:fetch_coordinator, timeout_in_ms)

  @doc """
  Get the current director for the cluster.
  """
  @spec fetch_director(monitor :: ref(), timeout_in_ms()) ::
          {:ok, Director.ref()} | {:error, :unavailable}
  def fetch_director(monitor, timeout_in_ms \\ 5_000),
    do: monitor |> call(:fetch_director, timeout_in_ms)

  @doc """
  Retrieve the list of nodes currently running the coordinator process for the
  cluster. If successful, it returns a tuple with `:ok` and the list of nodes;
  otherwise, it returns `{:error, :unavailable}` if unable to retrieve the
  information.

  ## Parameters
    - `monitor`: The reference to the GenServer instance of the cluster monitor.

  ## Returns
    - `{:ok, [node()]}`: A tuple containing `:ok` and a list of nodes running the coordinators.
    - `{:error, :unavailable}`: An error tuple indicating the information is not accessible at the moment.
  """
  @spec fetch_coordinator_nodes(monitor :: ref(), timeout_in_ms()) ::
          {:ok, [node()]} | {:error, :unavailable}
  def fetch_coordinator_nodes(monitor, timeout_in_ms \\ 5_000),
    do: monitor |> call(:fetch_coordinator_nodes, timeout_in_ms)

  @doc """
  Report the addition of a new worker to the cluster director. It does so by
  sending an asynchronous message to the specified monitor process. The monitor
  process will then gather some information from the worker and pass it to the
  cluster director.

  ## Parameters
    - `monitor`: The GenServer name or PID of the monitor that will handle the
      new worker information.
    - `worker`: The PID of the new worker process that has been added. It will
      be interrogated for details before passing it to the cluster director.

  ## Returns
    - `:ok`: Always returns `:ok` as the message is sent asynchronously.
  """
  @spec advertise_worker(monitor :: ref(), worker :: pid()) :: :ok
  def advertise_worker(monitor, worker),
    do: monitor |> cast({:advertise_worker, worker})
end
