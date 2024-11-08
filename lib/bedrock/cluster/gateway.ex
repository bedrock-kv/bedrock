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
          ref(),
          opts :: [
            retry_count: pos_integer(),
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: {:ok, pid()} | {:error, term()}
  def begin_transaction(gateway, opts \\ []),
    do: gateway |> call({:begin_transaction, opts}, opts[:timeout_in_ms] || :infinity)

  @doc """
  Ping all of the nodes in the cluster.
  """
  @spec ping_nodes(gateway_otp_name :: atom(), nodes :: [node()], Director.ref(), Bedrock.epoch()) ::
          :ok
  def ping_nodes(gateway_otp_name, nodes, director, epoch),
    do: nodes |> broadcast(gateway_otp_name, {:ping, director, epoch})

  @doc """
  Get a coordinator for the cluster. We ask the running instance of the cluster
  gateway to find one for us.
  """
  @spec fetch_coordinator(gateway :: ref(), Bedrock.timeout_in_ms()) ::
          {:ok, Coordinator.ref()} | {:error, :unavailable}
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
