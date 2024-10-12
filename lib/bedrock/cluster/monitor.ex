defmodule Bedrock.Cluster.Monitor do
  @moduledoc """
  The `Bedrock.Cluster.Monitor` is the interface to a GenServer that is
  responsible for finding and holding the current coordinator and controller for
  a cluster.
  """

  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator

  use Bedrock.Internal.ChildSpec

  @type ref :: GenServer.name()
  @type controller :: ClusterController.ref()

  @doc """
  Ping all of the nodes in the cluster.
  """
  @spec ping_nodes(
          monitor_otp_name :: atom(),
          nodes :: [node()],
          ClusterController.ref(),
          Bedrock.epoch()
        ) ::
          :ok
  def ping_nodes(monitor_otp_name, nodes, cluster_controller, epoch) do
    GenServer.abcast(
      nodes,
      monitor_otp_name,
      {:ping, cluster_controller, epoch}
    )

    :ok
  end

  @doc """
  Get a coordinator for the cluster. We ask the running instance of the cluster
  monitor to find one for us.
  """
  @spec fetch_coordinator(monitor :: ref()) ::
          {:ok, Coordinator.ref()} | {:error, :unavailable}
  def fetch_coordinator(monitor) do
    monitor |> GenServer.call(:fetch_coordinator)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the current controller for the cluster.
  """
  @spec fetch_controller(monitor :: ref()) ::
          {:ok, ClusterController.ref()} | {:error, :unavailable}
  def fetch_controller(monitor) do
    monitor |> GenServer.call(:fetch_controller)
  catch
    :exit, _ -> {:error, :unavailable}
  end

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
  @spec fetch_coordinator_nodes(monitor :: ref()) :: {:ok, [node()]} | {:error, :unavailable}
  def fetch_coordinator_nodes(monitor) do
    monitor |> GenServer.call(:fetch_coordinator_nodes)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Report the addition of a new worker to the cluster controller. It does so by
  sending an asynchronous message to the specified monitor process. The monitor
  process will then gather some information from the worker and pass it to the
  cluster controller.

  ## Parameters
    - `monitor`: The GenServer name or PID of the monitor that will handle the
      new worker information.
    - `worker`: The PID of the new worker process that has been added. It will
      be interrogated for details before passing it to the cluster controller.

  ## Returns
    - `:ok`: Always returns `:ok` as the message is sent asynchronously.
  """
  @spec advertise_worker(monitor :: ref(), worker :: pid()) :: :ok
  def advertise_worker(monitor, worker),
    do: GenServer.cast(monitor, {:advertise_worker, worker})
end
