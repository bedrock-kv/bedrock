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
  Retrieve the list of nodes currently running the coordinator processes for the specified cluster.

  The function communicates with the running instance of the cluster monitor to fetch
  the nodes responsible for coordination within the cluster. If successful, it returns
  a tuple with `:ok` and the list of nodes; otherwise, it returns `{:error, :unavailable}`
  if unable to retrieve the information.

  The purpose of this function is to help identify and interact with the nodes
  managing coordination tasks, which is crucial for maintaining the health and
  operation of the cluster.

  ## Parameters
    - monitor: The reference to the GenServer instance of the cluster monitor.

  ## Returns
    - `{:ok, [node()]}`: A tuple containing `:ok` and a list of nodes running the coordinators.
    - `{:error, :unavailable}`: An error tuple indicating the information is not accessible at the moment.

  ## Examples
      iex> Bedrock.Cluster.Monitor.fetch_coordinator_nodes(monitor_ref)
      {:ok, [:"node1@127.0.0.1", :"node2@127.0.0.1"]}

      iex> Bedrock.Cluster.Monitor.fetch_coordinator_nodes(monitor_ref)
      {:error, :unavailable}
  """
  @spec fetch_coordinator_nodes(monitor :: ref()) :: {:ok, [node()]} | {:error, :unavailable}
  def fetch_coordinator_nodes(monitor) do
    monitor |> GenServer.call(:fetch_coordinator_nodes)
  catch
    :exit, _ -> {:error, :unavailable}
  end
end
