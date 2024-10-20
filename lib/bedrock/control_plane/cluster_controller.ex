defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable state.
  """

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Transaction

  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.server()
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  @doc """
  Sends a 'pong' message to the specified cluster controller from the given node.

  ## Parameters
  - `cluster_controller`: The reference to the cluster controller (a GenServer).
  - `from_node`: The node from which the pong is sent.

  ## Returns
  - `:ok`: Indicates the message was successfully sent.
  """
  @spec send_pong(cluster_controller :: ref(), from_node :: node()) :: :ok
  def send_pong(cluster_controller, from_node),
    do: cluster_controller |> cast({:pong, from_node})

  @doc """
  Sends a 'ping' message to the specified cluster controller from the given node.

  ## Parameters
  - `cluster_controller`: The reference to the cluster controller (a GenServer).

  ## Returns
  - `:ok`: Indicates the message was successfully sent.
  """
  @spec send_ping(cluster_controller :: ref()) :: :ok
  def send_ping(cluster_controller),
    do: cluster_controller |> cast({:ping, self()})

  @doc """
  Reports a new worker to the cluster controller.

  ## Parameters
  - `cluster_controller`: The reference to the cluster controller (a GenServer).
  - `node`: The node where the new worker is located.
  - `worker_info`: A keyword list of information about the worker.

  ## Returns
  - `:ok`: Indicates the report was successfully sent.
  """
  @spec advertise_worker(cluster_controller :: ref(), node(), keyword()) :: :ok
  def advertise_worker(cluster_controller, node, worker_info),
    do: cluster_controller |> cast({:node_added_worker, node, worker_info})

  @doc """
  Requests a node to rejoin the cluster with the given capabilities and running services.

  ## Parameters
  - `cluster_controller`: The reference to the cluster controller (a GenServer).
  - `node`: The node that wants to rejoin.
  - `capabilities`: A list of atoms representing the capabilities of the node.
  - `running_services`: A list of keywords representing the services running on the node.
  - `timeout_in_ms`: (Optional) The timeout for this request in milliseconds, default is 5000ms.

  ## Returns
  - `:ok`: If the request was successful.
  - `{:error, :unavailable}`: If the cluster controller is unavailable.
  - `{:error, :nodes_must_be_added_by_an_administrator}`: If nodes must be added by an administrator.
  """
  @spec request_to_rejoin(
          cluster_controller :: ref(),
          node(),
          capabilities :: [atom()],
          running_services :: [keyword()],
          timeout_in_ms()
        ) :: :ok | {:error, :unavailable | :nodes_must_be_added_by_an_administrator}
  def request_to_rejoin(
        cluster_controller,
        node,
        capabilities,
        running_services,
        timeout_in_ms \\ 5_000
      ) do
    cluster_controller
    |> call(
      {:request_to_rejoin, node, capabilities, running_services},
      timeout_in_ms
    )
  end

  @doc """
  Reports the completion of a log lock operation to the cluster controller.

  ## Parameters
  - `cluster_controller`: The reference to the cluster controller (a GenServer).
  - `id`: The identifier of the log that was locked.
  - `info`: A keyword list containing transaction information:
    - `:last_tx_id`: The version of the last transaction.
    - `:minimum_durable_tx_id`: The version of the minimum durable transaction.

  ## Returns
  - `:ok`: Indicates the report was successfully sent.
  """
  @spec report_log_lock_complete(
          cluster_controller :: ref(),
          Log.id(),
          info :: [
            last_tx_id: Bedrock.version(),
            minimum_durable_tx_id: Bedrock.version()
          ]
        ) :: :ok
  def report_log_lock_complete(controller, id, info),
    do: controller |> cast({:log_lock_complete, id, info})
end
