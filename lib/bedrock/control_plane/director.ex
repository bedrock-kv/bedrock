defmodule Bedrock.ControlPlane.Director do
  @moduledoc """
  The director is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable state.
  """
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  @type running_service_info :: %{
          id: String.t(),
          otp_name: atom(),
          kind: :log | :storage,
          pid: pid()
        }

  @type running_service_info_by_id :: %{Worker.id() => running_service_info()}

  @spec fetch_transaction_system_layout(director_ref :: ref(), timeout_ms :: timeout_in_ms()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_transaction_system_layout(director, timeout_in_ms),
    do: director |> call(:fetch_transaction_system_layout, timeout_in_ms)

  @doc """
  Sends a 'pong' message to the specified cluster director from the given node.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `from_node`: The node from which the pong is sent.

  ## Returns
  - `:ok`: Indicates the message was successfully sent.
  """
  @spec send_pong(director :: ref(), from_node :: node()) :: :ok
  def send_pong(director, from_node),
    do: director |> cast({:pong, from_node})

  @doc """
  Sends a 'ping' message to the specified cluster director from the given node.
  We also include the minimum read version. The director will respond with a
  'pong' message if it is alive and the read version is acceptable.
  """
  @spec send_ping(director :: ref(), minimum_read_version :: Bedrock.version()) :: :ok
  def send_ping(director, minimum_read_version),
    do: director |> cast({:ping, self(), minimum_read_version})

  @doc """
  Reports a new worker to the cluster director.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `node`: The node where the new worker is located.
  - `worker_info`: A keyword list of information about the worker.

  ## Returns
  - `:ok`: Indicates the report was successfully sent.
  """
  @spec advertise_worker(
          director :: ref(),
          node(),
          info :: running_service_info()
        ) :: :ok
  def advertise_worker(director, node, worker_info),
    do: director |> cast({:node_added_worker, node, worker_info})

  @doc """
  Requests a node to rejoin the cluster with the given capabilities and running services.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `node`: The node that wants to rejoin.
  - `capabilities`: A list of atoms representing the capabilities of the node.
  - `running_services`: A list of keywords representing the services running on the node.
  - `timeout_in_ms`: (Optional) The timeout for this request in milliseconds, default is 5000ms.

  ## Returns
  - `:ok`: If the request was successful.
  - `{:error, :unavailable}`: If the cluster director is unavailable.
  - `{:error, :nodes_must_be_added_by_an_administrator}`: If nodes must be added by an administrator.
  """
  @spec request_to_rejoin(
          director :: ref(),
          node(),
          capabilities :: [Bedrock.Cluster.capability()],
          Director.running_service_info_by_id(),
          timeout_in_ms()
        ) ::
          :ok
          | {:error, :unavailable | :timeout | :unknown}
          | {:error, :nodes_must_be_added_by_an_administrator}
          | {:error, {:relieved_by, {Bedrock.epoch(), director :: pid()}}}
  def request_to_rejoin(
        director,
        node,
        capabilities,
        running_services,
        timeout_in_ms \\ 5_000
      ) do
    director
    |> call(
      {:request_to_rejoin, node, capabilities, running_services},
      timeout_in_ms
    )
  end

  @doc """
  Requests a foreman on a specific node to create a new worker.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `node`: The node where the worker should be created.
  - `worker_id`: The ID for the new worker.
  - `kind`: The type of worker (:log or :storage).
  - `timeout_in_ms`: (Optional) The timeout for this request in milliseconds, default is 10000ms.

  ## Returns
  - `{:ok, worker_info}`: If the worker was successfully created.
  - `{:error, reason}`: If the worker creation failed.
  """
  @spec request_worker_creation(
          director :: ref(),
          node(),
          Worker.id(),
          :log | :storage,
          timeout_in_ms()
        ) ::
          {:ok, running_service_info()}
          | {:error, :worker_creation_failed | :node_unavailable | :timeout}
  def request_worker_creation(director, node, worker_id, kind, timeout_in_ms \\ 10_000) do
    director
    |> call({:request_worker_creation, node, worker_id, kind}, timeout_in_ms)
  end

  @doc """
  Relieves the cluster director of its duties. This is used when a new
  director is elected and the old director should no longer be used. The
  old director will ignore all messages from the cluster other than to
  indicate that it has been relieved and by whom.
  """
  @spec stand_relieved(
          director :: ref(),
          {new_epoch :: Bedrock.epoch(), new_director :: pid()}
        ) :: :ok
  def stand_relieved(director, {_new_epoch, _new_director} = relief),
    do: director |> cast({:stand_relieved, relief})
end
