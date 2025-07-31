defmodule Bedrock.ControlPlane.Director do
  @moduledoc """
  The director is a singleton within the cluster that orchestrates transaction
  system lifecycle and epoch-based generation management.

  Created by the coordinator election winner, the director is responsible for
  bringing up the data plane and maintaining the cluster in a writable state.
  It coordinates recovery after component failures and manages the distributed
  consensus required for consistent system operation.

  ## Epoch Management

  The director implements epoch-based generation management to prevent split-brain
  scenarios during recovery. Each recovery increments the cluster epoch, which
  serves as a generation counter that ensures clean transitions between system
  configurations.

  Services locked with newer epochs take precedence over older ones, and processes
  from previous epochs terminate themselves when they detect a generation change.
  This approach eliminates the need for complex coordination protocols while
  maintaining system consistency during concurrent recovery attempts.

  The epoch mechanism ensures that only one director generation can make progress
  at any given time, preventing conflicting recovery operations that could
  compromise data integrity or system availability.
  """
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.Service.Worker

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

  @spec fetch_transaction_system_layout(director_ref :: ref(), timeout_in_ms :: timeout_in_ms()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_transaction_system_layout(director, timeout_in_ms \\ 5_000),
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

  @doc """
  Notifies the director that new services have been registered in the coordinator.
  This allows the director to update its service directory and retry stalled recovery
  if the new services might resolve insufficient_nodes conditions.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `service_infos`: List of service info tuples in format {service_id, kind, {otp_name, node}}

  ## Returns
  - `:ok`: Indicates the notification was successfully sent.
  """
  @spec notify_services_registered(
          director :: ref(),
          service_infos :: [{String.t(), atom(), {atom(), node()}}]
        ) :: :ok
  def notify_services_registered(director, service_infos),
    do: director |> cast({:service_registered, service_infos})

  @doc """
  Notifies the director that node capabilities have been updated.
  This allows the director to update its capability map and retry stalled recovery
  if the new capabilities might resolve insufficient_nodes conditions.

  ## Parameters
  - `director`: The reference to the cluster director (a GenServer).
  - `node_capabilities`: Map of capability -> [nodes] for use by recovery

  ## Returns
  - `:ok`: Indicates the notification was successfully sent.
  """
  @spec notify_capabilities_updated(
          director :: ref(),
          node_capabilities :: %{Bedrock.Cluster.capability() => [node()]}
        ) :: :ok
  def notify_capabilities_updated(director, node_capabilities),
    do: director |> cast({:capabilities_updated, node_capabilities})
end
