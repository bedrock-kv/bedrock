defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  Manages cluster state through Raft consensus and coordinates Director lifecycle.

  The Coordinator maintains the authoritative cluster configuration and service
  directory through distributed consensus. When elected as leader, it manages
  Director startup with proper service discovery timing to prevent race conditions.

  ## Service Registration Flow

  Gateways advertise their node's services to the elected leader Coordinator by
  calling `register_services/2` or `register_gateway/4`. The leader then persists
  this service information through Raft consensus, propagating updates to all
  Coordinators. The service directory is maintained consistently across the
  cluster, ensuring the Director receives a populated service directory at startup.

  ## Leader Readiness States

  New leaders transition through readiness states to prevent service discovery
  race conditions. Upon election, leaders enter `:leader_waiting_consensus` state,
  waiting for the first consensus round to populate the service directory. Once
  consensus completes, they transition to `:leader_ready` state and can start
  the Director. This ensures Directors receive complete service topology before
  beginning recovery.

  ## See Also

  - `Bedrock.ControlPlane.Director` - Recovery orchestration
  - `Bedrock.ControlPlane.Coordinator.State` - Internal state management
  """
  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type ref :: atom() | {atom(), node()}
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec config_key() :: atom()
  def config_key, do: :coordinator

  @spec fetch_config(coordinator_ref :: ref(), timeout_ms :: timeout_in_ms()) ::
          {:ok, config :: Config.t()} | {:error, :unavailable | :timeout}
  def fetch_config(coordinator, timeout \\ 5_000), do: call(coordinator, :fetch_config, timeout)

  @spec update_config(
          coordinator_ref :: ref(),
          config :: Config.t(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def update_config(coordinator, config, timeout \\ 5_000), do: call(coordinator, {:update_config, config}, timeout)

  @spec fetch_transaction_system_layout(
          coordinator_ref :: ref(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, transaction_system_layout :: TransactionSystemLayout.t()}
          | {:error, :unavailable | :timeout}
  def fetch_transaction_system_layout(coordinator, timeout \\ 5_000),
    do: call(coordinator, :fetch_transaction_system_layout, timeout)

  @spec update_transaction_system_layout(
          coordinator_ref :: ref(),
          transaction_system_layout :: TransactionSystemLayout.t(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def update_transaction_system_layout(coordinator, transaction_system_layout, timeout \\ 5_000),
    do: call(coordinator, {:update_transaction_system_layout, transaction_system_layout}, timeout)

  @type service_info :: {service_id :: String.t(), kind :: atom(), worker_ref :: {atom(), node()}}
  @type compact_service_info :: {service_id :: String.t(), kind :: atom(), name :: atom()}

  @spec register_services(
          coordinator_ref :: ref(),
          services :: [service_info()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def register_services(coordinator, services, timeout \\ 5_000),
    do: call(coordinator, {:register_services, services}, timeout)

  @spec deregister_services(
          coordinator_ref :: ref(),
          service_ids :: [String.t()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def deregister_services(coordinator, service_ids, timeout \\ 5_000),
    do: call(coordinator, {:deregister_services, service_ids}, timeout)

  @spec register_gateway(
          coordinator_ref :: ref(),
          gateway_pid :: pid(),
          compact_services :: [compact_service_info()],
          capabilities :: [Bedrock.Cluster.capability()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def register_gateway(coordinator, gateway_pid, compact_services, capabilities, timeout \\ 5_000),
    do: call(coordinator, {:register_gateway, gateway_pid, compact_services, capabilities}, timeout)
end
