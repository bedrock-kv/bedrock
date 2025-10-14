defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  Manages cluster state through Raft consensus and coordinates Director lifecycle.

  The Coordinator maintains the authoritative cluster configuration and service
  directory through distributed consensus. When elected as leader, it manages
  Director startup with capability-based readiness checking to ensure robust
  recovery in dynamic environments.

  ## Service Registration Flow

  Nodes advertise their services and capabilities to the elected leader Coordinator by
  calling `register_services/2` or `register_node_resources/4`. The leader then persists
  this service information through Raft consensus, propagating updates to all
  Coordinators. The service directory is maintained consistently across the
  cluster, ensuring the Director receives current service topology during recovery.

  ## Leader Readiness States

  New leaders transition through readiness states to ensure state consistency
  before Director recovery:

  - `:not_leader` - This node is not the cluster leader
  - `:leader_waiting_consensus` - Leader elected but waiting for first consensus to ensure fully processed state
  - `:leader_ready` - This node is leader and ready to attempt Director recovery
  - `:recovery_failed` - Director recovery failed; waiting for meaningful capability changes

  Upon election, leaders enter `:leader_waiting_consensus` state, waiting for the first
  consensus round to ensure all Raft log entries have been processed and state is current.
  Once consensus completes, they transition to `:leader_ready` and attempt Director recovery
  with fully up-to-date state.

  ## Capability-Based Recovery Retry

  Leaders track service capability changes through hashing of recovery-relevant
  capabilities (coordination, log, storage). Recovery is only retried when
  meaningful capability changes occur, avoiding unnecessary retry attempts on
  transient service announcements or time-based intervals.

  ## See Also

  - `Bedrock.ControlPlane.Director` - Recovery orchestration
  - `Bedrock.ControlPlane.Coordinator.State` - Internal state management
  - `Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTracker` - Capability change detection
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

  @spec register_node_resources(
          coordinator_ref :: ref(),
          client_pid :: pid(),
          compact_services :: [compact_service_info()],
          capabilities :: [Bedrock.Cluster.capability()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def register_node_resources(coordinator, client_pid, compact_services, capabilities, timeout \\ 5_000),
    do: call(coordinator, {:register_node_resources, client_pid, compact_services, capabilities}, timeout)
end
