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

  Leaders track their readiness state for Director recovery:

  - `:not_leader` - This node is not the cluster leader
  - `:leader_ready` - This node is leader and ready to attempt Director recovery
  - `:recovery_failed` - Director recovery failed; waiting for meaningful capability changes

  Upon election, leaders immediately transition to `:leader_ready` and attempt Director
  recovery. Config is loaded from object storage at init, and TSL is the output of recovery
  (not an input), so there's no need to wait for state restoration.

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

  @doc """
  Notify the coordinator of a config update.

  This is called by the Director during recovery to update the coordinator's
  cached config (e.g., recovery_attempt state). Config is persisted to object
  storage by the Director's persistence phase, not via Raft consensus.
  """
  @spec notify_config(coordinator_ref :: ref(), config :: Config.t()) :: :ok
  def notify_config(coordinator, config), do: GenServer.cast(coordinator, {:notify_config, config})

  @spec fetch_transaction_system_layout(
          coordinator_ref :: ref(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, transaction_system_layout :: TransactionSystemLayout.t()}
          | {:error, :unavailable | :timeout}
  def fetch_transaction_system_layout(coordinator, timeout \\ 5_000),
    do: call(coordinator, :fetch_transaction_system_layout, timeout)

  @doc """
  Notify the coordinator of a new transaction system layout.

  This is called by the Director after recovery completes to update the coordinator's
  cached TSL and broadcast it to all Link subscribers. Unlike the old Raft-based
  update, this is a direct notification without consensus - the TSL is persisted
  to object storage by the Director's persistence phase.
  """
  @spec notify_transaction_system_layout(
          coordinator_ref :: ref(),
          transaction_system_layout :: TransactionSystemLayout.t()
        ) :: :ok
  def notify_transaction_system_layout(coordinator, transaction_system_layout),
    do: GenServer.cast(coordinator, {:notify_transaction_system_layout, transaction_system_layout})

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
