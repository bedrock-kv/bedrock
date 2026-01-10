defmodule Bedrock.SystemKeys do
  @moduledoc """
  Centralized definition of all system keys used for cluster configuration
  and transaction system layout persistence.

  This module provides a single source of truth for system key names,
  preventing magic strings from being scattered throughout the codebase.

  ## Key Categories

  - **Cluster Configuration**: Static cluster settings (coordinators, policies, parameters)
  - **Transaction Layout**: Dynamic transaction system state (sequencer, proxies, resolvers, logs, storage)
  - **Legacy Compatibility**: Monolithic keys for backward compatibility and coordinator handoff
  """

  @system_prefix "\xff/system"

  # Cluster Configuration Keys
  # These contain static cluster settings that rarely change

  @doc "List of coordinator nodes in the cluster"
  @spec cluster_coordinators() :: Bedrock.key()
  def cluster_coordinators, do: "#{@system_prefix}/cluster/coordinators"

  @doc "Current cluster epoch number"
  @spec cluster_epoch() :: Bedrock.key()
  def cluster_epoch, do: "#{@system_prefix}/cluster/epoch"

  @doc "Cluster policy: allow volunteer nodes to join"
  @spec cluster_policies_volunteer_nodes() :: Bedrock.key()
  def cluster_policies_volunteer_nodes, do: "#{@system_prefix}/cluster/policies/volunteer_nodes"

  @doc "Cluster parameter: desired number of logs"
  @spec cluster_parameters_desired_logs() :: Bedrock.key()
  def cluster_parameters_desired_logs, do: "#{@system_prefix}/cluster/parameters/desired_logs"

  @doc "Cluster parameter: desired replication factor"
  @spec cluster_parameters_desired_replication() :: Bedrock.key()
  def cluster_parameters_desired_replication, do: "#{@system_prefix}/cluster/parameters/desired_replication"

  @doc "Cluster parameter: desired number of commit proxies"
  @spec cluster_parameters_desired_commit_proxies() :: Bedrock.key()
  def cluster_parameters_desired_commit_proxies, do: "#{@system_prefix}/cluster/parameters/desired_commit_proxies"

  @doc "Cluster parameter: desired number of coordinators"
  @spec cluster_parameters_desired_coordinators() :: Bedrock.key()
  def cluster_parameters_desired_coordinators, do: "#{@system_prefix}/cluster/parameters/desired_coordinators"

  @doc "Cluster parameter: desired number of read version proxies"
  @spec cluster_parameters_desired_read_version_proxies() :: Bedrock.key()
  def cluster_parameters_desired_read_version_proxies,
    do: "#{@system_prefix}/cluster/parameters/desired_read_version_proxies"

  @doc "Cluster parameter: empty transaction timeout in milliseconds"
  @spec cluster_parameters_empty_transaction_timeout_ms() :: Bedrock.key()
  def cluster_parameters_empty_transaction_timeout_ms,
    do: "#{@system_prefix}/cluster/parameters/empty_transaction_timeout_ms"

  @doc "Cluster parameter: ping rate in Hz"
  @spec cluster_parameters_ping_rate_in_hz() :: Bedrock.key()
  def cluster_parameters_ping_rate_in_hz, do: "#{@system_prefix}/cluster/parameters/ping_rate_in_hz"

  @doc "Cluster parameter: retransmission rate in Hz"
  @spec cluster_parameters_retransmission_rate_in_hz() :: Bedrock.key()
  def cluster_parameters_retransmission_rate_in_hz, do: "#{@system_prefix}/cluster/parameters/retransmission_rate_in_hz"

  @doc "Cluster parameter: transaction window in milliseconds"
  @spec cluster_parameters_transaction_window_in_ms() :: Bedrock.key()
  def cluster_parameters_transaction_window_in_ms, do: "#{@system_prefix}/cluster/parameters/transaction_window_in_ms"

  # Transaction System Layout Keys
  # These contain dynamic transaction system state that changes during recovery

  @doc "Current sequencer reference"
  @spec layout_sequencer() :: Bedrock.key()
  def layout_sequencer, do: "#{@system_prefix}/layout/sequencer"

  @doc "List of commit proxy references"
  @spec layout_proxies() :: Bedrock.key()
  def layout_proxies, do: "#{@system_prefix}/layout/proxies"

  @doc "Resolver for a key range (ceiling search by end_key)"
  @spec layout_resolver(end_key :: Bedrock.key()) :: Bedrock.key()
  def layout_resolver(end_key), do: "#{@system_prefix}/layout/resolvers/#{end_key}"

  @doc "Prefix for resolver keys (for range queries)"
  @spec layout_resolvers_prefix() :: Bedrock.key()
  def layout_resolvers_prefix, do: "#{@system_prefix}/layout/resolvers/"

  @doc "Configuration for a specific log by ID"
  @spec layout_log(Bedrock.service_id()) :: Bedrock.key()
  def layout_log(log_id), do: "#{@system_prefix}/layout/logs/#{log_id}"

  @doc "All log configurations (for range queries)"
  @spec layout_logs_prefix() :: Bedrock.key()
  def layout_logs_prefix, do: "#{@system_prefix}/layout/logs/"

  # Shard Management Keys
  # These map key ranges to shard tags and store shard metadata

  @doc "Shard key mapping (ceiling search by end_key) -> tag"
  @spec shard_key(end_key :: Bedrock.key()) :: Bedrock.key()
  def shard_key(end_key), do: "#{@system_prefix}/shard_keys/#{end_key}"

  @doc "Prefix for shard_keys (for range queries)"
  @spec shard_keys_prefix() :: Bedrock.key()
  def shard_keys_prefix, do: "#{@system_prefix}/shard_keys/"

  @doc "Shard metadata by tag"
  @spec shard(tag :: non_neg_integer()) :: Bedrock.key()
  def shard(tag), do: "#{@system_prefix}/shards/#{tag}"

  @doc "Prefix for shards (for range queries)"
  @spec shards_prefix() :: Bedrock.key()
  def shards_prefix, do: "#{@system_prefix}/shards/"

  @doc "Materializers for a key range (ceiling search by end_key)"
  @spec materializer_key(end_key :: Bedrock.key()) :: Bedrock.key()
  def materializer_key(end_key), do: "#{@system_prefix}/materializer_keys/#{end_key}"

  @doc "Prefix for materializer_keys (for range queries)"
  @spec materializer_keys_prefix() :: Bedrock.key()
  def materializer_keys_prefix, do: "#{@system_prefix}/materializer_keys/"

  @doc "Map of all services in the transaction system"
  @spec layout_services() :: Bedrock.key()
  def layout_services, do: "#{@system_prefix}/layout/services"

  @doc "Current director reference"
  @spec layout_director() :: Bedrock.key()
  def layout_director, do: "#{@system_prefix}/layout/director"

  @doc "Current rate keeper reference"
  @spec layout_rate_keeper() :: Bedrock.key()
  def layout_rate_keeper, do: "#{@system_prefix}/layout/rate_keeper"

  @doc "Transaction system layout ID"
  @spec layout_id() :: Bedrock.key()
  def layout_id, do: "#{@system_prefix}/layout/id"

  # Recovery State Keys
  # These contain ephemeral recovery state (may not be persisted long-term)

  @doc "Current recovery attempt number"
  @spec recovery_attempt() :: Bedrock.key()
  def recovery_attempt, do: "#{@system_prefix}/recovery/attempt"

  @doc "Current recovery state"
  @spec recovery_state() :: Bedrock.key()
  def recovery_state, do: "#{@system_prefix}/recovery/state"

  @doc "Timestamp of last successful recovery"
  @spec recovery_last_completed() :: Bedrock.key()
  def recovery_last_completed, do: "#{@system_prefix}/recovery/last_completed"

  # Legacy Compatibility Keys
  # These maintain backward compatibility and support coordinator handoff

  @doc "Monolithic cluster configuration (for coordinator epoch handoff)"
  @spec config_monolithic() :: Bedrock.key()
  def config_monolithic, do: "#{@system_prefix}/config"

  @doc "Monolithic transaction system layout (deprecated, use decomposed keys)"
  @spec layout_monolithic() :: Bedrock.key()
  def layout_monolithic, do: "#{@system_prefix}/transaction_system_layout"

  @doc "Legacy epoch key (use cluster_epoch instead)"
  @spec epoch_legacy() :: Bedrock.key()
  def epoch_legacy, do: "#{@system_prefix}/epoch"

  @doc "Legacy last recovery timestamp (use recovery_last_completed instead)"
  @spec last_recovery_legacy() :: Bedrock.key()
  def last_recovery_legacy, do: "#{@system_prefix}/last_recovery"

  # Utility Functions

  @doc """
  Returns all cluster configuration keys as a list.
  Useful for batch operations or validation.
  """
  @spec all_cluster_keys() :: [Bedrock.key()]
  def all_cluster_keys do
    [
      cluster_coordinators(),
      cluster_epoch(),
      cluster_policies_volunteer_nodes(),
      cluster_parameters_desired_logs(),
      cluster_parameters_desired_replication(),
      cluster_parameters_desired_commit_proxies(),
      cluster_parameters_desired_coordinators(),
      cluster_parameters_desired_read_version_proxies(),
      cluster_parameters_empty_transaction_timeout_ms(),
      cluster_parameters_ping_rate_in_hz(),
      cluster_parameters_retransmission_rate_in_hz(),
      cluster_parameters_transaction_window_in_ms()
    ]
  end

  @doc """
  Returns all transaction layout keys as a list (excluding dynamic keys with IDs/end_keys).
  Useful for batch operations or validation.
  """
  @spec all_layout_keys() :: [Bedrock.key()]
  def all_layout_keys do
    [
      layout_sequencer(),
      layout_proxies(),
      layout_services(),
      layout_director(),
      layout_rate_keeper(),
      layout_id()
    ]
  end

  @doc """
  Returns all legacy compatibility keys as a list.
  Useful for migration and cleanup operations.
  """
  @spec all_legacy_keys() :: [Bedrock.key()]
  def all_legacy_keys do
    [
      config_monolithic(),
      layout_monolithic(),
      epoch_legacy(),
      last_recovery_legacy()
    ]
  end

  @doc """
  Checks if a key is a system key (starts with the system prefix).
  """
  @spec system_key?(Bedrock.key()) :: boolean()
  def system_key?(key) when is_binary(key) do
    String.starts_with?(key, @system_prefix)
  end

  @spec system_key?(term()) :: boolean()
  def system_key?(_), do: false

  @doc """
  Returns the system key prefix.
  """
  @spec system_prefix() :: Bedrock.key()
  def system_prefix, do: @system_prefix
end
