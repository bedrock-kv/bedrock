defmodule Bedrock.Cluster.CoordinatorClient do
  @moduledoc """
  Coordinator discovery and Transaction System Layout caching.

  The CoordinatorClient is responsible for:
  - Discovering and maintaining connection to the cluster Coordinator
  - Caching the Transaction System Layout (TSL)
  - Subscribing to TSL updates via push notifications
  - Providing coordinator reference and TSL to other components

  This is a stripped-down service focused solely on cluster state management.
  Transaction creation is handled by Internal.Repo, and service registration
  is handled directly by Foreman.
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator

  @type ref :: pid() | atom() | {atom(), node()}

  @doc """
  Get the current known coordinator reference.
  Returns the coordinator if available, or error if coordinator discovery is pending.
  """
  @spec get_coordinator(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, Coordinator.ref()} | {:error, :unavailable | :timeout | :unknown}
  def get_coordinator(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_known_coordinator, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cached Transaction System Layout.
  Returns the TSL if coordinator is connected and TSL has been received.
  """
  @spec get_transaction_system_layout(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :unavailable | :timeout | :unknown}
  def get_transaction_system_layout(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_transaction_system_layout, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cluster descriptor.
  This includes the coordinator nodes and other cluster configuration.
  """
  @spec get_descriptor(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, Bedrock.Cluster.Descriptor.t()} | {:error, :unavailable | :timeout | :unknown}
  def get_descriptor(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_descriptor, opts[:timeout_in_ms] || 1000)
end
