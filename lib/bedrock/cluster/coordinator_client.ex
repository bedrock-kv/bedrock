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

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator

  @type ref :: pid() | atom() | {atom(), node()}

  @doc """
  Fetch the current known coordinator reference.
  Returns the coordinator if available, or error if coordinator discovery is pending.
  """
  @spec fetch_coordinator(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, Coordinator.ref()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_coordinator(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_known_coordinator, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the current known coordinator reference. Raises if unavailable.
  """
  @spec fetch_coordinator!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) :: Coordinator.ref()
  def fetch_coordinator!(coordinator_client, opts \\ []) do
    case fetch_coordinator(coordinator_client, opts) do
      {:ok, coordinator} -> coordinator
      {:error, reason} -> raise "Failed to fetch coordinator: #{inspect(reason)}"
    end
  end

  @doc """
  Fetch the cached Transaction System Layout.
  Returns the TSL if coordinator is connected and TSL has been received.
  """
  @spec fetch_transaction_system_layout(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_transaction_system_layout(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_transaction_system_layout, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cached Transaction System Layout. Raises if unavailable.
  """
  @spec fetch_transaction_system_layout!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          TransactionSystemLayout.t()
  def fetch_transaction_system_layout!(coordinator_client, opts \\ []) do
    case fetch_transaction_system_layout(coordinator_client, opts) do
      {:ok, tsl} -> tsl
      {:error, reason} -> raise "Failed to fetch transaction system layout: #{inspect(reason)}"
    end
  end

  @doc """
  Fetch the cluster descriptor.
  This includes the coordinator nodes and other cluster configuration.
  """
  @spec fetch_descriptor(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, Descriptor.t()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_descriptor(coordinator_client, opts \\ []),
    do: call(coordinator_client, :get_descriptor, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cluster descriptor. Raises if unavailable.
  """
  @spec fetch_descriptor!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          Descriptor.t()
  def fetch_descriptor!(coordinator_client, opts \\ []) do
    case fetch_descriptor(coordinator_client, opts) do
      {:ok, descriptor} -> descriptor
      {:error, reason} -> raise "Failed to fetch descriptor: #{inspect(reason)}"
    end
  end
end
