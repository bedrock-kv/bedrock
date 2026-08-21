defmodule Bedrock.Cluster.Link do
  @moduledoc """
  Bidirectional link between a node and the cluster.

  The Link is responsible for:
  - Discovering and maintaining connection to the cluster Coordinator
  - Caching the Transaction System Layout (TSL)
  - Subscribing to TSL updates via push notifications
  - Providing coordinator reference and TSL to other components

  This is a focused service for cluster state management and node-to-cluster connectivity.
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
  def fetch_coordinator(link, opts \\ []), do: call(link, :get_known_coordinator, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the current known coordinator reference. Raises if unavailable.
  """
  @spec fetch_coordinator!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) :: Coordinator.ref()
  def fetch_coordinator!(link, opts \\ []) do
    case fetch_coordinator(link, opts) do
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
  def fetch_transaction_system_layout(link, opts \\ []),
    do: call(link, :get_transaction_system_layout, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cached Transaction System Layout. Raises if unavailable.
  """
  @spec fetch_transaction_system_layout!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          TransactionSystemLayout.t()
  def fetch_transaction_system_layout!(link, opts \\ []) do
    case fetch_transaction_system_layout(link, opts) do
      {:ok, tsl} -> tsl
      {:error, reason} -> raise "Failed to fetch transaction system layout: #{inspect(reason)}"
    end
  end

  @doc """
  Fetch the node's cached routing projection (shard boundaries plus
  materializer refs, as served by `Bedrock.DataPlane.CommitProxy.fetch_routing/2`).

  The Link is the node-wide location cache (FDB's `DatabaseContext`
  locationCache): it only stores. On a miss the caller fetches from a
  commit proxy and caches the result back with `cache_routing/2`.
  """
  @spec fetch_cached_routing(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, map()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_cached_routing(link, opts \\ []), do: call(link, :get_routing, opts[:timeout_in_ms] || 1000)

  @doc "Caches a routing projection fetched from a commit proxy."
  @spec cache_routing(ref(), map()) :: :ok
  def cache_routing(link, routing), do: cast(link, {:cache_routing, routing})

  @doc """
  Drops the cached routing projection.

  Called by the client retry loop when a read fails in a routing-shaped way
  (unroutable key, dead materializer, unavailable) - a dead pid is exactly
  what a stale snapshot looks like, so the next transaction refetches.

  Synchronous on purpose: the retry that invalidates must not be able to
  read the stale projection back on its next fetch. A cast would be
  ordered only by accident of the intervening wiring call.
  """
  @spec invalidate_routing(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          :ok | {:error, :unavailable | :timeout | :unknown}
  def invalidate_routing(link, opts \\ []), do: call(link, :invalidate_routing, opts[:timeout_in_ms] || 1000)

  @doc """
  Fetch the cluster descriptor.
  This includes the coordinator nodes and other cluster configuration.
  """
  @spec fetch_descriptor(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok, Descriptor.t()} | {:error, :unavailable | :timeout | :unknown}
  def fetch_descriptor(link, opts \\ []), do: call(link, :get_descriptor, opts[:timeout_in_ms] || 1000)

  @doc """
  Get the cluster descriptor. Raises if unavailable.
  """
  @spec fetch_descriptor!(ref(), opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          Descriptor.t()
  def fetch_descriptor!(link, opts \\ []) do
    case fetch_descriptor(link, opts) do
      {:ok, descriptor} -> descriptor
      {:error, reason} -> raise "Failed to fetch descriptor: #{inspect(reason)}"
    end
  end
end
