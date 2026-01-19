defmodule Bedrock.DataPlane.Demux.Server do
  @moduledoc """
  Demux coordinator that receives transactions from Log, slices by shard,
  and routes to ShardServers.

  ## Responsibilities

  1. Receive transactions pushed from Log via `GenServer.cast`
  2. Walk transaction, slice mutations per shard using MutationSlicer
  3. Send `{:txn, version, slice}` to each touched ShardServer
  4. Spin up ShardServer on first touch (lazy)
  5. Track durability: receive reports from ShardServers
  6. Report min_durable_version to Log for WAL trimming

  ## Durability Tracking

  When a ShardServer flushes data to ObjectStorage, it sends
  `{:durable, shard_id, max_version}` to this process. We track
  the minimum durable version across all active shards using gb_sets.

  ## Shard Activation

  When the first transaction touches a shard, we:
  1. Start a ShardServer via DynamicSupervisor
  2. Add the shard to durability tracking with initial version = last_seen_version
  """

  use GenServer

  alias Bedrock.DataPlane.Demux.Durability
  alias Bedrock.DataPlane.Demux.MutationSlicer
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Demux.ShardServerSupervisor
  alias Bedrock.DataPlane.Transaction

  require Logger

  @type shard_id :: non_neg_integer()
  @type version :: Bedrock.version()

  defstruct [
    :cluster,
    :object_storage,
    :log,
    :shard_supervisor,
    shard_servers: %{},
    durability: nil,
    last_seen_version: nil
  ]

  @doc """
  Starts the Demux.Server.

  ## Options

  - `:cluster` - Required. Cluster name.
  - `:object_storage` - Required. ObjectStorage backend.
  - `:log` - Required. PID of the owning Log.
  - `:shard_supervisor` - Required. Module for ShardServer supervisor.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Pushes a committed transaction to the Demux for distribution.

  Called by Log after a transaction is committed. This is async (cast).

  ## Parameters

  - `server` - Demux.Server pid or name
  - `version` - Commit version (8-byte binary)
  - `transaction` - Encoded transaction binary with SHARD_INDEX
  """
  @spec push(GenServer.server(), version(), binary()) :: :ok
  def push(server, version, transaction) do
    GenServer.cast(server, {:push, version, transaction})
  end

  @doc """
  Gets the ShardServer for a given shard.

  Called by materializers to discover their ShardServer for pulling.
  Creates the ShardServer if it doesn't exist yet.
  """
  @spec get_shard_server(GenServer.server(), shard_id()) :: {:ok, pid()} | {:error, term()}
  def get_shard_server(server, shard_id) do
    GenServer.call(server, {:get_shard_server, shard_id})
  end

  @doc """
  Returns the current minimum durable version across all shards.

  Returns nil if no shards are active.
  """
  @spec min_durable_version(GenServer.server()) :: version() | nil
  def min_durable_version(server) do
    GenServer.call(server, :min_durable_version)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    object_storage = Keyword.fetch!(opts, :object_storage)
    log = Keyword.fetch!(opts, :log)
    shard_supervisor = Keyword.fetch!(opts, :shard_supervisor)

    state = %__MODULE__{
      cluster: cluster,
      object_storage: object_storage,
      log: log,
      shard_supervisor: shard_supervisor,
      shard_servers: %{},
      durability: Durability.new(),
      last_seen_version: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:push, version, transaction}, state) do
    state = do_push(state, version, transaction)
    {:noreply, state}
  end

  @impl true
  def handle_call({:get_shard_server, shard_id}, _from, state) do
    case get_or_create_shard_server(state, shard_id) do
      {:ok, pid, state} -> {:reply, {:ok, pid}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:min_durable_version, _from, state) do
    {:reply, Durability.min_durable_version(state.durability), state}
  end

  @impl true
  def handle_info({:durable, shard_id, durable_version}, state) do
    state = handle_durability_report(state, shard_id, durable_version)
    {:noreply, state}
  end

  # Private implementation

  defp do_push(state, version, transaction) do
    # Update last seen version
    state = %{state | last_seen_version: version}

    # Get commit version from transaction
    commit_version = Transaction.commit_version!(transaction)

    # Slice transaction by shard
    case MutationSlicer.slice(transaction, commit_version) do
      {:ok, slices} ->
        # Route each slice to its ShardServer
        Enum.reduce(slices, state, fn {shard_id, slice}, acc ->
          route_to_shard(acc, shard_id, version, slice)
        end)

      {:error, :no_shard_index} ->
        # Transaction has no shard index - might be metadata-only
        # Log but don't fail
        Logger.debug("Transaction #{inspect(version)} has no shard index, skipping demux")
        state

      {:error, reason} ->
        Logger.error("Failed to slice transaction: #{inspect(reason)}")
        state
    end
  end

  defp route_to_shard(state, shard_id, version, slice) do
    case get_or_create_shard_server(state, shard_id) do
      {:ok, pid, state} ->
        # Push to ShardServer (async)
        ShardServer.push(pid, version, slice)
        state

      {:error, reason} ->
        Logger.error("Failed to get ShardServer for shard #{shard_id}: #{inspect(reason)}")
        state
    end
  end

  defp get_or_create_shard_server(state, shard_id) do
    case Map.fetch(state.shard_servers, shard_id) do
      {:ok, pid} ->
        {:ok, pid, state}

      :error ->
        create_shard_server(state, shard_id)
    end
  end

  defp create_shard_server(state, shard_id) do
    opts = [
      shard_id: shard_id,
      demux: self(),
      cluster: state.cluster,
      object_storage: state.object_storage
    ]

    case ShardServerSupervisor.start_child(opts) do
      {:ok, pid} ->
        # Track the new ShardServer
        shard_servers = Map.put(state.shard_servers, shard_id, pid)

        # Activate in durability tracking
        # Initial durable version = last_seen_version (or 0 if none)
        initial_version = state.last_seen_version || <<0::64>>
        {:ok, durability} = Durability.activate_shard(state.durability, shard_id, initial_version)

        state = %{state | shard_servers: shard_servers, durability: durability}
        {:ok, pid, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_durability_report(state, shard_id, durable_version) do
    case Durability.update_shard(state.durability, shard_id, durable_version) do
      {:ok, durability} ->
        old_min = Durability.min_durable_version(state.durability)
        new_min = Durability.min_durable_version(durability)

        # If min advanced, notify log
        if new_min != old_min and new_min != nil do
          notify_log_durability(state.log, new_min)
        end

        %{state | durability: durability}

      {:error, reason} ->
        Logger.warning("Failed to update durability for shard #{shard_id}: #{inspect(reason)}")
        state
    end
  end

  defp notify_log_durability(log, min_durable_version) do
    send(log, {:min_durable_version, min_durable_version})
  end
end
