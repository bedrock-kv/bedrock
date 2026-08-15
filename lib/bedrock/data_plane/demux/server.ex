defmodule Bedrock.DataPlane.Demux.Server do
  @moduledoc """
  Demux coordinator that receives transactions from Log, slices by shard,
  routes to ShardServers, and commands deterministic chunk cuts.

  ## Responsibilities

  1. Receive transactions pushed from Log via `GenServer.cast`
  2. Walk transaction, slice mutations per shard using MutationSlicer
  3. Send `{:txn, version, slice}` to each touched ShardServer
  4. Spin up ShardServer on first touch (lazy)
  5. Compute deterministic cut versions and command flushes
  6. Track durability: receive confirmations from ShardServers
  7. Report min_durable_version to Log for WAL trimming

  ## Deterministic cuts

  Versions are microsecond timestamps, so all flush timing is pure version
  arithmetic: versions fall into fixed buckets of `cut_interval_us`
  (`bucket = div(version, cut_interval_us)`). When a pushed version — data or
  heartbeat — crosses into a new bucket, the previous bucket closes and every
  ShardServer is commanded `{:flush, cut_version}` with the last version of
  the closed bucket. The same versions produce the same cuts on every
  replica and every replay, which makes chunks byte-identical and lets
  `put_if_not_exists`/`:already_exists` count as confirmation.

  ## Known-committed gating

  A cut fires only once the known-committed version — piggybacked by commit
  proxies on every push — has reached it. Nothing not known-committed ever
  becomes durable, so chunks can never contain versions a recovery would
  discard, and recovery needs no chunk cleanup: the uncommitted tail lives
  only in ShardServer buffers, which die with the Demux tree.

  ## Durability Tracking

  A ShardServer confirms a cut by sending `{:durable, shard_id, cut_version}`
  — its floor contribution: everything it has ever seen at or below that
  version is durable (an empty buffer confirms immediately, so idle shards
  never pin the floor). We track the minimum across all active shards using
  gb_sets. With no shards at all, the last completed cut is the floor, so a
  heartbeat-only log still trims.

  ## Shard Activation

  When the first transaction touches a shard, we:
  1. Start a ShardServer linked to this process
  2. Add the shard to durability tracking with the last completed cut as its
     initial floor contribution — never a merely-buffered version

  ## Failure Semantics

  ShardServers are linked to this process. If any ShardServer crashes,
  this process crashes, which should crash the owning Log. Recovery
  replays from WAL with idempotent ObjectStorage writes.
  """

  use GenServer

  alias Bedrock.DataPlane.Demux.Durability
  alias Bedrock.DataPlane.Demux.MutationSlicer
  alias Bedrock.DataPlane.Demux.PersistenceTelemetry
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  require Logger

  @type shard_id :: non_neg_integer()
  @type version :: Bedrock.version()

  # Default cut interval (~5 seconds of version-time, in microseconds)
  @default_cut_interval_us 5_000_000

  defstruct [
    :cluster,
    :object_storage,
    :log,
    :cut_interval_us,
    shard_servers: %{},
    shard_server_opts: [],
    durability: nil,
    current_bucket: nil,
    last_cut_version: nil,
    known_committed_version: nil,
    pending_cut_version: nil
  ]

  @doc """
  Starts the Demux.Server linked to the calling process.

  ## Options

  - `:cluster` - Required. Cluster name.
  - `:object_storage` - Required. ObjectStorage backend.
  - `:log` - Required. PID of the owning Log.
  - `:cut_interval_us` - Optional. Version-time bucket width for deterministic
    cuts (default: 5_000_000, ~5s).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Pushes a committed transaction to the Demux for distribution.

  Called by Log after a transaction is committed. This is async (cast).

  ## Parameters

  - `server` - Demux.Server pid or name
  - `version` - Commit version (8-byte binary)
  - `transaction` - Encoded transaction binary with SHARD_INDEX
  """
  @spec push(GenServer.server(), version(), binary(), known_committed_version :: version() | nil) :: :ok
  def push(server, version, transaction, known_committed_version \\ nil) do
    GenServer.cast(server, {:push, version, transaction, known_committed_version})
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
  Returns the current minimum durable version across all shards, or the last
  completed cut when no shards are active.

  Returns nil if no cut has completed and no shards are active.
  """
  @spec min_durable_version(GenServer.server()) :: version() | nil
  def min_durable_version(server) do
    GenServer.call(server, :min_durable_version)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    # Trapping exits lets terminate/2 stop ShardServers synchronously — a
    # recovery reset must be certain no stale flush can land after teardown.
    # Crash propagation is preserved by re-raising in handle_info(:EXIT).
    Process.flag(:trap_exit, true)

    cluster = Keyword.fetch!(opts, :cluster)
    object_storage = Keyword.fetch!(opts, :object_storage)
    log = Keyword.fetch!(opts, :log)
    shard_server_opts = Keyword.get(opts, :shard_server_opts, [])
    cut_interval_us = Keyword.get(opts, :cut_interval_us, @default_cut_interval_us)

    state = %__MODULE__{
      cluster: cluster,
      object_storage: object_storage,
      log: log,
      cut_interval_us: cut_interval_us,
      shard_servers: %{},
      shard_server_opts: shard_server_opts,
      durability: Durability.new(),
      current_bucket: nil,
      last_cut_version: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:push, version, transaction, known_committed_version}, state) do
    state = do_push(state, version, transaction, known_committed_version)
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
    {:reply, current_min_durable_version(state), state}
  end

  @impl true
  def handle_info({:durable, shard_id, durable_version}, state) do
    state = handle_durability_report(state, shard_id, durable_version)
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  # A ShardServer (or the owning log) died: propagate — fail-fast. Our
  # terminate/2 will stop the remaining ShardServers synchronously.
  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.shard_servers, fn {_shard_id, pid} -> stop_shard_server(pid) end)
    :ok
  end

  defp stop_shard_server(pid) do
    GenServer.stop(pid, :shutdown, 5_000)
  catch
    :exit, _ -> :ok
  end

  # Private implementation

  defp do_push(state, version, transaction, known_committed_version) do
    state =
      state
      |> note_known_committed(known_committed_version)
      |> maybe_cut(version)
      |> maybe_fire_pending_cut()

    # Get commit version from transaction
    commit_version = Transaction.commit_version!(transaction)

    # Slice transaction by shard
    case MutationSlicer.slice(transaction, commit_version) do
      {:ok, []} ->
        # Empty transaction (heartbeat) - nothing to route
        state

      {:ok, slices} ->
        # Route each slice to its ShardServer
        Enum.reduce(slices, state, fn {shard_id, slice}, acc ->
          route_to_shard(acc, shard_id, version, slice)
        end)

      {:error, reason} ->
        Logger.error("Failed to slice transaction: #{inspect(reason)}")
        state
    end
  end

  defp note_known_committed(state, nil), do: state

  defp note_known_committed(%{known_committed_version: nil} = state, version),
    do: %{state | known_committed_version: version}

  defp note_known_committed(state, version) when version > state.known_committed_version,
    do: %{state | known_committed_version: version}

  defp note_known_committed(state, _version), do: state

  # The first version seen just establishes the current bucket.
  defp maybe_cut(%{current_bucket: nil} = state, version), do: %{state | current_bucket: bucket_of(state, version)}

  defp maybe_cut(state, version) do
    bucket = bucket_of(state, version)

    if bucket > state.current_bucket do
      # The cut closes every bucket before the one just entered, so a quiet
      # stretch spanning several buckets is closed by a single cut. It is
      # only a CANDIDATE until the known-committed version reaches it.
      cut_version = Version.from_integer(bucket * state.cut_interval_us - 1)
      %{state | current_bucket: bucket, pending_cut_version: cut_version}
    else
      state
    end
  end

  # THE durability invariant: nothing not known-committed ever becomes
  # durable. A cut fires only once the known-committed version has reached
  # it, so chunks can never contain versions a recovery would discard —
  # which is what makes deterministic re-flushes byte-identical and
  # `:already_exists` a truthful confirmation.
  defp maybe_fire_pending_cut(%{pending_cut_version: nil} = state), do: state
  defp maybe_fire_pending_cut(%{known_committed_version: nil} = state), do: state

  defp maybe_fire_pending_cut(%{pending_cut_version: cut_version} = state)
       when state.known_committed_version >= cut_version do
    Enum.each(state.shard_servers, fn {_shard_id, pid} -> ShardServer.flush(pid, cut_version) end)

    # With no shards there is nothing to persist: the cut itself is the
    # floor, so heartbeat-only logs still trim.
    if map_size(state.shard_servers) == 0 do
      notify_log_durability(state.log, cut_version)
    end

    %{state | last_cut_version: cut_version, pending_cut_version: nil}
  end

  defp maybe_fire_pending_cut(state), do: state

  defp bucket_of(state, version), do: version |> Version.to_integer() |> div(state.cut_interval_us)

  defp current_min_durable_version(state),
    do: Durability.min_durable_version(state.durability) || state.last_cut_version

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
    opts =
      [
        shard_id: shard_id,
        demux: self(),
        cluster: state.cluster,
        object_storage: state.object_storage
      ] ++ state.shard_server_opts

    # Start ShardServer linked to this process - crash propagates up
    case ShardServer.start_link(opts) do
      {:ok, pid} ->
        # Track the new ShardServer
        shard_servers = Map.put(state.shard_servers, shard_id, pid)

        # Activate in durability tracking with the last completed cut: the
        # activating slice is only buffered, so it must not count as durable.
        initial_version = state.last_cut_version || Version.zero()
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

          {_min, pinning_shard_id} = Durability.min_entry(durability)

          PersistenceTelemetry.emit_floor_advanced(
            new_min,
            pinning_shard_id,
            Durability.active_shard_count(durability)
          )
        end

        %{state | durability: durability}

      {:error, reason} ->
        Logger.warning("Failed to update durability for shard #{shard_id}: #{inspect(reason)}")
        state
    end
  end

  # Tagged with our pid so the log can reject watermarks from a stale Demux
  # incarnation after a recovery reset.
  defp notify_log_durability(log, min_durable_version) do
    send(log, {:min_durable_version, self(), min_durable_version})
  end
end
