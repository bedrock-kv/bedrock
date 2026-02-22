defmodule Bedrock.DataPlane.Demux.ShardServer do
  @moduledoc """
  Per-shard GenServer that buffers transactions and writes to ObjectStorage.

  Each ShardServer handles a single shard's transaction stream:
  1. Receives transaction slices from Demux
  2. Buffers in memory with newest at head
  3. Enqueues flush batches for async ObjectStorage persistence
  4. Notifies waiting materializers via WaitingList
  5. Reports durability to Demux only after persistence confirmation

  ## Buffer Management

  Transactions are stored as `[{version, slice}]` with newest at head.
  Flushing is triggered when:
  - `oldest_buffered_version < latest_version - version_gap_threshold`

  The default version gap threshold is 5,000,000 (~5 seconds in microsecond versions).

  ## Pull API

  Materializers call `pull/3` to get transactions from a given version.
  If data is available, it's returned immediately. Otherwise, the materializer
  is added to a WaitingList and notified when new data arrives.
  """

  use GenServer

  alias Bedrock.DataPlane.Demux.PersistenceWorker
  alias Bedrock.DataPlane.Demux.ShardServer.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.Keys

  @type shard_id :: non_neg_integer()
  @type version :: Bedrock.version()
  @type slice :: binary()

  # Default version gap threshold (~5 seconds in microseconds)
  @default_version_gap 5_000_000

  # Default pull timeout (30 seconds)
  @default_pull_timeout 30_000

  # Default pull limit
  @default_pull_limit 100

  # Periodic flush check interval (5 seconds)
  @flush_check_interval 5_000

  @doc """
  Starts a ShardServer for the given shard.

  ## Options

  - `:shard_id` - Required. The shard ID this server handles.
  - `:demux` - Required. PID of the Demux coordinator for durability reporting.
  - `:cluster` - Required. Cluster name for ObjectStorage paths.
  - `:object_storage` - Required. ObjectStorage backend.
  - `:version_gap` - Optional. Version gap threshold for flushing (default: 5_000_000).
  - `:persistence_queue_capacity` - Optional. Max queued flush batches (default: 1024).
  - `:persistence_max_retries` - Optional. Retry limit for flush failures (default: 5).
  - `:persistence_retry_backoff_ms` - Optional. Base retry backoff for flush retries (default: 25).
  - `:persistence_retry_tick_ms` - Optional. Retry polling tick for flush retries (default: 25).
  """
  def start_link(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(shard_id, opts[:registry]))
  end

  @doc """
  Returns the via tuple for a ShardServer.
  """
  def via_tuple(shard_id, registry \\ nil) do
    if registry do
      {:via, Registry, {registry, {:shard_server, shard_id}}}
    else
      {:global, {__MODULE__, shard_id}}
    end
  end

  @doc """
  Pushes a transaction slice to the ShardServer.

  Called by Demux when a transaction touches this shard.
  This is a cast (async, non-blocking).
  """
  @spec push(GenServer.server(), version(), slice()) :: :ok
  def push(server, version, slice) do
    GenServer.cast(server, {:push, version, slice})
  end

  @doc """
  Pulls transactions starting from the given version.

  Returns transactions from the buffer and/or ObjectStorage.
  If no data is available at the requested version, waits up to `timeout`
  for new data to arrive.

  ## Options

  - `:timeout` - How long to wait for data (default: 30_000ms)
  - `:limit` - Maximum transactions to return (default: 100)

  ## Returns

  - `{:ok, [{version, slice}]}` - Available transactions
  - `{:error, :timeout}` - No data arrived within timeout
  """
  @spec pull(GenServer.server(), version(), keyword()) ::
          {:ok, [{version(), slice()}]} | {:error, :timeout}
  def pull(server, from_version, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_pull_timeout)
    limit = Keyword.get(opts, :limit, @default_pull_limit)

    GenServer.call(server, {:pull, from_version, limit, timeout}, timeout + 5_000)
  end

  @doc """
  Returns the latest version in the buffer.
  """
  @spec latest_version(GenServer.server()) :: version() | nil
  def latest_version(server) do
    GenServer.call(server, :latest_version)
  end

  @doc """
  Returns the current durable version (flushed to ObjectStorage).
  """
  @spec durable_version(GenServer.server()) :: version() | nil
  def durable_version(server) do
    GenServer.call(server, :durable_version)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    demux = Keyword.fetch!(opts, :demux)
    cluster = Keyword.fetch!(opts, :cluster)
    object_storage = Keyword.fetch!(opts, :object_storage)
    version_gap = Keyword.get(opts, :version_gap, @default_version_gap)
    persistence_queue_capacity = Keyword.get(opts, :persistence_queue_capacity, 1_024)
    persistence_max_retries = Keyword.get(opts, :persistence_max_retries, 5)
    persistence_retry_backoff_ms = Keyword.get(opts, :persistence_retry_backoff_ms, 25)
    persistence_retry_tick_ms = Keyword.get(opts, :persistence_retry_tick_ms, 25)

    shard_tag = Keys.shard_tag(shard_id)
    chunk_reader = ChunkReader.new(object_storage, shard_tag)
    owner_pid = self()

    {:ok, persistence_worker} =
      PersistenceWorker.start_link(
        perform: fn payload -> persist_flush_payload(owner_pid, payload, object_storage, shard_tag) end,
        capacity: persistence_queue_capacity,
        max_retries: persistence_max_retries,
        retry_base_backoff_ms: persistence_retry_backoff_ms,
        retry_tick_ms: persistence_retry_tick_ms
      )

    state = %State{
      shard_id: shard_id,
      demux: demux,
      cluster: cluster,
      object_storage: object_storage,
      persistence_worker: persistence_worker,
      chunk_reader: chunk_reader,
      version_gap: version_gap,
      buffer: [],
      waiting_list: %{},
      flush_in_progress: false,
      pending_flush_max_version: nil,
      durable_version: nil,
      latest_version: nil
    }

    # Schedule periodic flush check
    schedule_flush_check()

    {:ok, state}
  end

  @impl true
  def handle_cast({:push, version, slice}, state) do
    state = do_push(state, version, slice)
    {:noreply, state}
  end

  @impl true
  def handle_call({:pull, from_version, limit, timeout}, from, state) do
    case do_pull(state, from_version, limit) do
      {:ok, transactions, state} when transactions != [] ->
        {:reply, {:ok, transactions}, state}

      {:ok, [], state} ->
        # No data available, add to waiting list
        reply_fn = fn response -> GenServer.reply(from, response) end
        {waiting_list, _timeout} = WaitingList.insert(state.waiting_list, from_version, {limit}, reply_fn, timeout)
        state = %{state | waiting_list: waiting_list}
        {:noreply, state, next_timeout(state)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:latest_version, _from, state) do
    {:reply, state.latest_version, state}
  end

  @impl true
  def handle_call(:durable_version, _from, state) do
    {:reply, state.durable_version, state}
  end

  @impl true
  def handle_info(:timeout, state) do
    # Handle waiting list timeouts
    {waiting_list, expired} = WaitingList.expire(state.waiting_list)
    WaitingList.reply_to_expired(expired, {:error, :timeout})
    state = %{state | waiting_list: waiting_list}
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def handle_info({:flush_persisted, max_flushed_version}, state) do
    state = handle_flush_persisted(state, max_flushed_version)
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def handle_info(:flush_check, state) do
    # Time-based flush: if buffer has old data, flush it all
    state = maybe_time_based_flush(state)
    schedule_flush_check()
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def terminate(_reason, state) do
    # Best-effort synchronous flush on shutdown.
    flush_remaining_sync(state)
    :ok
  end

  # Private implementation

  defp do_push(state, version, slice) do
    # Add to buffer (newest at head)
    buffer = [{version, slice} | state.buffer]
    latest_version = version

    state = %{state | buffer: buffer, latest_version: latest_version}

    # Check if we need to flush
    state = maybe_flush(state)

    # Notify waiting materializers
    state = notify_waiters(state, version)

    state
  end

  defp do_pull(state, from_version, limit) do
    # First check buffer for matching transactions
    buffer_txns = get_from_buffer(state.buffer, from_version, limit)

    if buffer_txns == [] do
      # Check ObjectStorage via ChunkReader
      case get_from_storage(state, from_version, limit) do
        {:ok, storage_txns} -> {:ok, storage_txns, state}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, buffer_txns, state}
    end
  end

  defp get_from_buffer(buffer, from_version, limit) do
    buffer
    # oldest first
    |> Enum.reverse()
    |> Enum.filter(fn {v, _} -> v >= from_version end)
    |> Enum.take(limit)
  end

  defp get_from_storage(state, from_version, limit) do
    transactions =
      state.chunk_reader
      |> ChunkReader.read_from_version(from_version, limit: limit)
      |> Enum.to_list()

    {:ok, transactions}
  rescue
    e -> {:error, {:storage_read_failed, e}}
  end

  defp maybe_flush(state) do
    cond do
      state.flush_in_progress ->
        state

      state.buffer == [] ->
        state

      should_flush?(state.buffer, state.latest_version, state.version_gap) ->
        # Get transactions to flush (oldest entries beyond the gap)
        {to_flush, _to_keep} = split_buffer_for_flush(state)
        enqueue_flush_batch(state, to_flush)

      true ->
        state
    end
  end

  defp should_flush?([], _latest, _gap), do: false
  defp should_flush?(_buffer, nil, _gap), do: false

  defp should_flush?(buffer, latest, gap) do
    {oldest_version, _} = List.last(buffer)
    Version.distance(latest, oldest_version) >= gap
  end

  defp enqueue_flush_batch(state, []), do: state

  defp enqueue_flush_batch(state, to_flush) do
    {max_flushed_version, _} = hd(to_flush)

    payload = %{
      transactions: to_flush,
      max_version: max_flushed_version
    }

    case PersistenceWorker.enqueue(state.persistence_worker, payload) do
      :ok ->
        %{state | flush_in_progress: true, pending_flush_max_version: max_flushed_version}

      {:error, :queue_full} ->
        # Retry on the next push/flush check.
        state
    end
  end

  defp handle_flush_persisted(state, max_flushed_version) do
    case state.pending_flush_max_version do
      nil ->
        state

      expected_max_version ->
        # Ignore stale completion notifications if they do not match the
        # current in-flight flush marker.
        if max_flushed_version == expected_max_version do
          previous_durable_version = state.durable_version
          durable_version = max_version(previous_durable_version, max_flushed_version)
          buffer = drop_persisted_from_buffer(state.buffer, durable_version)

          state = %{
            state
            | buffer: buffer,
              durable_version: durable_version,
              flush_in_progress: false,
              pending_flush_max_version: nil
          }

          if previous_durable_version != durable_version do
            report_durability(state.demux, state.shard_id, durable_version)
          end

          maybe_flush(state)
        else
          state
        end
    end
  end

  # Time-based flush: if oldest buffered version is older than flush interval, flush all
  defp maybe_time_based_flush(%{buffer: []} = state), do: state
  defp maybe_time_based_flush(%{flush_in_progress: true} = state), do: state

  defp maybe_time_based_flush(state) do
    {oldest_version, _} = List.last(state.buffer)
    oldest_us = Version.to_integer(oldest_version)
    now_us = System.os_time(:microsecond)
    age_us = now_us - oldest_us

    # Flush if data is older than flush check interval (convert ms to μs)
    if age_us >= @flush_check_interval * 1000 do
      flush_all(state)
    else
      state
    end
  end

  # Flush the entire buffer to object storage
  defp flush_all(%{buffer: []} = state), do: state

  defp flush_all(state) do
    enqueue_flush_batch(state, state.buffer)
  end

  defp drop_persisted_from_buffer(buffer, durable_version) when is_binary(durable_version) do
    Enum.filter(buffer, fn {version, _slice} ->
      Version.newer?(version, durable_version)
    end)
  end

  defp max_version(nil, v), do: v

  defp max_version(v1, v2) do
    case Version.compare(v1, v2) do
      :lt -> v2
      :eq -> v1
      :gt -> v1
    end
  end

  defp split_buffer_for_flush(%{buffer: buffer, latest_version: latest, version_gap: gap}) do
    # Buffer is newest-first, we want to flush oldest entries
    # Split where version < (latest - gap)
    threshold = Version.subtract(latest, gap)

    buffer
    # oldest first
    |> Enum.reverse()
    |> Enum.split_while(fn {v, _} -> Version.older?(v, threshold) end)
    |> case do
      {to_flush, to_keep} ->
        # Return both in newest-first order for consistency
        {Enum.reverse(to_flush), Enum.reverse(to_keep)}
    end
  end

  defp persist_flush_payload(owner_pid, payload, object_storage, shard_tag) do
    case persist_transactions(object_storage, shard_tag, payload.transactions) do
      :ok ->
        send(owner_pid, {:flush_persisted, payload.max_version})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_transactions(_object_storage, _shard_tag, []), do: :ok

  defp persist_transactions(object_storage, shard_tag, transactions_newest_first) do
    transactions_oldest_first = Enum.reverse(transactions_newest_first)

    encoded_transactions =
      Enum.map(transactions_oldest_first, fn {version, slice} ->
        {Version.to_integer(version), slice}
      end)

    with {:ok, chunk_binary} <- Chunk.encode(encoded_transactions),
         {max_version_int, _} <- List.last(encoded_transactions) do
      key = Keys.chunk_path(shard_tag, max_version_int)
      put_chunk(object_storage, key, chunk_binary)
    end
  end

  defp put_chunk(object_storage, key, chunk_binary) do
    case ObjectStorage.put_if_not_exists(object_storage, key, chunk_binary) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
      {:error, reason} -> {:error, {:write_failed, reason}}
    end
  end

  defp flush_remaining_sync(%{buffer: []}), do: :ok

  defp flush_remaining_sync(state) do
    shard_tag = Keys.shard_tag(state.shard_id)
    _ = persist_transactions(state.object_storage, shard_tag, state.buffer)
    :ok
  end

  defp report_durability(demux, shard_id, version) do
    send(demux, {:durable, shard_id, version})
  end

  defp notify_waiters(state, new_version) do
    # Notify all waiters waiting for version <= new_version
    # Use Version.increment to get next version as threshold
    threshold = Version.increment(new_version)
    {waiting_list, entries} = WaitingList.remove_all_less_than(state.waiting_list, threshold)

    Enum.each(entries, fn {_deadline, reply_fn, {limit}} ->
      # Get data for this waiter
      transactions = get_from_buffer(state.buffer, Version.zero(), limit)
      reply_fn.({:ok, transactions})
    end)

    %{state | waiting_list: waiting_list}
  end

  defp next_timeout(%{waiting_list: waiting_list}) do
    WaitingList.next_timeout(waiting_list)
  end

  defp schedule_flush_check do
    Process.send_after(self(), :flush_check, @flush_check_interval)
  end
end
