defmodule Bedrock.DataPlane.Demux.ShardServer do
  @moduledoc """
  Per-shard GenServer that buffers transactions and writes to ObjectStorage.

  Each ShardServer handles a single shard's transaction stream:
  1. Receives transaction slices from Demux
  2. Buffers in memory with newest at head
  3. Flushes to ObjectStorage via ChunkWriter when version window exceeded
  4. Notifies waiting materializers via WaitingList
  5. Reports durability to Demux after each flush

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

  alias Bedrock.DataPlane.Demux.ShardServer.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.ChunkWriter
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

    shard_tag = Keys.shard_tag(shard_id)

    {:ok, chunk_writer} = ChunkWriter.new(object_storage, shard_tag)
    chunk_reader = ChunkReader.new(object_storage, shard_tag)

    state = %State{
      shard_id: shard_id,
      demux: demux,
      cluster: cluster,
      object_storage: object_storage,
      chunk_writer: chunk_writer,
      chunk_reader: chunk_reader,
      version_gap: version_gap,
      buffer: [],
      waiting_list: %{},
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
  def handle_info(:flush_check, state) do
    # Time-based flush: if buffer has old data, flush it all
    state = maybe_time_based_flush(state)
    schedule_flush_check()
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def terminate(_reason, state) do
    # Force flush any remaining buffered data on shutdown
    state = flush_all(state)

    case ChunkWriter.flush(state.chunk_writer) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
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
    if should_flush?(state.buffer, state.latest_version, state.version_gap) do
      do_flush(state)
    else
      state
    end
  end

  defp should_flush?(buffer, latest, gap) do
    {oldest_version, _} = List.last(buffer)
    Version.distance(latest, oldest_version) >= gap
  end

  defp do_flush(state) do
    # Get transactions to flush (oldest entries beyond the gap)
    {to_flush, to_keep} = split_buffer_for_flush(state)

    if to_flush == [] do
      state
    else
      # Add to ChunkWriter (convert binary versions to integers)
      chunk_writer =
        Enum.reduce(to_flush, state.chunk_writer, fn {version, slice}, writer ->
          version_int = Version.to_integer(version)
          {:ok, writer} = ChunkWriter.add_transaction(writer, version_int, slice)
          writer
        end)

      # Force flush the writer
      case ChunkWriter.flush(chunk_writer) do
        {:ok, chunk_writer} ->
          # Report durability to Demux
          # to_flush is newest-first
          {max_flushed_version, _} = hd(to_flush)
          report_durability(state.demux, state.shard_id, max_flushed_version)

          %{state | buffer: to_keep, chunk_writer: chunk_writer, durable_version: max_flushed_version}

        {:error, _reason} ->
          # Keep trying on next push
          %{state | chunk_writer: chunk_writer}
      end
    end
  end

  # Time-based flush: if oldest buffered version is older than flush interval, flush all
  defp maybe_time_based_flush(%{buffer: []} = state), do: state

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
    # Buffer is newest-first, reverse for oldest-first when adding to writer
    to_flush = Enum.reverse(state.buffer)

    chunk_writer =
      Enum.reduce(to_flush, state.chunk_writer, fn {version, slice}, writer ->
        version_int = Version.to_integer(version)
        {:ok, writer} = ChunkWriter.add_transaction(writer, version_int, slice)
        writer
      end)

    case ChunkWriter.flush(chunk_writer) do
      {:ok, chunk_writer} ->
        # Report durability - buffer was newest-first, so head is max version
        {max_flushed_version, _} = hd(state.buffer)
        report_durability(state.demux, state.shard_id, max_flushed_version)

        %{state | buffer: [], chunk_writer: chunk_writer, durable_version: max_flushed_version}

      {:error, _reason} ->
        %{state | chunk_writer: chunk_writer}
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
