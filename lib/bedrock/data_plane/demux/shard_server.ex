defmodule Bedrock.DataPlane.Demux.ShardServer do
  @moduledoc """
  Per-shard GenServer that buffers transactions and writes to ObjectStorage.

  Each ShardServer handles a single shard's transaction stream:
  1. Receives transaction slices from Demux
  2. Buffers in memory with newest at head
  3. Obeys `{:flush, cut_version}` commands from Demux, persisting everything
     at or below the cut as one chunk via the async persistence queue
  4. Notifies waiting materializers via WaitingList
  5. Reports durability to Demux only after persistence confirmation

  ## Commanded cuts

  ShardServers never decide when to flush. Demux computes deterministic cut
  versions (pure version arithmetic — versions are microsecond timestamps)
  and commands `flush/2`. Because every replica of a shard sees the same
  slices and the same cuts, the resulting chunks are byte-identical, which is
  why `put_if_not_exists` returning `{:error, :already_exists}` counts as a
  successful confirmation.

  On a cut the server reports its **floor contribution** — the cut version
  itself, meaning "everything I have ever seen at or below this version is
  durable" — not the max flushed data version. An empty buffer confirms the
  cut immediately, so idle shards never pin the WAL trim floor.

  Chunks are named for the last commit they contain
  (`c/{shard}/{inverted(max_contained)}`), preserving the cheap
  next-key-after seek contract for readers.

  ## Pull API

  Materializers call `pull/3` to get transactions from a given version. Where
  the reply comes from is decided by one comparison against the durable
  version (the last confirmed cut): at or below it, the chunk range in object
  storage; above it, the buffer. The two regions always meet — buffered
  entries are only evicted once their chunk write confirms — so the stream is
  continuous from any starting position.

  Every reply carries currency — `%{high_water: v, kcv: k}` — so an empty
  reply still means something: "nothing for you, but you are current through
  v" (FoundationDB's empty tag peek shape). Busy shards learn currency from
  slice pushes. Idle shards learn it by subscription, never by timer:
  parking a puller sends the Demux a one-shot currency request carrying what
  this shard has already seen; the Demux replies immediately if it knows
  more, and otherwise holds the interest and answers on its next push. The
  whole chain is event-driven — a reader is never waiting on a clock, only
  on messages that are already in flight.

  ## Ownership

  A ShardServer is anonymous and private to the process that starts it. In
  production that caller is one log replica's Demux, whose shard map is the
  only registry. `start_link/1` records the actual caller as the owner, so
  durability and currency messages cannot be redirected through an option.
  """

  use GenServer

  alias Bedrock.DataPlane.Demux.PersistenceTelemetry
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
  @type currency :: %{high_water: version() | nil, kcv: version() | nil}

  # Default pull timeout (30 seconds)
  @default_pull_timeout 30_000

  # Default pull limit
  @default_pull_limit 100

  @doc """
  Starts an anonymous ShardServer for the given shard, owned by the caller.

  ## Options

  - `:shard_id` - Required. The shard ID this server handles.
  - `:cluster` - Required. Cluster name for ObjectStorage paths.
  - `:object_storage` - Required. ObjectStorage backend.
  - `:persistence_queue_capacity` - Optional. Max queued flush batches (default: 1024).
  - `:persistence_max_retries` - Optional. Retry limit for flush failures (default: 5).
  - `:persistence_retry_backoff_ms` - Optional. Base retry backoff for flush retries (default: 25).
  - `:persistence_retry_tick_ms` - Optional. Retry polling tick for flush retries (default: 25).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    owner = self()
    GenServer.start_link(__MODULE__, Keyword.put(opts, :demux, owner))
  end

  @doc """
  Pushes a transaction slice to the ShardServer, along with the
  known-committed version piggybacked from the commit proxies.

  Called by Demux when a transaction touches this shard.
  This is a cast (async, non-blocking).
  """
  @spec push(GenServer.server(), version(), slice(), kcv :: version() | nil) :: :ok
  def push(server, version, slice, kcv \\ nil) do
    GenServer.cast(server, {:push, version, slice, kcv})
  end

  @doc """
  Commands the ShardServer to make everything at or below `cut_version`
  durable and confirm.

  Called by Demux on deterministic bucket boundaries. This is a cast; the
  confirmation arrives at the Demux tagged with this server's pid once
  persistence is confirmed (immediately, when nothing is buffered at or below
  the cut).
  """
  @spec flush(GenServer.server(), version()) :: :ok
  def flush(server, cut_version) do
    GenServer.cast(server, {:flush, cut_version})
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

  - `{:ok, [{version, slice}], currency}` - Available transactions (possibly
    `[]` when the shard is known current past `from_version` with nothing to
    deliver), with `currency = %{high_water: v, kcv: k}`
  - `{:error, :timeout}` - Nothing learned within timeout
  """
  @spec pull(GenServer.server(), version(), keyword()) ::
          {:ok, [{version(), slice()}], currency()} | {:error, :timeout | term()}
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
  Returns the current durable version (the highest confirmed cut).
  """
  @spec durable_version(GenServer.server()) :: version() | nil
  def durable_version(server) do
    GenServer.call(server, :durable_version)
  end

  # GenServer callbacks

  @known_options ~w[
    shard_id demux cluster object_storage
    persistence_queue_capacity persistence_max_retries
    persistence_retry_backoff_ms persistence_retry_tick_ms
  ]a

  @impl true
  def init(opts) do
    # Refuse unknown options at startup. A silently-ignored option lets
    # configuration written against a removed protocol keep "working" and
    # fail far away as an inscrutable timeout.
    case Keyword.keys(opts) -- @known_options do
      [] -> :ok
      unknown -> raise ArgumentError, "unknown ShardServer options: #{inspect(unknown)}"
    end

    shard_id = Keyword.fetch!(opts, :shard_id)
    demux = Keyword.fetch!(opts, :demux)
    cluster = Keyword.fetch!(opts, :cluster)
    object_storage = Keyword.fetch!(opts, :object_storage)
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
        on_drop: fn payload, reason -> send(owner_pid, {:flush_dropped, payload, reason}) end,
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
      buffer: [],
      waiting_list: %{},
      flush_in_progress: false,
      pending_cuts: [],
      pending_flush_cut: nil,
      durable_version: nil,
      latest_version: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:push, version, slice, kcv}, state) do
    state = do_push(state, version, slice, kcv)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:flush, cut_version}, state) do
    state = %{state | pending_cuts: state.pending_cuts ++ [cut_version]}
    state = process_cuts(state)
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def handle_call({:pull, from_version, limit, timeout}, from, state) do
    case do_pull(state, from_version, limit) do
      {:ok, transactions, state} when transactions != [] ->
        {:reply, {:ok, transactions, currency(state)}, state}

      {:ok, [], state} ->
        if known_current_past?(state, from_version) do
          # Nothing to deliver, but the stream is known current past the
          # requested position: say so, and let the puller advance.
          {:reply, {:ok, [], currency(state)}, state}
        else
          # Nothing known at or after the requested position yet: park, and
          # subscribe to the demux's next currency advance. Event-driven —
          # the reply arrives when the next push does, not when a timer
          # fires.
          reply_fn = fn response -> GenServer.reply(from, response) end

          {waiting_list, _timeout} =
            WaitingList.insert(state.waiting_list, from_version, {from_version, limit}, reply_fn, timeout)

          state = request_currency(%{state | waiting_list: waiting_list})
          {:noreply, state, next_timeout(state)}
        end

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
  def handle_info({:currency, high_water, kcv}, state) do
    state = %{state | high_water: max_version(state.high_water, high_water), kcv: max_version(state.kcv, kcv)}
    state = wake_current_waiters(state)

    # Anyone still parked is waiting for a further advance: re-register so
    # the demux answers us again on its next push.
    state = if map_size(state.waiting_list) > 0, do: request_currency(state), else: state

    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def handle_info({:flush_persisted, cut_version}, state) do
    state = handle_flush_persisted(state, cut_version)
    {:noreply, state, next_timeout(state)}
  end

  @impl true
  def handle_info({:flush_dropped, payload, reason}, state) do
    # A flush that exhausted its retries can never confirm its cut; wedging
    # here would freeze the WAL trim floor silently. Crash instead — the
    # link chain (ShardServer -> Demux -> Log) turns this into a recovery.
    {:stop, {:flush_permanently_failed, %{shard_id: state.shard_id, cut_version: payload.cut_version, reason: reason}},
     state}
  end

  @impl true
  def terminate(_reason, state) do
    # No best-effort flush: the WAL owns shutdown durability, and a partial
    # flush would produce a non-deterministic, un-cut chunk. Stopping the
    # persistence worker synchronously guarantees no chunk write can land
    # after this server is gone. Chunks are never deleted — deterministic
    # replay re-produces byte-identical chunks (see shale/recovery.ex),
    # and the shard-keyed, epoch-spanning chunk history is what lets a
    # recruited or snapshot-restored materializer pull from arbitrarily
    # far back.
    stop_persistence_worker(state.persistence_worker)
    :ok
  end

  defp stop_persistence_worker(pid) do
    # Unlink first: the worker's exit signal would otherwise kill this
    # (non-trapping) server mid-terminate and clobber its own exit reason.
    Process.unlink(pid)
    GenServer.stop(pid, :shutdown, 5_000)
  catch
    :exit, _ ->
      # Kill-and-await: a worker wedged inside a storage call must not be
      # left free to complete a write after this server is gone.
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        5_000 -> :ok
      end
  end

  # Private implementation

  defp do_push(state, version, slice, kcv) do
    # Add to buffer (newest at head)
    buffer = [{version, slice} | state.buffer]

    state = %{
      state
      | buffer: buffer,
        latest_version: version,
        high_water: max_version(state.high_water, version),
        kcv: max_version(state.kcv, kcv)
    }

    # Notify waiting materializers
    notify_waiters(state, version)
  end

  defp currency(state), do: %{high_water: state.high_water, kcv: state.kcv}

  defp known_current_past?(%{high_water: nil}, _from_version), do: false
  defp known_current_past?(state, from_version), do: not Version.newer?(from_version, state.high_water)

  # One-shot currency subscription: tell the demux what we've seen; it
  # replies now if it knows more, otherwise on its next push. Duplicate
  # registrations are harmless (the demux tracks interest as a set, and
  # currency advances are monotonic).
  defp request_currency(state) do
    send(state.demux, {:currency_request, self(), state.high_water})
    state
  end

  # A currency advance satisfies every waiter whose start position the
  # high-water has passed — with data if the buffer has any, otherwise with
  # an honest "empty through the high-water".
  defp wake_current_waiters(%{high_water: nil} = state), do: state

  defp wake_current_waiters(state) do
    threshold = Version.increment(state.high_water)
    {waiting_list, entries} = WaitingList.remove_all_less_than(state.waiting_list, threshold)
    state = %{state | waiting_list: waiting_list}

    Enum.each(entries, fn {_deadline, reply_fn, {from_version, limit}} ->
      transactions = get_from_buffer(state.buffer, from_version, limit)
      reply_fn.({:ok, transactions, currency(state)})
    end)

    state
  end

  defp process_cuts(%{flush_in_progress: true} = state), do: state
  defp process_cuts(%{pending_cuts: []} = state), do: state

  defp process_cuts(%{pending_cuts: [cut | rest]} = state) do
    to_flush = Enum.reject(state.buffer, fn {v, _} -> Version.newer?(v, cut) end)

    case to_flush do
      [] ->
        # Nothing buffered at or below the cut: confirm immediately. This is
        # what keeps idle shards from pinning the trim floor.
        state
        |> Map.put(:pending_cuts, rest)
        |> confirm_cut(cut)
        |> process_cuts()

      to_flush ->
        payload = %{
          shard_id: state.shard_id,
          transactions: to_flush,
          cut_version: cut
        }

        # At most one payload is ever in flight (flush_in_progress gates
        # this path), so the queue cannot be full.
        :ok = PersistenceWorker.enqueue(state.persistence_worker, payload)

        # Flushed entries stay in the buffer (still pullable) until the
        # chunk write is confirmed.
        %{state | pending_cuts: rest, flush_in_progress: true, pending_flush_cut: cut}
    end
  end

  defp handle_flush_persisted(state, cut_version) when cut_version != state.pending_flush_cut, do: state

  defp handle_flush_persisted(state, cut_version) do
    buffer = Enum.filter(state.buffer, fn {v, _} -> Version.newer?(v, cut_version) end)

    %{state | buffer: buffer, flush_in_progress: false, pending_flush_cut: nil}
    |> confirm_cut(cut_version)
    |> process_cuts()
  end

  defp confirm_cut(state, cut_version) do
    previous_durable_version = state.durable_version
    durable_version = max_version(previous_durable_version, cut_version)
    state = %{state | durable_version: durable_version}
    maybe_emit_watermark_advanced(state, previous_durable_version, durable_version)
    state
  end

  defp maybe_emit_watermark_advanced(_state, previous_durable_version, durable_version)
       when previous_durable_version == durable_version, do: :ok

  defp maybe_emit_watermark_advanced(state, _previous_durable_version, durable_version) do
    report_durability(state.demux, state.shard_id, durable_version)

    PersistenceTelemetry.emit_watermark_advanced(
      state.shard_id,
      durable_version,
      length(state.buffer)
    )
  end

  # Where the stream reads from is decided against the durable version (the
  # last confirmed cut): above it, the buffer; at or below it, the chunk
  # range — falling through to the buffer only when the chunks are exhausted
  # (which proves nothing exists for this shard between there and the
  # buffer). Checking the buffer first would silently skip the entire chunk
  # range when pulling from an old position.
  defp do_pull(state, from_version, limit) do
    if state.durable_version != nil and Version.newer?(from_version, state.durable_version) do
      {:ok, get_from_buffer(state.buffer, from_version, limit), state}
    else
      case get_from_storage(state, from_version, limit) do
        {:ok, []} -> {:ok, get_from_buffer(state.buffer, from_version, limit), state}
        {:ok, storage_txns} -> {:ok, storage_txns, state}
        {:error, reason} -> {:error, reason}
      end
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
    from_version_int = version_to_integer(from_version)

    transactions =
      state.chunk_reader
      |> ChunkReader.read_from_version(from_version_int, limit: limit)
      |> Enum.map(fn {version_int, slice} -> {Version.from_integer(version_int), slice} end)
      |> Enum.to_list()

    {:ok, transactions}
  rescue
    e in ChunkReader.ReadError -> {:error, {:storage_read_failed, e.reason}}
    # A listing that could not be completed is the same class of fact as
    # a chunk that could not be read, and must report the same SHAPE:
    # operators and telemetry match on the reason, not on an exception
    # struct. Named explicitly so it does not ride the catch-all below,
    # which exists for genuine bugs and should stay visible as such.
    e in ObjectStorage.ListError -> {:error, {:storage_read_failed, e.reason}}
    e -> {:error, {:storage_read_failed, e}}
  end

  defp version_to_integer(<<_::unsigned-big-64>> = version), do: Version.to_integer(version)
  defp version_to_integer(version) when is_integer(version) and version >= 0, do: version

  defp max_version(nil, v), do: v

  defp max_version(v1, v2) do
    case Version.compare(v1, v2) do
      :lt -> v2
      :eq -> v1
      :gt -> v1
    end
  end

  defp persist_flush_payload(owner_pid, payload, object_storage, shard_tag) do
    started_at = System.monotonic_time(:microsecond)
    batch_size = length(payload.transactions)

    case persist_transactions(object_storage, shard_tag, payload.transactions) do
      :ok ->
        duration_us = System.monotonic_time(:microsecond) - started_at
        PersistenceTelemetry.emit_write_ok(payload.shard_id, batch_size, duration_us)
        send(owner_pid, {:flush_persisted, payload.cut_version})
        :ok

      {:error, reason} ->
        duration_us = System.monotonic_time(:microsecond) - started_at
        PersistenceTelemetry.emit_write_error(payload.shard_id, batch_size, duration_us, reason)
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

    # The chunk is named for the last commit it contains — the seek contract
    # readers rely on — never for the cut that triggered the flush.
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

  defp report_durability(demux, shard_id, version) do
    send(demux, {:durable, self(), shard_id, version})
  end

  defp notify_waiters(state, new_version) do
    # Notify all waiters waiting for version <= new_version
    # Use Version.increment to get next version as threshold
    threshold = Version.increment(new_version)
    {waiting_list, entries} = WaitingList.remove_all_less_than(state.waiting_list, threshold)

    Enum.each(entries, fn {_deadline, reply_fn, {from_version, limit}} ->
      # Each waiter gets data from its own start position — never from zero.
      transactions = get_from_buffer(state.buffer, from_version, limit)
      reply_fn.({:ok, transactions, currency(state)})
    end)

    %{state | waiting_list: waiting_list}
  end

  defp next_timeout(%{waiting_list: waiting_list}) do
    WaitingList.next_timeout(waiting_list)
  end
end
