defmodule Bedrock.DataPlane.Log.Shale.Server do
  @moduledoc false
  use GenServer

  import Bedrock.DataPlane.Log.Shale.ColdStarting, only: [reload_segments_at_path: 1]
  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]

  import Bedrock.DataPlane.Log.Shale.LongPulls,
    only: [
      process_expired_deadlines_for_waiting_pullers: 2,
      try_to_add_to_waiting_pullers: 5,
      determine_timeout_for_next_puller_deadline: 2,
      notify_waiting_pullers: 3
    ]

  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Telemetry
  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  require Logger

  # Retry backoff configuration for resource exhaustion
  @initial_retry_delay_ms 1_000
  @max_retry_delay_ms 30_000
  @max_retry_attempts 10

  @doc false
  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            otp_name: atom(),
            id: Log.id(),
            foreman: pid(),
            path: Path.t(),
            object_storage: module(),
            start_unlocked: boolean(),
            reject_pushes_above_lag_us: non_neg_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)
    path = Keyword.fetch!(opts, :path)
    object_storage = Keyword.fetch!(opts, :object_storage)
    start_unlocked = Keyword.get(opts, :start_unlocked, false)
    reject_pushes_above_lag_us = Keyword.get(opts, :reject_pushes_above_lag_us)

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             cluster,
             otp_name,
             id,
             foreman,
             path,
             object_storage,
             start_unlocked,
             reject_pushes_above_lag_us
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  @spec init({module(), atom(), Log.id(), pid(), Path.t(), module(), boolean(), non_neg_integer() | nil}) ::
          {:ok, State.t(), {:continue, :initialization}}
  def init({cluster, otp_name, id, foreman, path, object_storage, start_unlocked, reject_pushes_above_lag_us}) do
    initial_mode = if start_unlocked, do: :running, else: :locked

    {:ok,
     %State{
       path: path,
       cluster: cluster,
       mode: initial_mode,
       init_state: {:retrying, 1},
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       object_storage: object_storage,
       reject_pushes_above_lag_us: reject_pushes_above_lag_us,
       available_after: Version.zero(),
       oldest_version: Version.zero(),
       last_version: Version.zero()
     }, {:continue, :initialization}}
  end

  @impl true
  @spec handle_continue(
          :initialization
          | {:notify_appended, [{Bedrock.version(), Transaction.encoded()}]}
          | :check_for_expired_pullers
          | :wait_for_next_puller_deadline,
          State.t()
        ) ::
          {:noreply, State.t()} | {:noreply, State.t(), timeout()}
  def handle_continue(:initialization, t) do
    trace_metadata(%{cluster: t.cluster, id: t.id, otp_name: t.otp_name})
    trace_started()

    case do_initialization(t) do
      {:ok, t} ->
        t
        |> Map.put(:init_state, :initialized)
        |> noreply()

      {:error, {:resource_exhausted, reason}} ->
        handle_resource_exhaustion(t, reason)
    end
  end

  @impl true
  def handle_continue({:notify_appended, events}, t) do
    # Wake pullers from the authoritative append events themselves — each
    # event carries its own version and bytes, in predecessor-chain order.
    t
    |> Map.update!(:waiting_pullers, fn waiting_pullers ->
      Enum.reduce(events, waiting_pullers, fn {version, transaction}, acc ->
        notify_waiting_pullers(acc, version, transaction)
      end)
    end)
    |> noreply(continue: :check_for_expired_pullers)
  end

  @impl true
  def handle_continue(:check_for_expired_pullers, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &process_expired_deadlines_for_waiting_pullers(&1, monotonic_now())
    )
    |> noreply(continue: :wait_for_next_puller_deadline)
  end

  @impl true
  def handle_continue(:wait_for_next_puller_deadline, t) do
    t
    |> Map.get(:waiting_pullers)
    |> determine_timeout_for_next_puller_deadline(monotonic_now())
    |> case do
      nil -> noreply(t)
      timeout -> {:noreply, t, timeout}
    end
  end

  @impl true
  @spec handle_info(:timeout | :retry_initialization | {:min_durable_version, pid(), Bedrock.version()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :check_for_expired_pullers}}
  def handle_info(:timeout, t), do: noreply(t, continue: :check_for_expired_pullers)

  def handle_info(:retry_initialization, t) do
    case do_initialization(t) do
      {:ok, t} ->
        Logger.info("Shale initialization succeeded after retry",
          log_id: t.id,
          path: t.path
        )

        t
        |> Map.put(:init_state, :initialized)
        |> noreply()

      {:error, {:resource_exhausted, reason}} ->
        handle_resource_exhaustion(t, reason)
    end
  end

  def handle_info({:min_durable_version, demux, version}, %{mode: :running, demux: demux} = t) do
    t
    |> advance_min_durable_version(version)
    |> noreply()
  end

  # Watermarks from a stale Demux incarnation (pid mismatch after a recovery
  # reset) or outside :running are dropped; confirmations are re-derived from
  # fresh flushes, so losing them is always safe.
  def handle_info({:min_durable_version, _demux, _version}, t), do: noreply(t)

  @impl true
  @spec handle_call(
          {:info, [atom()]}
          | {:lock_for_recovery, Bedrock.epoch()}
          | {:recover_from, [pid()], Bedrock.version(), Bedrock.version()}
          | {:push, binary(), Bedrock.version()}
          | {:push, binary(), Bedrock.version(), Bedrock.version() | nil}
          | {:pull, Bedrock.version(), keyword()}
          | :ping,
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), {:continue, atom()}}
  def handle_call({:info, fact_names}, _, t), do: t |> info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    trace_lock_for_recovery(epoch)

    with {:ok, t} <- lock_for_recovery(t, epoch, director),
         {:ok, info} <- info(t, Log.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:recover_from, source_logs, replay_after, last_inclusive}, {_director, _}, t) do
    trace_recover_from(source_logs, replay_after, last_inclusive)

    case recover_from(t, source_logs, replay_after, last_inclusive) do
      {:ok, t} -> reply(t, {:ok, self()})
      {:error, reason, t} -> reply(t, {:error, {:failed_to_recover, reason}})
    end
  end

  @impl true
  def handle_call({:push, transaction_bytes, expected_version, known_committed_version}, from, %State{} = t) do
    with {:ok, transaction} <- Transaction.validate(transaction_bytes),
         :ok <- validate_has_shard_index(transaction) do
      t
      |> push(expected_version, transaction, from)
      |> apply_push_transition(known_committed_version)
    else
      {:error, _reason} = error -> reply(t, error, continue: :check_for_expired_pullers)
    end
  end

  # Pushes without a known-committed watermark (older callers, tests): the
  # WAL still appends, but cuts stay gated until a watermark arrives.
  @impl true
  def handle_call({:push, transaction_bytes, expected_version}, from, %State{} = t),
    do: handle_call({:push, transaction_bytes, expected_version, nil}, from, t)

  @impl true
  def handle_call({:pull, from_version, opts}, from, t) do
    trace_pull_transactions(from_version, opts)

    case pull(t, from_version, opts) do
      {:ok, t, transactions} ->
        reply(t, {:ok, transactions})

      {:waiting_for, from_version} ->
        t.waiting_pullers
        |> try_to_add_to_waiting_pullers(
          monotonic_now(),
          reply_to_fn(from),
          from_version,
          opts
        )
        |> case do
          {:error, _reason} = error ->
            reply(t, error, continue: :check_for_expired_pullers)

          {:ok, waiting_pullers} ->
            t
            |> Map.put(:waiting_pullers, waiting_pullers)
            |> noreply(continue: :check_for_expired_pullers)
        end

      {:error, _reason} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_call(:ping, _from, t), do: reply(t, :pong)

  @impl true
  def handle_call({:get_shard_server, shard_id}, _from, t) do
    result = Demux.Server.get_shard_server(t.demux, shard_id)
    reply(t, result)
  end

  defp validate_has_shard_index(transaction) do
    case Transaction.shard_index(transaction) do
      {:ok, [_ | _]} ->
        # Non-empty shard_index is valid
        :ok

      {:ok, []} ->
        # Empty shard_index is valid for empty/heartbeat transactions
        # that advance the Lamport clock without mutations
        :ok

      {:ok, nil} ->
        # No shard_index section at all is valid for empty/heartbeat transactions
        :ok

      {:error, _} ->
        {:error, :missing_shard_index}
    end
  end

  # The Server is the sole owner of live effects, applied exactly once
  # from the transition Pushing returned: every caller reply is issued
  # here (tokens are the callers' `from`s), every append event forwards
  # to the Demux in predecessor-chain order, and puller notification runs
  # from those same authoritative events. The binaries in the events are
  # the exact ref-counted inputs written to the WAL, so forwarding
  # neither slices nor copies them.
  defp apply_push_transition(
         %{state: t, appended: appended, replies: replies, parked?: parked?},
         known_committed_version
       ) do
    Enum.each(replies, fn {from, result} -> GenServer.reply(from, result) end)
    forward_appended_transactions(t.demux, appended, known_committed_version)

    # A parked push carries commit evidence even though nothing appended:
    # the known-committed version still advances the Demux's cut gate.
    if parked?, do: advance_demux_kcv(t.demux, known_committed_version)

    if appended == [] do
      noreply(t, continue: :check_for_expired_pullers)
    else
      noreply(t, continue: {:notify_appended, appended})
    end
  end

  defp forward_appended_transactions(nil, _events, _known_committed_version), do: :ok

  defp forward_appended_transactions(demux, events, known_committed_version) do
    Enum.each(events, fn {version, transaction} ->
      Demux.Server.push(demux, version, transaction, known_committed_version)
    end)
  end

  defp advance_demux_kcv(_demux, nil), do: :ok
  defp advance_demux_kcv(nil, _known_committed_version), do: :ok

  defp advance_demux_kcv(demux, known_committed_version) do
    Demux.Server.advance_known_committed_version(demux, known_committed_version)
  end

  @spec check_running(term()) :: {:error, :unavailable}
  def check_running(_t), do: {:error, :unavailable}

  @spec reply_to_fn(GenServer.from()) :: (term() -> :ok)
  def reply_to_fn(from), do: &GenServer.reply(from, &1)

  @spec monotonic_now() :: integer()
  def monotonic_now, do: :erlang.monotonic_time(:millisecond)

  # Transactional cold start: validate the existing WAL set FIRST — it
  # needs no started resources — then acquire the recycler and demux.
  # Every attempt owns what it starts: a failure after acquisition tears
  # the acquired resources down synchronously before returning, so a
  # retrying server never accumulates stray recyclers or demuxes.
  defp do_initialization(t) do
    with {:ok, wal_snapshot} <- load_or_create_segments(t),
         {:ok, recycler_pid} <- start_segment_recycler(t.path),
         {:ok, demux} <- start_demux_or_release_recycler(t, recycler_pid) do
      {available_after, oldest_version, last_version, active_segment, segments} = wal_snapshot

      {:ok,
       t
       |> Map.put(:available_after, available_after)
       |> Map.put(:oldest_version, oldest_version)
       |> Map.put(:last_version, last_version)
       |> Map.put(:active_segment, active_segment)
       |> Map.put(:segments, segments)
       |> Map.put(:segment_recycler, recycler_pid)
       |> Map.put(:demux, demux)}
    end
  end

  # Normalize demux acquisition into one transactional step for the caller:
  # success transfers both resources into State; failure releases the staged
  # recycler before returning the classified error.
  defp start_demux_or_release_recycler(t, recycler_pid) do
    case start_demux(t) do
      {:ok, demux} ->
        {:ok, demux}

      {:error, reason} ->
        :ok = stop_owned_recycler(recycler_pid)
        classify_resource_error(reason)
    end
  end

  defp start_segment_recycler(path) do
    case SegmentRecycler.start_link(
           path: path,
           min_available: 2,
           max_available: 3,
           segment_size: 64 * 1024 * 1024
         ) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> classify_resource_error(reason)
    end
  end

  defp classify_resource_error(reason) when reason in [:emfile, :enfile, :enomem],
    do: {:error, {:resource_exhausted, reason}}

  defp classify_resource_error(reason), do: {:error, reason}

  defp stop_owned_recycler(pid) do
    Process.unlink(pid)
    GenServer.stop(pid, :shutdown, 5_000)
    :ok
  catch
    :exit, _ -> :ok
  end

  defp load_or_create_segments(t) do
    loader = t.segment_loader || (&reload_segments_at_path/1)

    case loader.(t.path) do
      {:ok, []} ->
        {:ok, {Version.zero(), Version.zero(), Version.zero(), nil, []}}

      {:ok, [active_segment | segments]} ->
        active_segment = Segment.ensure_transactions_are_loaded(active_segment)
        last_version = Segment.last_version(active_segment) || active_segment.previous_version
        available_after = List.last([active_segment | segments]).previous_version
        oldest_version = determine_oldest_transaction_version([active_segment | segments], available_after)
        {:ok, {available_after, oldest_version, last_version, active_segment, segments}}

      {:error, {:wal_io, _path, reason}} when reason in [:emfile, :enfile, :enomem] ->
        # Transient resource exhaustion: the caller retries with backoff.
        {:error, {:resource_exhausted, reason}}

      {:error, {:wal_io, path, reason}} ->
        raise "WAL I/O failure at #{path}: #{inspect(reason)}. Check directory permissions and filesystem health."

      {:error, {:wal_format, path, reason}} ->
        raise "Unable to establish WAL replay cursor for #{path}: #{reason}"
    end
  end

  defp start_demux(t), do: DemuxControl.start(t)

  defp advance_min_durable_version(t, incoming_version) do
    # The floor can never pass the WAL tip, whatever a confirmation claims.
    incoming_version = min(incoming_version, t.last_version)

    min_durable_version =
      case t.min_durable_version do
        nil -> incoming_version
        existing when incoming_version > existing -> incoming_version
        existing -> existing
      end

    if min_durable_version == t.min_durable_version do
      t
    else
      t
      |> Map.put(:min_durable_version, min_durable_version)
      |> trim_durable_segments()
    end
  end

  defp trim_durable_segments(%{segment_recycler: nil} = t), do: t
  defp trim_durable_segments(%{segments: []} = t), do: t
  defp trim_durable_segments(%{min_durable_version: nil} = t), do: t

  # Only reachable in :running mode (the watermark handler is the sole
  # caller and it gates on mode). Materializers never hold the WAL back:
  # they catch up from object-storage chunks and snapshots, so the trim
  # floor is object-storage confirmation alone (the Demux watermark).
  defp trim_durable_segments(t) do
    trim_floor = t.min_durable_version
    segments_oldest_first = Enum.reverse(t.segments)

    {segments_to_trim_oldest_first, remaining_segments_oldest_first} =
      Enum.split_while(segments_oldest_first, &segment_fully_durable?(&1, trim_floor))

    Enum.each(segments_to_trim_oldest_first, fn segment ->
      :ok = Segment.return_to_recycler(segment, t.segment_recycler)
    end)

    remaining_segments = Enum.reverse(remaining_segments_oldest_first)
    lag_us = Version.distance(t.last_version, trim_floor)

    trace_trim(
      trim_floor,
      t.last_version,
      lag_us,
      length(segments_to_trim_oldest_first),
      length(remaining_segments) + 1
    )

    available_after = determine_available_after(t.active_segment, remaining_segments)

    t
    |> Map.put(:segments, remaining_segments)
    |> Map.put(:available_after, available_after)
    |> Map.put(
      :oldest_version,
      determine_oldest_transaction_version([t.active_segment | remaining_segments], available_after)
    )
  end

  # The optional WAL backpressure hard limit is enforced in Pushing,
  # against each transaction's own prospective commit version — including
  # entries drained from the pending queue.

  defp segment_fully_durable?(segment, min_durable_version) do
    loaded_segment = Segment.ensure_transactions_are_loaded(segment)

    case Segment.last_version(loaded_segment) do
      nil -> true
      last_version -> last_version <= min_durable_version
    end
  end

  defp determine_available_after(nil, []), do: Version.zero()

  defp determine_available_after(active_segment, segments) do
    [active_segment | segments]
    |> Enum.reject(&is_nil/1)
    |> List.last()
    |> Map.fetch!(:previous_version)
  end

  defp determine_oldest_transaction_version(segments, available_after) do
    segments
    |> Enum.reverse()
    |> Enum.find_value(fn segment ->
      segment
      |> Segment.transactions()
      |> List.last()
      |> case do
        nil -> nil
        transaction -> Transaction.commit_version!(transaction)
      end
    end)
    |> Kernel.||(available_after)
  end

  defp handle_resource_exhaustion(t, reason) do
    attempt =
      case t.init_state do
        {:retrying, n} -> n
        :initialized -> 1
      end

    if attempt >= @max_retry_attempts do
      Logger.error("Shale initialization failed: resource exhaustion after #{attempt} attempts",
        log_id: t.id,
        path: t.path,
        reason: reason
      )

      raise "Shale initialization failed after #{attempt} attempts due to #{reason}. Check system resource limits (file descriptors, memory)."
    end

    delay = calculate_retry_delay(attempt)

    Logger.warning("Shale initialization delayed due to resource exhaustion",
      log_id: t.id,
      path: t.path,
      reason: reason,
      attempt: attempt,
      retry_in_ms: delay
    )

    Process.send_after(self(), :retry_initialization, delay)

    t
    |> Map.put(:init_state, {:retrying, attempt + 1})
    |> noreply()
  end

  defp calculate_retry_delay(attempt) do
    # Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 30s
    delay = trunc(@initial_retry_delay_ms * :math.pow(2, attempt - 1))
    min(delay, @max_retry_delay_ms)
  end
end
