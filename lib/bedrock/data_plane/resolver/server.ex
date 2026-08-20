defmodule Bedrock.DataPlane.Resolver.Server do
  @moduledoc """
  GenServer implementation for the Resolver conflict detection engine.

  Manages resolver state including version ordering through waiting queues.
  Handles out-of-order transaction resolution by queuing later versions until
  earlier ones complete.

  Starts in running mode and is immediately ready to process transaction
  resolution requests.
  """
  use GenServer

  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [resolve: 3, remove_old_transactions: 2]

  import Bedrock.DataPlane.Resolver.Telemetry,
    only: [
      emit_received: 2,
      emit_completed: 3,
      emit_waiting_list_inserted: 3,
      emit_waiting_resolved: 3
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.Time
  alias Bedrock.Internal.WaitingList

  @type reply_fn :: (result :: {:ok, [non_neg_integer()]} | {:error, any()} -> :ok)

  @default_waiting_timeout_ms 30_000

  @spec child_spec(
          opts :: [
            key_range: Bedrock.key_range(),
            epoch: Bedrock.epoch(),
            last_version: Bedrock.version(),
            director: pid(),
            cluster: module(),
            sweep_interval_ms: pos_integer(),
            version_retention_ms: pos_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    key_range = opts[:key_range] || raise "Missing :key_range option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    last_version = opts[:last_version] || Version.zero()
    director = opts[:director] || raise "Missing :director option"
    cluster = opts[:cluster] || raise "Missing :cluster option"
    sweep_interval_ms = opts[:sweep_interval_ms] || 1_000
    version_retention_ms = opts[:version_retention_ms] || 6_000

    %{
      id: {__MODULE__, cluster, key_range, epoch},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {last_version, epoch, director, sweep_interval_ms, version_retention_ms}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({last_version, epoch, director, sweep_interval_ms, version_retention_ms}) do
    # Monitor the Director - if it dies, this resolver should terminate
    Process.monitor(director)

    then(
      %State{
        conflicts: Conflicts.new(last_version),
        last_version: last_version,
        waiting: %{},
        epoch: epoch,
        director: director,
        sweep_interval_ms: sweep_interval_ms,
        version_retention_ms: version_retention_ms,
        last_sweep_time: Time.monotonic_now_in_ms(),
        proxy_progress: %{},
        metadata_window: MetadataAccumulator.new()
      },
      &{:ok, &1}
    )
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {_last_version, _next_version}, _transactions, _metadata, _ack},
        _from,
        t
      )
      when epoch != t.epoch do
    reply(t, {:error, {:epoch_mismatch, expected: t.epoch, received: epoch}})
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, metadata_ack},
        from,
        t
      )
      when epoch == t.epoch and last_version == t.last_version do
    emit_received(transactions, next_version)

    noreply(t,
      continue: {:process_ready, {next_version, transactions, metadata_per_tx, metadata_ack, reply_fn(from)}}
    )
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, metadata_ack},
        from,
        t
      )
      when epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    data = {next_version, transactions, metadata_per_tx, metadata_ack}

    {new_waiting, _timeout} =
      WaitingList.insert(
        t.waiting,
        last_version,
        data,
        reply_fn(from),
        @default_waiting_timeout_ms
      )

    emit_waiting_list_inserted(transactions, new_waiting, next_version)

    noreply(%{t | waiting: new_waiting}, continue: :next_timeout)
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, _transactions, _metadata, metadata_ack},
        _from,
        t
      )
      when epoch == t.epoch and is_binary(last_version) and last_version < t.last_version do
    # A retry of an already-processed batch (its reply was lost): REPLAY the
    # recorded verdict - recomputing or fabricating one could tell clients
    # "aborted" for transactions the system committed. The metadata window is
    # recomputed - it is differential by the caller's ack, so recomputation
    # is the correct, idempotent choice. A verdict pruned past the retention
    # horizon (or never recorded) has no truthful answer: fail explicitly and
    # let the proxy fail fast into recovery.
    case Map.fetch(t.recent_replies, next_version) do
      {:ok, aborted_indices} ->
        {metadata_window, t} = get_metadata_window_for_proxy(t, metadata_ack)
        reply(t, {:ok, aborted_indices, metadata_window})

      :error ->
        reply(t, {:error, :version_beyond_retention})
    end
  end

  @impl true
  def handle_info(:timeout, t) do
    {new_waiting, expired_entries} = WaitingList.expire(t.waiting)

    Enum.each(expired_entries, fn {_deadline, reply_fn, _data} ->
      reply_fn.({:error, :waiting_timeout})
    end)

    noreply(%{t | waiting: new_waiting}, continue: :next_timeout)
  end

  def handle_info({:DOWN, _ref, :process, director_pid, _reason}, %{director: director_pid} = t) do
    # Director has died - this resolver should terminate gracefully
    {:stop, :normal, t}
  end

  def handle_info(_msg, t) do
    {:noreply, t}
  end

  @impl true
  def handle_continue({:process_ready, {next_version, transactions, metadata_per_tx, metadata_ack, reply_fn}}, t) do
    {conflicts, aborted} = resolve(t.conflicts, transactions, next_version)

    t = %{
      t
      | conflicts: conflicts,
        last_version: next_version,
        recent_replies: Map.put(t.recent_replies, next_version, aborted)
    }

    emit_completed(transactions, aborted, next_version)

    # Record each metadata-carrying transaction's mutations with this
    # resolver's LOCAL verdict; the proxy ANDs verdicts across all resolvers'
    # windows to reach the global verdict (FDB's stateMutations relay).
    t = accumulate_metadata_verdicts(t, next_version, metadata_per_tx, aborted)

    # Get the differential window for this proxy and record its confirmed progress
    {metadata_window, t} = get_metadata_window_for_proxy(t, metadata_ack)

    reply_fn.({:ok, aborted, metadata_window})

    case WaitingList.remove(t.waiting, next_version) do
      {updated_waiting, nil} ->
        noreply(%{t | waiting: updated_waiting}, continue: :next_timeout)

      {updated_waiting, {_deadline, reply_fn, {waiting_next_version, transactions, metadata, ack}}} ->
        emit_waiting_resolved(transactions, [], waiting_next_version)

        noreply(%{t | waiting: updated_waiting},
          continue: {:process_ready, {waiting_next_version, transactions, metadata, ack, reply_fn}}
        )
    end
  end

  @impl true
  def handle_continue(:next_timeout, t) do
    timeout = WaitingList.next_timeout(t.waiting)
    time_since_last_sweep = Time.elapsed_monotonic_in_ms(t.last_sweep_time)
    should_sweep = time_since_last_sweep >= t.sweep_interval_ms

    if should_sweep do
      retention_microseconds = t.version_retention_ms * 1000
      current_version_int = Version.to_integer(t.last_version)

      updated_state =
        if current_version_int >= retention_microseconds do
          floor = Version.subtract(t.last_version, retention_microseconds)
          new_conflicts = remove_old_transactions(t.conflicts, floor)
          recent_replies = Map.reject(t.recent_replies, fn {version, _aborted} -> version < floor end)

          %{
            t
            | conflicts: new_conflicts,
              recent_replies: recent_replies,
              last_sweep_time: Time.monotonic_now_in_ms()
          }
        else
          %{t | last_sweep_time: Time.monotonic_now_in_ms()}
        end

      next_timeout = max(0, min(timeout, t.sweep_interval_ms))
      noreply(updated_state, timeout: next_timeout)
    else
      time_until_next_sweep = max(0, t.sweep_interval_ms - time_since_last_sweep)
      next_timeout = min(timeout, time_until_next_sweep)
      noreply(t, timeout: next_timeout)
    end
  end

  @spec reply_fn(GenServer.from()) :: reply_fn()
  defp reply_fn(from), do: &GenServer.reply(from, &1)

  # ===========================================================================
  # Metadata accumulation and distribution helpers
  # ===========================================================================

  # Records each metadata-carrying transaction's mutations with this
  # resolver's LOCAL verdict, in transaction order. Verdict-false entries are
  # recorded too: at the proxy's merge, absence must never be mistaken for a
  # veto, so a veto is always explicit.
  @spec accumulate_metadata_verdicts(State.t(), Bedrock.version(), [[tuple()]], [non_neg_integer()]) :: State.t()
  defp accumulate_metadata_verdicts(t, commit_version, metadata_per_tx, aborted) do
    aborted_set = MapSet.new(aborted)

    metadata_per_tx
    |> Enum.with_index()
    |> Enum.filter(fn {mutations, _idx} -> mutations != [] end)
    |> Enum.map(fn {mutations, idx} -> {mutations, not MapSet.member?(aborted_set, idx)} end)
    |> case do
      [] -> t
      entries -> %{t | metadata_window: MetadataAccumulator.append(t.metadata_window, commit_version, entries)}
    end
  end

  # Builds the differential metadata window for a proxy and records its
  # confirmed progress.
  #
  # The window covers (from, last_version] where `from` is the version the
  # proxy has CONFIRMED applying (its ack). Progress advances only via acks,
  # so a reply lost to a call timeout is simply re-sent on the proxy's next
  # call, and windows to concurrently in-flight batches overlap -
  # out-of-order arrival at the proxy is lossless (the proxy filters entries
  # at or below its applied version).
  #
  # If pruning has discarded coverage the proxy never confirmed, the window's
  # from_version is the pruned floor - above the proxy's applied version -
  # which the proxy detects as an unrecoverable coverage gap.
  @spec get_metadata_window_for_proxy(State.t(), Resolver.metadata_ack()) ::
          {Resolver.metadata_window(), State.t()}
  defp get_metadata_window_for_proxy(t, {proxy_id, acked}) do
    t =
      t
      |> record_proxy_progress(proxy_id, acked)
      |> expire_stale()
      |> prune_metadata_window()

    # Serve the differential from the RECORDED (monotone) ack: a retried call
    # can carry a stale ack, but the proxy's applied version is at least every
    # ack it has ever sent, so entries at or below the recorded ack are
    # already applied there. (The requesting proxy was just recorded, so it
    # cannot have been expired above.)
    {acked, _last_seen} = Map.fetch!(t.proxy_progress, proxy_id)

    floor = t.metadata_pruned_through
    gap? = floor != nil and (acked == nil or acked < floor)
    from = if gap?, do: floor, else: acked

    entries = MetadataAccumulator.mutations_since(t.metadata_window, from)

    window = if entries == [] and not gap?, do: nil, else: {from, t.last_version, entries}

    {window, t}
  end

  @spec record_proxy_progress(State.t(), pid(), Bedrock.version() | nil) :: State.t()
  defp record_proxy_progress(t, proxy_id, acked) do
    updated_progress =
      Map.update(t.proxy_progress, proxy_id, {acked, t.last_version}, fn {prev_acked, _} ->
        {max_ack(prev_acked, acked), t.last_version}
      end)

    %{t | proxy_progress: updated_progress}
  end

  # Acks are monotone per proxy; a retried call may carry a stale ack.
  defp max_ack(nil, acked), do: acked
  defp max_ack(prev, nil), do: prev
  defp max_ack(prev, acked), do: max(prev, acked)

  # Expires proxies not seen within the version retention horizon, so a dead
  # (or pathologically stalled) proxy neither leaks a progress entry nor
  # blocks window pruning forever. A live proxy calls at least once per
  # empty-batch interval, far inside the horizon.
  @spec expire_stale(State.t()) :: State.t()
  defp expire_stale(t) do
    case retention_cutoff(t) do
      nil ->
        t

      cutoff ->
        %{t | proxy_progress: Map.filter(t.proxy_progress, fn {_pid, {_acked, seen}} -> seen >= cutoff end)}
    end
  end

  # The version retention horizon, or nil while the version stream is still
  # inside the first horizon.
  @spec retention_cutoff(State.t()) :: Bedrock.version() | nil
  defp retention_cutoff(t) do
    retention = t.version_retention_ms * 1000

    if Version.to_integer(t.last_version) >= retention do
      Version.subtract(t.last_version, retention)
    end
  end

  # Prunes the metadata window through the minimum version confirmed by every
  # known proxy, CAPPED at the retention horizon. A proxy that has confirmed
  # nothing (nil ack) blocks pruning until it confirms or expires.
  #
  # The cap makes gap detection symmetric with proxy expiry: no entry younger
  # than the retention horizon is ever discarded, so a proxy calling within
  # retention (any live proxy - the empty-batch cadence is far inside it) can
  # never observe a gap. Without it, acks alone could prune an entry before a
  # proxy the resolver has not yet HEARD FROM (epoch start: one proxy commits
  # and acks metadata before another's first call) ever saw it, gap-exiting a
  # healthy proxy. Memory stays bounded: at most one retention horizon of
  # metadata entries is retained beyond what acks allow.
  @spec prune_metadata_window(State.t()) :: State.t()
  defp prune_metadata_window(%{proxy_progress: progress} = t) when map_size(progress) == 0, do: t

  defp prune_metadata_window(t) do
    t.proxy_progress
    |> Map.values()
    |> Enum.map(fn {acked, _seen} -> acked end)
    |> Enum.min()
    |> cap_at_retention_cutoff(t)
    |> case do
      nil ->
        t

      min_acked ->
        # Record the newest ENTRY version being discarded, not min_acked
        # itself: acks are window to_versions (commit-stream versions, coarser
        # than entry versions), so min_acked can run far ahead of the last
        # entry it covers. Using it as the gap floor would fail returning
        # laggards that confirmed every discarded entry - a spurious full
        # recovery. A proxy acked >= this floor has everything discarded.
        discarded = MetadataAccumulator.newest_version_at_or_below(t.metadata_window, min_acked)

        %{
          t
          | metadata_window: MetadataAccumulator.prune_through(t.metadata_window, min_acked),
            metadata_pruned_through: max_ack(t.metadata_pruned_through, discarded)
        }
    end
  end

  defp cap_at_retention_cutoff(nil, _t), do: nil

  defp cap_at_retention_cutoff(min_acked, t) do
    case retention_cutoff(t) do
      nil -> nil
      cutoff -> min(min_acked, cutoff)
    end
  end
end
