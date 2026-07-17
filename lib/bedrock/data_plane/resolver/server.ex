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
      emit_processing: 2,
      emit_completed: 3,
      emit_reply_sent: 3,
      emit_waiting_list: 2,
      emit_waiting_list_inserted: 3,
      emit_waiting_resolved: 3,
      emit_validation_error: 2,
      emit_waiting_list_validation_error: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Validation
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.Time
  alias Bedrock.Internal.WaitingList

  @type reply_fn :: (result :: {:ok, [non_neg_integer()]} | {:error, any()} -> :ok)

  @default_waiting_timeout_ms 30_000

  @spec child_spec(
          opts :: [
            lock_token: Bedrock.lock_token(),
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
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
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
           {lock_token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({lock_token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}) do
    # Monitor the Director - if it dies, this resolver should terminate
    Process.monitor(director)

    then(
      %State{
        lock_token: lock_token,
        conflicts: Conflicts.new(),
        oldest_version: last_version,
        last_version: last_version,
        waiting: %{},
        mode: :running,
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
        {:resolve_transactions, epoch, {_last_version, _next_version}, _transactions, _metadata, _ack, _directives},
        _from,
        t
      )
      when epoch != t.epoch do
    reply(t, {:error, {:epoch_mismatch, expected: t.epoch, received: epoch}})
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, metadata_ack,
         metadata_directives},
        from,
        t
      )
      when t.mode == :running and epoch == t.epoch and last_version == t.last_version do
    emit_received(transactions, next_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        noreply(t,
          continue:
            {:process_ready,
             {next_version, transactions, metadata_per_tx, metadata_ack, metadata_directives, reply_fn(from)}}
        )

      {:error, reason} ->
        emit_validation_error(transactions, reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, metadata_ack,
         metadata_directives},
        from,
        t
      )
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    emit_waiting_list(transactions, next_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        data = {next_version, transactions, metadata_per_tx, metadata_ack, metadata_directives}

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

      {:error, reason} ->
        emit_waiting_list_validation_error(transactions, reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, _next_version}, transactions, _metadata, metadata_ack,
         {_hold?, confirms}},
        _from,
        t
      )
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version < t.last_version do
    # All transactions aborted due to stale version - return a differential
    # metadata window for the proxy. This is a retry of an already-processed
    # batch, so any hold was recorded then (re-holding a confirmed version
    # would wedge it) - but its confirms may be new; apply them (idempotent).
    {t, confirmed_any?} = apply_metadata_confirms(t, confirms)
    {metadata_window, t} = get_metadata_window_for_proxy(t, metadata_ack, confirmed_any?)
    aborted_indices = Enum.to_list(0..(length(transactions) - 1)//1)
    reply(t, {:ok, aborted_indices, metadata_window})
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
  def handle_continue(
        {:process_ready, {next_version, transactions, metadata_per_tx, metadata_ack, {hold?, confirms}, reply_fn}},
        t
      ) do
    emit_processing(transactions, next_version)

    {conflicts, aborted} = resolve(t.conflicts, transactions, next_version)
    t = %{t | conflicts: conflicts, last_version: next_version}
    emit_completed(transactions, aborted, next_version)

    # Fold in confirmed deferred metadata, then either hold this batch
    # (sharded mode - accumulation deferred until the proxy confirms against
    # the merged GLOBAL abort set; any metadata_per_tx is ignored so immediate
    # and deferred accumulation can never double-apply) or accumulate
    # immediately (single-resolver mode - the local abort set IS global).
    {t, confirmed_any?} = apply_metadata_confirms(t, confirms)

    t =
      if hold? do
        %{t | held_metadata_versions: MapSet.put(t.held_metadata_versions, next_version)}
      else
        accumulate_committed_metadata(t, next_version, metadata_per_tx, aborted)
      end

    # Get the differential window for this proxy and record its confirmed progress
    {metadata_window, t} = get_metadata_window_for_proxy(t, metadata_ack, confirmed_any?)

    reply_fn.({:ok, aborted, metadata_window})
    emit_reply_sent(transactions, aborted, next_version)

    case WaitingList.remove(t.waiting, next_version) do
      {updated_waiting, nil} ->
        noreply(%{t | waiting: updated_waiting}, continue: :next_timeout)

      {updated_waiting, {_deadline, reply_fn, {waiting_next_version, transactions, metadata, ack, directives}}} ->
        emit_waiting_resolved(transactions, [], waiting_next_version)

        noreply(%{t | waiting: updated_waiting},
          continue: {:process_ready, {waiting_next_version, transactions, metadata, ack, directives, reply_fn}}
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
          new_conflicts = remove_old_transactions(t.conflicts, Version.subtract(t.last_version, retention_microseconds))
          %{t | conflicts: new_conflicts, last_sweep_time: Time.monotonic_now_in_ms()}
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

  # Accumulates metadata mutations from committed (non-aborted) transactions
  @spec accumulate_committed_metadata(State.t(), Bedrock.version(), [[tuple()]], [non_neg_integer()]) :: State.t()
  defp accumulate_committed_metadata(t, commit_version, metadata_per_tx, aborted) do
    metadata_per_tx
    |> MetadataAccumulator.committed_mutations(MapSet.new(aborted))
    |> case do
      [] -> t
      mutations -> %{t | metadata_window: MetadataAccumulator.append(t.metadata_window, commit_version, mutations)}
    end
  end

  # Folds confirmed deferred metadata (already filtered by the proxy's merged
  # GLOBAL abort set) into the window at the original commit versions.
  # Confirmations for different held versions can arrive out of order, so the
  # insert is sorted; the settled-version cap below guarantees no proxy has
  # been served (or acked) past any still-held version, so a late insert is
  # never behind anyone's ack. Idempotent: only still-held versions apply, so
  # a retried call re-carrying confirms is a no-op. Returns whether any hold
  # was released (derived from the held-set delta).
  @spec apply_metadata_confirms(State.t(), [{Bedrock.version(), [tuple()]}]) ::
          {State.t(), confirmed_any? :: boolean()}
  defp apply_metadata_confirms(t, confirms) do
    updated =
      Enum.reduce(confirms, t, fn {version, mutations}, t ->
        if MapSet.member?(t.held_metadata_versions, version) do
          %{
            t
            | metadata_window: MetadataAccumulator.insert_sorted(t.metadata_window, version, mutations),
              held_metadata_versions: MapSet.delete(t.held_metadata_versions, version)
          }
        else
          t
        end
      end)

    {updated, updated.held_metadata_versions != t.held_metadata_versions}
  end

  # The highest version whose metadata is fully settled: everything at or
  # below it is either accumulated or known metadata-free. Held (deferred,
  # unconfirmed) versions cap it - windows must never let a proxy ack past
  # metadata that could still be confirmed later.
  @spec settled_version(State.t()) :: Bedrock.version()
  defp settled_version(%{held_metadata_versions: held, last_version: last_version}) do
    case Enum.min(held, fn -> nil end) do
      nil -> last_version
      oldest_held -> Version.subtract(oldest_held, 1)
    end
  end

  # Builds the differential metadata window for a proxy and records its
  # confirmed progress.
  #
  # The window covers (from, settled] where `from` is the version the proxy
  # has CONFIRMED applying (its ack) and `settled` is last_version except when
  # deferred metadata is still held (see settled_version/1). Progress advances
  # only via acks, so a reply lost to a call timeout is simply re-sent on the
  # proxy's next call, and windows to concurrently in-flight batches overlap -
  # out-of-order arrival at the proxy is lossless (its per-entry version guard
  # skips already-applied entries).
  #
  # If pruning (or a held-version expiry) has discarded coverage the proxy
  # never confirmed, the window's from_version is the pruned floor - above the
  # proxy's applied version - which the proxy detects as an unrecoverable
  # coverage gap.
  #
  # A window is nil only when there is nothing to report AND settled has
  # reached last_version: while metadata is held (or when this call released a
  # hold), even an empty window is returned so the merged to_version at the
  # proxy honestly reflects this resolver's settled floor and confirmed holds
  # advance the proxy's ack (letting it stop re-sending confirmations).
  @spec get_metadata_window_for_proxy(State.t(), Resolver.metadata_ack(), confirmed_any? :: boolean()) ::
          {Resolver.metadata_window(), State.t()}
  defp get_metadata_window_for_proxy(t, {proxy_id, acked}, confirmed_any?) do
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
    settled = settled_version(t)

    # Entries above the settled floor (confirmed while an older version is
    # still held) are withheld until everything below them settles.
    entries =
      t.metadata_window
      |> MetadataAccumulator.mutations_since(from)
      |> withhold_unsettled(settled, t.last_version)

    window =
      if entries == [] and not gap? and not confirmed_any? and settled == t.last_version,
        do: nil,
        else: {from, settled, entries}

    {window, t}
  end

  # Fast path: with no held versions, settled == last_version and no entry can
  # exceed it.
  defp withhold_unsettled(entries, settled, last_version) when settled == last_version, do: entries
  defp withhold_unsettled(entries, settled, _), do: Enum.take_while(entries, fn {version, _} -> version <= settled end)

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

  # Expires state older than the version retention horizon:
  #
  # - Proxies not seen within the horizon are dropped so a dead (or
  #   pathologically stalled) proxy neither leaks a progress entry nor blocks
  #   window pruning forever. A live proxy calls at least once per empty-batch
  #   interval, far inside the horizon.
  # - Held (deferred, unconfirmed) metadata versions older than the horizon
  #   are dropped so an unconfirmed hold cannot cap window distribution
  #   forever. A hold this old is an invariant breach - its proxy stopped
  #   confirming (it died mid-batch, or is stalled beyond any healthy cadence)
  #   and the metadata MAY have committed - so the expired version is folded
  #   into metadata_pruned_through: every proxy acked below it (necessarily
  #   all of them, since holds cap acks) takes the coverage-gap fail-fast exit
  #   into director-driven recovery, which rebuilds metadata from durable
  #   state, rather than silently missing possibly-committed metadata.
  @spec expire_stale(State.t()) :: State.t()
  defp expire_stale(t) do
    case retention_cutoff(t) do
      nil ->
        t

      cutoff ->
        {expired_holds, live_holds} = MapSet.split_with(t.held_metadata_versions, &(&1 < cutoff))

        %{
          t
          | proxy_progress: Map.filter(t.proxy_progress, fn {_pid, {_acked, seen}} -> seen >= cutoff end),
            held_metadata_versions: live_holds,
            metadata_pruned_through: max_ack(t.metadata_pruned_through, Enum.max(expired_holds, fn -> nil end))
        }
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
