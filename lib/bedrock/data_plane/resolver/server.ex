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
            version_retention_ms: pos_integer(),
            commit_proxy_count: pos_integer()
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
    commit_proxy_count = opts[:commit_proxy_count] || 1

    %{
      id: {__MODULE__, cluster, key_range, epoch},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {last_version, epoch, director, sweep_interval_ms, version_retention_ms, commit_proxy_count}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({last_version, epoch, director, sweep_interval_ms, version_retention_ms, commit_proxy_count}) do
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
        commit_proxy_count: commit_proxy_count,
        last_sweep_time: Time.monotonic_now_in_ms(),
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
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, proxy_id},
        from,
        t
      )
      when epoch == t.epoch and last_version == t.last_version do
    emit_received(transactions, next_version)

    noreply(t,
      continue: {:process_ready, {next_version, transactions, metadata_per_tx, proxy_id, reply_fn(from)}}
    )
  end

  # No clause exists for a same-epoch call with last_version <
  # t.last_version, deliberately: the hazard is structurally precluded.
  # The sequencer hands out a strictly advancing version chain and
  # proxies never retry a resolver call (fail-fast into recovery), so a
  # stale window cannot be re-presented within an epoch. If one ever
  # arrives, the chain has been violated and the FunctionClauseError
  # crashes this resolver into recovery — the correct outcome, reached by
  # construction rather than by a guard for a state that cannot occur.
  @impl true
  def handle_call(
        {:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx, proxy_id},
        from,
        t
      )
      when epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    data = {next_version, transactions, metadata_per_tx, proxy_id}

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
  def handle_continue({:process_ready, {next_version, transactions, metadata_per_tx, proxy_id, reply_fn}}, t) do
    {conflicts, aborted} = resolve(t.conflicts, transactions, next_version)

    t = %{t | conflicts: conflicts, last_version: next_version}

    emit_completed(transactions, aborted, next_version)

    # Record each metadata-carrying transaction's mutations with this
    # resolver's LOCAL verdict; the proxy ANDs verdicts across all resolvers'
    # windows to reach the global verdict (FDB's stateMutations relay).
    t = accumulate_metadata_verdicts(t, next_version, metadata_per_tx, aborted)

    # Serve this proxy its exact window and advance its served floor.
    {metadata_window, t} = serve_window(t, proxy_id)

    reply_fn.({:ok, aborted, metadata_window})

    case WaitingList.remove(t.waiting, next_version) do
      {updated_waiting, nil} ->
        noreply(%{t | waiting: updated_waiting}, continue: :next_timeout)

      {updated_waiting, {_deadline, reply_fn, {waiting_next_version, transactions, metadata, proxy_id}}} ->
        emit_waiting_resolved(transactions, [], waiting_next_version)

        noreply(%{t | waiting: updated_waiting},
          continue: {:process_ready, {waiting_next_version, transactions, metadata, proxy_id, reply_fn}}
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

  # Serves a proxy its exact metadata window - (last_served, last_version] -
  # and advances its served floor. Windows to one proxy tile exactly, so the
  # proxy's applied version always equals the next window's from_version
  # (asserted there); there is no ack, no overlap, and no receiver-side
  # filtering. This is FDB's stateMutations relay: per-proxy lastVersion,
  # exact half-open intervals, verdicts carried per entry.
  @spec serve_window(State.t(), pid()) :: {Resolver.metadata_window(), State.t()}
  defp serve_window(t, proxy_id) do
    from = Map.get(t.last_served, proxy_id)
    entries = MetadataAccumulator.mutations_since(t.metadata_window, from)
    window = {from, t.last_version, entries}

    t = %{t | last_served: Map.put(t.last_served, proxy_id, t.last_version)}

    {window, prune_metadata_window(t)}
  end

  # Prunes entries every proxy has been served (windows are exact, so a
  # served entry can never be requested again) - but only once every one of
  # the epoch's proxies has been served at least once. FDB gates
  # oldestProxyVersion pruning on having heard from all commitProxyCount
  # proxies for the same reason: an entry must survive until the last
  # first-contact window that needs it. A dead proxy freezes pruning only
  # until the Director notices and recovers the epoch.
  @spec prune_metadata_window(State.t()) :: State.t()
  defp prune_metadata_window(t) do
    if map_size(t.last_served) < t.commit_proxy_count do
      t
    else
      floor = t.last_served |> Map.values() |> Enum.min()
      %{t | metadata_window: MetadataAccumulator.prune_through(t.metadata_window, floor)}
    end
  end
end
