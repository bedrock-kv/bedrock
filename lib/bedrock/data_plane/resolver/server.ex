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
  def handle_call({:resolve_transactions, epoch, {_last_version, _next_version}, _transactions, _metadata}, _from, t)
      when epoch != t.epoch do
    reply(t, {:error, {:epoch_mismatch, expected: t.epoch, received: epoch}})
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx}, from, t)
      when t.mode == :running and epoch == t.epoch and last_version == t.last_version do
    emit_received(transactions, next_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        proxy_pid = elem(from, 0)

        noreply(t,
          continue: {:process_ready, {next_version, transactions, metadata_per_tx, proxy_pid, reply_fn(from)}}
        )

      {:error, reason} ->
        emit_validation_error(transactions, reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions, metadata_per_tx}, from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    emit_waiting_list(transactions, next_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        proxy_pid = elem(from, 0)
        data = {next_version, transactions, metadata_per_tx, proxy_pid}

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
  def handle_call({:resolve_transactions, epoch, {last_version, _next_version}, transactions, _metadata}, from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version < t.last_version do
    # All transactions aborted due to stale version - return differential metadata for the proxy
    proxy_pid = elem(from, 0)
    {metadata_updates, t} = get_metadata_updates_for_proxy(t, proxy_pid)
    aborted_indices = Enum.to_list(0..(length(transactions) - 1))
    reply(t, {:ok, aborted_indices, metadata_updates})
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
  def handle_continue({:process_ready, {next_version, transactions, metadata_per_tx, proxy_pid, reply_fn}}, t) do
    emit_processing(transactions, next_version)

    {conflicts, aborted} = resolve(t.conflicts, transactions, next_version)
    t = %{t | conflicts: conflicts, last_version: next_version}
    emit_completed(transactions, aborted, next_version)

    # Accumulate metadata from non-aborted transactions
    t = accumulate_committed_metadata(t, next_version, metadata_per_tx, aborted)

    # Get differential updates for this proxy and update its progress
    {metadata_updates, t} = get_metadata_updates_for_proxy(t, proxy_pid)

    reply_fn.({:ok, aborted, metadata_updates})
    emit_reply_sent(transactions, aborted, next_version)

    case WaitingList.remove(t.waiting, next_version) do
      {updated_waiting, nil} ->
        noreply(%{t | waiting: updated_waiting}, continue: :next_timeout)

      {updated_waiting, {_deadline, reply_fn, {waiting_next_version, transactions, metadata_per_tx, proxy_pid}}} ->
        emit_waiting_resolved(transactions, [], waiting_next_version)

        noreply(%{t | waiting: updated_waiting},
          continue: {:process_ready, {waiting_next_version, transactions, metadata_per_tx, proxy_pid, reply_fn}}
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
    aborted_set = MapSet.new(aborted)

    metadata_per_tx
    |> Enum.with_index()
    |> Enum.reject(fn {_mutations, idx} -> MapSet.member?(aborted_set, idx) end)
    |> Enum.flat_map(fn {mutations, _idx} -> mutations end)
    |> case do
      [] -> t
      mutations -> %{t | metadata_window: MetadataAccumulator.append(t.metadata_window, commit_version, mutations)}
    end
  end

  # Gets differential metadata updates for a proxy and updates its progress
  @spec get_metadata_updates_for_proxy(State.t(), pid()) :: {[MetadataAccumulator.entry()], State.t()}
  defp get_metadata_updates_for_proxy(t, proxy_pid) do
    last_seen = Map.get(t.proxy_progress, proxy_pid)
    updates = MetadataAccumulator.mutations_since(t.metadata_window, last_seen)

    # Update proxy progress to current version
    updated_progress = Map.put(t.proxy_progress, proxy_pid, t.last_version)
    t = %{t | proxy_progress: updated_progress}

    # Prune metadata window based on minimum progress across all proxies
    t = prune_metadata_window(t)

    {updates, t}
  end

  # Prunes the metadata window based on the minimum version seen by any proxy
  @spec prune_metadata_window(State.t()) :: State.t()
  defp prune_metadata_window(%{proxy_progress: progress} = t) when map_size(progress) == 0 do
    t
  end

  defp prune_metadata_window(t) do
    min_version = t.proxy_progress |> Map.values() |> Enum.min()
    updated_window = MetadataAccumulator.prune_before(t.metadata_window, min_version)
    %{t | metadata_window: updated_window}
  end
end
