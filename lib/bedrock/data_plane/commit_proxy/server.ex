defmodule Bedrock.DataPlane.CommitProxy.Server do
  @moduledoc """
  GenServer implementation of the Commit Proxy.

  ## Overview

  The Commit Proxy batches transaction requests from clients to optimize throughput while
  maintaining strict consistency guarantees. It coordinates with resolvers for conflict
  detection and logs for durable persistence.

  ## Lifecycle

  1. **Initialization**: Starts in `:locked` mode, waiting for recovery completion
  2. **Recovery**: Director calls `recover_from/5` to provide the routing snapshot and unlock
  3. **Transaction Processing**: Accepts `:commit` calls, batches transactions, and finalizes
  4. **Empty Transaction Timeout**: Creates empty transactions during quiet periods to advance read versions

  ## Batching Strategy

  - **Size-based**: Batches finalize when reaching `max_per_batch` transactions
  - **Time-based**: Batches finalize after `max_latency_in_ms` milliseconds
  - **Immediate**: Single transactions may bypass batching for low-latency processing

  ## Timeout Mechanisms

  - **Fast timeout (0ms)**: Allows GenServer to process any queued `:commit` messages before
    finalizing the current batch, ensuring optimal batching efficiency
  - **Empty transaction timeout**: Creates empty `{nil, %{}}` transactions during quiet periods
    to keep read versions advancing and provide system health checking

  ## Error Handling

  Uses fail-fast recovery model where unrecoverable errors (sequencer unavailable, log failures)
  trigger process exit and Director-coordinated cluster recovery.
  """

  use GenServer

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      apply_finalization_policy: 1,
      add_transaction_to_batch: 4,
      single_transaction_batch: 2
    ]

  import Bedrock.DataPlane.CommitProxy.Finalization, only: [finalize_batch: 2]

  import Bedrock.DataPlane.CommitProxy.Telemetry,
    only: [
      trace_metadata: 0,
      trace_metadata: 1,
      trace_metadata_applied: 2,
      trace_unknown_key_skipped: 1,
      trace_ingress_validation_failed: 1
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Metadata
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Transaction

  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            director: pid(),
            epoch: Bedrock.epoch(),
            lock_token: Bedrock.lock_token(),
            instance: non_neg_integer(),
            sequencer: pid(),
            resolver_layout: ResolverLayout.t(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer(),
            empty_transaction_timeout_ms: non_neg_integer()
          ]
        ) :: Supervisor.child_spec() | no_return()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    instance = opts[:instance] || raise "Missing :instance option"
    # sequencer and resolver_layout can be nil at startup - set via recover_from/5
    sequencer = opts[:sequencer]
    resolver_layout = opts[:resolver_layout]
    max_latency_in_ms = opts[:max_latency_in_ms] || 4
    max_per_batch = opts[:max_per_batch] || 32
    empty_transaction_timeout_ms = opts[:empty_transaction_timeout_ms] || 1_000

    %{
      id: {__MODULE__, cluster, epoch, instance},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token,
            sequencer, resolver_layout}
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init(
          {module(), pid(), Bedrock.epoch(), non_neg_integer(), pos_integer(), non_neg_integer(), binary(), pid(),
           ResolverLayout.t()}
        ) ::
          {:ok, State.t(), timeout()}
  def init(
        {cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token,
         sequencer, resolver_layout}
      ) do
    # Monitor the Director - if it dies, this commit proxy should terminate
    Process.monitor(director)

    trace_metadata(%{cluster: cluster, pid: self()})

    routing_data = RoutingData.new_empty()

    then(
      %State{
        cluster: cluster,
        director: director,
        epoch: epoch,
        max_latency_in_ms: max_latency_in_ms,
        max_per_batch: max_per_batch,
        empty_transaction_timeout_ms: empty_transaction_timeout_ms,
        lock_token: lock_token,
        sequencer: sequencer,
        resolver_layout: resolver_layout,
        routing_data: routing_data
      },
      &{:ok, &1, empty_transaction_timeout_ms}
    )
  end

  @impl true
  @spec terminate(term(), State.t()) :: :ok
  def terminate(_reason, %State{} = t) do
    abort_current_batch(t)
    :ok
  end

  @impl true
  @spec handle_call(
          {:recover_from, binary(), pid(), ResolverLayout.t(), RoutingData.snapshot()}
          | {:commit, Bedrock.epoch(), Bedrock.transaction()}
          | {:commit, Bedrock.epoch(), Bedrock.transaction(), :user | :system}
          | {:apply_metadata_and_route, pos_integer(), Bedrock.version(), term(), term()},
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), timeout() | {:continue, term()}}
  def handle_call(
        {:recover_from, lock_token, sequencer, resolver_layout, routing_snapshot},
        _from,
        %{mode: :locked} = t
      ) do
    if lock_token == t.lock_token do
      routing_data = RoutingData.from_snapshot(routing_snapshot)

      reply(
        %{t | mode: :running, sequencer: sequencer, resolver_layout: resolver_layout, routing_data: routing_data},
        :ok
      )
    else
      reply(t, {:error, :unauthorized})
    end
  end

  def handle_call({:commit, epoch, transaction}, from, t),
    do: handle_call({:commit, epoch, transaction, :user}, from, t)

  def handle_call({:commit, epoch, transaction, commit_mode}, from, %{mode: :running, epoch: epoch} = t)
      when is_binary(transaction) and commit_mode in [:user, :system] do
    case first_rejected_mutation(transaction, commit_mode) do
      nil -> accept_commit(transaction, from, t)
      :invalid_transaction -> reply(t, {:error, :invalid_transaction})
      {reason, key} -> reply(t, {:error, {reason, key}})
    end
  end

  def handle_call({:commit, _epoch, _transaction, _commit_mode}, _from, %{mode: :running} = t),
    do: reply(t, {:error, :wrong_epoch})

  def handle_call({:commit, _epoch, _transaction, _commit_mode}, _from, %{mode: :locked} = t),
    do: reply(t, {:error, :locked})

  # A finalization task asks for its batch's committed metadata to be applied
  # and for the routing snapshot to push with. Requests are served strictly in
  # batch-sequence order - a request whose predecessor has not yet been
  # applied waits - so every batch routes with exactly the metadata at or
  # below its own commit version, its own included. This is FDB's
  # postResolution ordering (apply metadata, then assign mutations to logs,
  # one batch at a time), keyed like FDB's latestLocalCommitBatchLogging on a
  # PROXY-LOCAL sequence: global sequencer versions interleave across
  # proxies, so chaining on them would park forever whenever another proxy
  # took the intervening version. The snapshot handed back is immutable, so
  # the log push itself proceeds in parallel with later batches' applies.
  def handle_call(
        {:apply_metadata_and_route, seq, commit_version, window, deferred},
        from,
        %{mode: :running, routed_seq: prev_seq} = t
      )
      when seq == prev_seq + 1 do
    t = apply_and_route(t, seq, commit_version, window, deferred)
    GenServer.reply(from, {:ok, t.routing_data})
    noreply_resuming_cadence(drain_pending_applies(t))
  end

  def handle_call({:apply_metadata_and_route, seq, commit_version, window, deferred}, from, %{mode: :running} = t) do
    pending = Map.put(t.pending_applies, seq, {from, commit_version, window, deferred})
    noreply_resuming_cadence(%{t | pending_applies: pending})
  end

  def handle_call({:apply_metadata_and_route, _seq, _cv, _window, _deferred}, _from, %{mode: :locked} = t),
    do: reply(t, {:error, :locked})

  defp accept_commit(transaction, from, t) do
    case start_batch_if_needed(t) do
      {:error, reason} ->
        GenServer.reply(from, {:error, :abort})
        exit(reason)

      updated_t ->
        updated_t
        |> add_transaction_to_batch(transaction, reply_fn(from), nil)
        |> apply_finalization_policy()
        |> case do
          {t, nil} ->
            # Use zero timeout to process any pending messages first
            noreply(t, timeout: 0)

          {t, batch} ->
            # Finalize asynchronously and reset for next batch
            t = finalize_batch_async(batch, t)

            maybe_set_empty_transaction_timeout(t)
        end
    end
  end

  # The legal write range depends on who is committing: user commits end at
  # the system boundary, system commits at the end of the keyspace. Keys past
  # the commit's bound belong to no shard the caller may touch. Before this
  # check, finalization silently routed such single-key mutations into the
  # LAST shard (the system shard) via the router's ceiling fallback, and a
  # clear_range starting past the last boundary failed the whole batch -
  # after it had consumed a commit version - stopping the proxy into
  # director recovery. Rejecting at ingress costs the one offending commit.
  # clear_range ends are exclusive, so an end AT the bound is legal.
  #
  # Atomics on system keys are rejected in EVERY mode: the metadata pipeline
  # (Metadata, RoutingData, and every view fed by the commit stream) replays
  # only sets and clears, while the materializer would apply the atomic
  # durably - the two copies of truth would silently diverge.
  #
  # Returns nil when valid, {reason, key} for the offending mutation, or
  # :invalid_transaction when the mutation section decodes but its payload
  # is corrupt (the decode stream raises lazily; unguarded, that would
  # crash the proxy here).
  @spec first_rejected_mutation(Transaction.encoded(), :user | :system) ::
          {:key_out_of_range | :atomic_on_system_key, Bedrock.key()} | :invalid_transaction | nil
  defp first_rejected_mutation(transaction, commit_mode) do
    bound = keyspace_bound(commit_mode)

    case Transaction.mutations(transaction) do
      {:ok, mutations} -> Enum.find_value(mutations, &rejected_mutation(&1, bound))
      {:error, :section_not_found} -> nil
      {:error, _} -> :invalid_transaction
    end
  rescue
    error ->
      trace_ingress_validation_failed(error)
      :invalid_transaction
  end

  defp keyspace_bound(:user), do: Bedrock.end_of_user_keyspace()
  defp keyspace_bound(:system), do: Bedrock.end_of_keyspace()

  defp rejected_mutation({:set, key, _value}, bound), do: key_past_bound(key, bound)
  defp rejected_mutation({:clear, key}, bound), do: key_past_bound(key, bound)

  defp rejected_mutation({:atomic, _op, key, _value}, bound) do
    key_past_bound(key, bound) ||
      if key >= Bedrock.end_of_user_keyspace(), do: {:atomic_on_system_key, key}
  end

  # No catch-all clause: a mutation shape this validator does not know is
  # caught by the rescue above and rejected as :invalid_transaction, so a
  # future mutation type fails closed instead of bypassing the gate.
  defp rejected_mutation({:clear_range, start_key, end_key}, bound) do
    key_past_bound(start_key, bound) || if end_key > bound, do: {:key_out_of_range, end_key}
  end

  defp key_past_bound(key, bound), do: if(key >= bound, do: {:key_out_of_range, key})

  @impl true
  @spec handle_info(
          :timeout
          | {:finalization_failed, term()}
          | {:DOWN, reference(), :process, pid(), term()},
          State.t()
        ) ::
          {:noreply, State.t()}
          | {:noreply, State.t(), timeout()}
          | {:stop, term(), State.t()}
  def handle_info(:timeout, %{batch: nil, mode: :running} = t) do
    empty_transaction = Transaction.empty_transaction()

    case single_transaction_batch(t, empty_transaction) do
      {:ok, batch} ->
        # Send empty batch asynchronously
        t = finalize_batch_async(batch, t)

        maybe_set_empty_transaction_timeout(t)

      {:error, _sequencer_unavailable} ->
        exit({:sequencer_unavailable, :timeout_empty_transaction})
    end
  end

  def handle_info(:timeout, %{batch: nil} = t) do
    noreply(t, timeout: t.empty_transaction_timeout_ms)
  end

  def handle_info(:timeout, %{batch: batch} = t) do
    # Timeout reached - finalize current batch asynchronously
    t = finalize_batch_async(batch, t)

    maybe_set_empty_transaction_timeout(%{t | batch: nil})
  end

  def handle_info({:DOWN, _ref, :process, director_pid, _reason}, %{director: director_pid} = t) do
    # Director has died - this commit proxy should terminate gracefully
    {:stop, :normal, t}
  end

  # A failed finalization cannot be treated as an isolated transaction
  # abort. Version assignment and conflict resolution have already mutated
  # epoch-local state, and some required logs may have fsynced the batch.
  # Stop explicitly so the Director's component monitor initiates recovery.
  def handle_info({:finalization_failed, reason}, t), do: stop(t, reason)

  def handle_info(_msg, t) do
    {:noreply, t}
  end

  # Spawns finalization for a batch and assigns it the next proxy-local batch
  # sequence; returns the updated state. Batches are created and spawned one
  # at a time in this process, so sequence order is commit-version order.
  defp finalize_batch_async(batch, state) do
    trace_meta = trace_metadata()
    server_pid = self()
    seq = state.batch_seq + 1

    %{
      epoch: epoch,
      sequencer: sequencer,
      resolver_layout: resolver_layout,
      metadata: %{version: applied_metadata_version},
      deferred_metadata: deferred_metadata
    } = state

    Task.start_link(fn ->
      trace_metadata(trace_meta)

      case finalize_batch(batch,
             epoch: epoch,
             sequencer: sequencer,
             resolver_layout: resolver_layout,
             # Stable proxy identity (this server, not the per-batch task) plus
             # the metadata version this proxy has confirmed applying - the
             # resolver keys its progress and differential windows off this.
             metadata_ack: {server_pid, applied_metadata_version},
             # Deferred-metadata confirmations for sharded resolvers: committed
             # metadata from earlier batches, re-sent until acked via windows.
             metadata_confirms: deferred_metadata,
             # Serialized apply-and-route: the server folds this batch's
             # committed metadata into its state in commit-version order and
             # returns the immutable routing snapshot the batch pushes with.
             metadata_apply_fn: fn commit_version, window, deferred ->
               GenServer.call(
                 server_pid,
                 {:apply_metadata_and_route, seq, commit_version, window, deferred},
                 :infinity
               )
             end
           ) do
        {:ok, _n_aborts, _n_oks} ->
          :ok

        {:error, reason} ->
          send(server_pid, {:finalization_failed, reason})
      end
    end)

    %{state | batch_seq: seq}
  end

  # Applies one batch's committed metadata - its resolver window plus, in
  # sharded mode, its own globally-committed metadata - and advances the
  # chain. Windows fold into structured metadata AND routing in one step;
  # deferred metadata folds into routing only (the ack must not advance past
  # what the resolvers' windows have confirmed) and is recorded for
  # re-sending as confirmations until a window covers it.
  defp apply_and_route(t, seq, commit_version, window, deferred) do
    t
    |> apply_metadata_window(window)
    |> apply_deferred_metadata(commit_version, deferred)
    |> Map.put(:routed_seq, seq)
  end

  defp drain_pending_applies(t) do
    case Map.pop(t.pending_applies, t.routed_seq + 1) do
      {nil, _pending} ->
        t

      {{from, commit_version, window, deferred}, pending} ->
        t = apply_and_route(%{t | pending_applies: pending}, t.routed_seq + 1, commit_version, window, deferred)
        GenServer.reply(from, {:ok, t.routing_data})
        drain_pending_applies(t)
    end
  end

  defp apply_deferred_metadata(t, _commit_version, nil), do: t

  defp apply_deferred_metadata(t, commit_version, {committed}) do
    t = add_deferred_metadata(t, commit_version, committed)

    if committed == [] do
      t
    else
      %{t | routing_data: RoutingData.apply_mutations(t.routing_data, [{commit_version, committed}])}
    end
  end

  # Folds a resolver metadata window into the proxy's structured metadata AND
  # its routing data, in one step. Requests are served in commit-version chain
  # order, but windows still overlap (each covers (from, to] starting at the
  # version this proxy had confirmed when the batch spawned), so entries at or
  # below the applied version are dropped before application - which keeps the
  # non-idempotent parts of routing data (log index assignment) exact.
  #
  # A window whose from_version exceeds what this proxy has applied means the
  # resolver pruned history this proxy never saw (only possible after this
  # proxy was expired for lagging beyond the retention horizon). Its metadata
  # can never be completed differentially, so fail fast: the director-driven
  # recovery rebuilds proxies with full state.
  @spec apply_metadata_window(
          State.t(),
          {Bedrock.version() | nil, Bedrock.version(), [term()]} | nil
        ) :: State.t() | no_return()
  defp apply_metadata_window(t, nil), do: t

  defp apply_metadata_window(t, {from_version, to_version, entries}) do
    applied_version = t.metadata.version

    if from_version != nil and (applied_version == nil or from_version > applied_version) do
      exit({:metadata_coverage_gap, %{from_version: from_version, applied_version: applied_version}})
    end

    new_entries =
      if applied_version == nil,
        do: entries,
        else: Enum.filter(entries, fn {entry_version, _mutations} -> entry_version > applied_version end)

    {metadata, stats} = Metadata.apply_updates(t.metadata, new_entries)
    routing_data = RoutingData.apply_mutations(t.routing_data, new_entries)

    if stats.applied > 0, do: trace_metadata_applied(stats.applied, stats.families)
    if stats.skipped_keys != [], do: trace_unknown_key_skipped(stats.skipped_keys)

    # The window covers through to_version even when the last entry is older;
    # acking to_version lets the resolver prune its window fully. Out-of-order
    # windows keep this monotone.
    version = if metadata.version == nil or to_version > metadata.version, do: to_version, else: metadata.version

    # A window covering a deferred version proves every resolver folded that
    # confirmation in (in sharded mode the merged to_version is the MINIMUM
    # settled version across resolvers) - stop re-sending it.
    deferred_metadata = Enum.drop_while(t.deferred_metadata, fn {v, _mutations} -> v <= version end)

    %{t | metadata: %{metadata | version: version}, routing_data: routing_data, deferred_metadata: deferred_metadata}
  end

  @spec add_deferred_metadata(State.t(), Bedrock.version(), [term()]) :: State.t()
  defp add_deferred_metadata(t, version, mutations) do
    {earlier, later} = Enum.split_while(t.deferred_metadata, fn {v, _} -> v < version end)
    %{t | deferred_metadata: earlier ++ [{version, mutations} | later]}
  end

  @spec reply_fn(GenServer.from()) :: Batch.reply_fn()
  def reply_fn(from), do: &GenServer.reply(from, &1)

  # Moved to Batching module to avoid duplication

  @spec maybe_set_empty_transaction_timeout(State.t()) :: {:noreply, State.t(), timeout()}
  defp maybe_set_empty_transaction_timeout(%{mode: :running} = t),
    do: noreply(t, timeout: t.empty_transaction_timeout_ms)

  defp maybe_set_empty_transaction_timeout(t), do: noreply(t)

  # Info messages cancel any pending GenServer timeout, so restore the right
  # cadence: an OPEN batch is waiting on a zero timeout to flush (losing it
  # would strand the batch - and its blocked callers - until the next
  # message), otherwise resume the empty-transaction heartbeat.
  @spec noreply_resuming_cadence(State.t()) :: {:noreply, State.t(), timeout()} | {:noreply, State.t()}
  defp noreply_resuming_cadence(%{mode: :running, batch: nil} = t),
    do: noreply(t, timeout: t.empty_transaction_timeout_ms)

  defp noreply_resuming_cadence(%{mode: :running} = t), do: noreply(t, timeout: 0)

  @spec abort_current_batch(State.t()) :: :ok
  defp abort_current_batch(%{batch: nil}), do: :ok

  defp abort_current_batch(%{batch: batch}) do
    batch
    |> Batch.all_callers()
    |> Enum.each(fn reply_fn -> reply_fn.({:error, :abort}) end)
  end
end
