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
    only: [trace_metadata: 0, trace_metadata: 1]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy.Batch
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
          | {:apply_metadata_and_route, pos_integer(), Bedrock.version(), term()}
          | :fetch_routing,
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

  def handle_call({:commit, epoch, transaction, commit_mode}, from, %{mode: :running, epoch: epoch} = t)
      when is_binary(transaction) and commit_mode in [:user, :system] do
    accept_commit(transaction, commit_mode, from, t)
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
        {:apply_metadata_and_route, seq, commit_version, window},
        from,
        %{mode: :running, routed_seq: prev_seq} = t
      )
      when seq == prev_seq + 1 do
    t = apply_and_route(t, seq, commit_version, window)
    GenServer.reply(from, {:ok, t.routing_data})
    noreply_resuming_cadence(drain_pending_applies(t))
  end

  def handle_call({:apply_metadata_and_route, seq, commit_version, window}, from, %{mode: :running} = t) do
    pending = Map.put(t.pending_applies, seq, {from, commit_version, window})
    noreply_resuming_cadence(%{t | pending_applies: pending})
  end

  def handle_call({:apply_metadata_and_route, _seq, _cv, _window}, _from, %{mode: :locked} = t),
    do: reply(t, {:error, :locked})

  # Client routing requests (FDB GetKeyServerLocations): answered from the
  # live routing view. Replying resumes the batch cadence - a routing fetch
  # must not swallow an open batch's pending timeout.
  def handle_call(:fetch_routing, from, %{mode: :running} = t) do
    GenServer.reply(from, {:ok, RoutingData.client_projection(t.routing_data)})
    noreply_resuming_cadence(t)
  end

  def handle_call(:fetch_routing, _from, %{mode: :locked} = t), do: reply(t, {:error, :locked})

  # Worker rejoin validation (FDB's storage-server rejoin against the
  # proxy's txnStateStore): one tag-keyed lookup in the live routing view.
  # Same cadence rule as :fetch_routing — the reply must not swallow an
  # open batch's pending timeout.
  def handle_call({:resolve_materializer, tag}, from, %{mode: :running} = t) do
    GenServer.reply(from, RoutingData.resolve_materializer(t.routing_data, tag))
    noreply_resuming_cadence(t)
  end

  def handle_call({:resolve_materializer, _tag}, _from, %{mode: :locked} = t), do: reply(t, {:error, :locked})

  defp accept_commit(transaction, commit_mode, from, t) do
    case start_batch_if_needed(t) do
      {:error, reason} ->
        GenServer.reply(from, {:error, :aborted})
        exit(reason)

      updated_t ->
        updated_t
        |> add_transaction_to_batch(transaction, reply_fn(from), commit_mode)
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

    %{epoch: epoch, sequencer: sequencer, resolver_layout: resolver_layout} = state

    Task.start_link(fn ->
      trace_metadata(trace_meta)

      case finalize_batch(batch,
             epoch: epoch,
             sequencer: sequencer,
             resolver_layout: resolver_layout,
             # Stable proxy identity (this server, not the per-batch task):
             # the resolver keys this proxy's exact windows off it.
             proxy_id: server_pid,
             # Serialized apply-and-route: the server folds this batch's
             # committed metadata into its state in commit-version order and
             # returns the immutable routing snapshot the batch pushes with.
             metadata_apply_fn: fn commit_version, window ->
               GenServer.call(server_pid, {:apply_metadata_and_route, seq, commit_version, window}, :infinity)
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

  # Applies one batch's committed metadata window - which covers through the
  # batch's own version, its own committed metadata included - into
  # structured metadata AND routing in one step, and advances the chain.
  defp apply_and_route(t, seq, _commit_version, window) do
    t
    |> apply_metadata_window(window)
    |> Map.put(:routed_seq, seq)
  end

  defp drain_pending_applies(t) do
    case Map.pop(t.pending_applies, t.routed_seq + 1) do
      {nil, _pending} ->
        t

      {{from, commit_version, window}, pending} ->
        t = apply_and_route(%{t | pending_applies: pending}, t.routed_seq + 1, commit_version, window)
        GenServer.reply(from, {:ok, t.routing_data})
        drain_pending_applies(t)
    end
  end

  # Folds one exact resolver window into the routing view and advances the
  # applied version. Windows tile per proxy and are applied in batch-sequence
  # order, so no filtering or overlap tolerance exists here - the tiling
  # assertion below is the whole protocol contract on this side.
  @spec apply_metadata_window(
          State.t(),
          {Bedrock.version() | nil, Bedrock.version(), [term()]}
        ) :: State.t() | no_return()
  defp apply_metadata_window(t, {from_version, to_version, entries}) do
    # Windows are exact and applied in batch-sequence order, so they tile:
    # this window's from is precisely what this proxy has applied. A mismatch
    # means resolver and proxy disagree about history - unrecoverable
    # differentially, so fail fast into director-driven recovery.
    if from_version != t.applied_version do
      exit({:metadata_coverage_gap, %{from_version: from_version, applied_version: t.applied_version}})
    end

    %{
      t
      | applied_version: to_version,
        routing_data: RoutingData.apply_mutations(t.routing_data, entries)
    }
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
    |> Enum.each(fn reply_fn -> reply_fn.({:error, :aborted}) end)
  end
end
