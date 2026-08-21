defmodule Bedrock.DataPlane.CommitProxy.Finalization do
  @moduledoc """
  Transaction finalization pipeline that handles conflict resolution and log persistence.

  ## Version Chain Integrity

  CRITICAL: This module maintains the Lamport clock version chain established by the sequencer.
  The sequencer provides both `last_commit_version` and `commit_version` as a proper chain link:

  - `last_commit_version`: The actual last committed version from the sequencer
  - `commit_version`: The new version assigned to this batch

  Always use the exact version values provided by the sequencer through the batch to maintain
  proper MVCC conflict detection and transaction ordering. Version gaps can exist due to failed
  transactions, recovery scenarios, or system restarts.
  """

  import Bedrock.DataPlane.CommitProxy.Telemetry,
    only: [
      trace_commit_proxy_batch_started: 3,
      trace_commit_proxy_batch_finished: 4,
      trace_commit_proxy_batch_failed: 3,
      trace_metadata_updates_received: 2,
      trace_ingress_validation_failed: 1
    ]

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.ConflictSharding
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Time

  @type metadata_mutations :: [Bedrock.Internal.TransactionBuilder.Tx.mutation()]

  @typedoc """
  Applies a batch's committed metadata window to the commit proxy's routing
  state, serialized in commit-version order, and returns the routing snapshot
  the batch must push with. The window's entries are plain
  `{version, [mutation]}` - verdicts have already been resolved.
  """
  @type metadata_apply_fn() :: (commit_version :: Bedrock.version(), Resolver.metadata_window() ->
                                  {:ok, RoutingData.t()} | {:error, term()})

  @type resolver_fn() :: (Resolver.ref(),
                          Bedrock.epoch(),
                          Bedrock.version(),
                          Bedrock.version(),
                          [Transaction.encoded()],
                          [metadata_mutations()],
                          keyword() ->
                            {:ok, [non_neg_integer()], Resolver.metadata_window()}
                            | {:error, term()})

  @type log_push_batch_fn() :: (last_commit_version :: Bedrock.version(),
                                transactions_by_log :: %{
                                  Log.id() => Transaction.encoded()
                                },
                                commit_version :: Bedrock.version(),
                                opts :: [
                                  log_services: %{Log.id() => pid() | {atom(), node()}},
                                  timeout: Bedrock.timeout_in_ms(),
                                  async_stream_fn: async_stream_fn()
                                ] ->
                                  :ok | {:error, log_push_error() | recovery_required_error()})

  @type log_push_single_fn() :: (ServiceDescriptor.t(), binary(), Bedrock.version() ->
                                   :ok | {:error, :unavailable})

  @type async_stream_fn() :: (enumerable :: Enumerable.t(), fun :: (term() -> term()), opts :: keyword() ->
                                Enumerable.t())

  @type abort_reply_fn() :: ([Batch.reply_fn()] -> :ok)

  @type success_reply_fn() :: ([{Batch.reply_fn(), non_neg_integer(), non_neg_integer()}], Bedrock.version() -> :ok)

  @type sequencer_notify_fn() :: (Sequencer.ref(), Bedrock.epoch(), Bedrock.version(), opts :: keyword() ->
                                    :ok | {:error, term()})

  @type resolution_error() ::
          :timeout
          | :unavailable
          | {:resolver_unavailable, term()}

  @type storage_coverage_error() ::
          {:storage_team_coverage_error, binary()}

  @type log_push_error() :: {:log_failures, [{Log.id(), term()}]} | :log_push_failed

  @type recovery_required_error() :: {:recovery_required, log_push_error()}

  @type finalization_error() ::
          resolution_error()
          | storage_coverage_error()
          | log_push_error()
          | recovery_required_error()

  # ============================================================================
  # Data Structures
  # ============================================================================

  defmodule FinalizationPlan do
    @moduledoc """
    Pipeline state for transaction finalization using unified transaction storage
    for maximum efficiency and clarity.
    """

    @enforce_keys [
      :transactions,
      :transaction_count,
      :commit_version,
      :last_commit_version
    ]
    defstruct [
      :transactions,
      :transaction_count,
      :commit_version,
      :last_commit_version,
      :known_committed_version,
      routing_data: nil,
      transactions_by_log: %{},
      replied_indices: MapSet.new(),
      aborted_count: 0,
      stage: :initialized,
      error: nil
    ]

    @type t :: %__MODULE__{
            transactions: %{
              non_neg_integer() => {non_neg_integer(), Batch.reply_fn(), Transaction.encoded(), Batch.commit_mode()}
            },
            transaction_count: non_neg_integer(),
            commit_version: Bedrock.version(),
            last_commit_version: Bedrock.version(),
            known_committed_version: Bedrock.version() | nil,
            routing_data: RoutingData.t() | nil,
            transactions_by_log: %{Log.id() => Transaction.encoded()},
            replied_indices: MapSet.t(non_neg_integer()),
            aborted_count: non_neg_integer(),
            stage: atom(),
            error: term() | nil
          }
  end

  # ============================================================================
  # Main Pipeline
  # ============================================================================

  @doc """
  Executes the complete transaction finalization pipeline for a batch of transactions.

  This function processes a batch through a multi-stage pipeline: conflict resolution,
  abort notification, log preparation, log persistence, sequencer notification, and
  success notification. The pipeline maintains transactional consistency by ensuring
  all operations complete successfully or all pending clients are notified of failure.

  ## Pipeline Stages

  1. **Conflict Resolution**: Calls resolvers to determine which transactions must be aborted
  2. **Abort Notification**: Immediately notifies clients of aborted transactions
  3. **Log Preparation**: Distributes successful transaction mutations to appropriate logs
  4. **Log Persistence**: Pushes transactions to ALL log servers and waits for acknowledgment
  5. **Sequencer Notification**: Reports successful commit version to the sequencer
  6. **Success Notification**: Notifies clients of successful transactions with commit version

  ## Metadata Distribution

  During conflict resolution, metadata mutations (keys with \\xFF prefix) are
  extracted from each transaction and sent to every resolver along with the
  stable proxy identity. Each resolver returns the proxy's exact metadata
  window. The batch then makes one
  `metadata_apply_fn` call: the commit proxy server applies the window - plus,
  in sharded mode, the batch's own globally-committed metadata - serialized in
  batch-sequence order, and returns the immutable routing snapshot the batch
  pushes with.

  With SHARDED resolvers no resolver knows the merged global abort set - and
  none needs to: every resolver receives every transaction's metadata and
  records it with its LOCAL verdict; the window merge ANDs the verdicts
  positionally into the exact global verdict (see
  `Bedrock.DataPlane.Resolver`).

  ## Parameters

    - `batch`: Transaction batch with commit version details from the sequencer
    - `opts`: Required configuration (epoch, sequencer, resolver_layout, routing_data)
      plus optional functions for testing and configuration overrides

  ## Returns

    - `{:ok, n_aborts, n_successes}` - Pipeline completed
    - `{:error, finalization_error()}` - Pipeline failed; all pending clients notified of failure

  ## Error Handling

  On any pipeline failure, all transactions that haven't been replied to are automatically
  notified with abort responses before returning the error.
  """
  @spec finalize_batch(
          Batch.t(),
          opts :: [
            epoch: Bedrock.epoch(),
            sequencer: pid(),
            resolver_layout: ResolverLayout.t(),
            resolver_fn: resolver_fn(),
            resolver_timeout_in_ms: non_neg_integer(),
            proxy_id: pid(),
            metadata_apply_fn: metadata_apply_fn(),
            batch_log_push_fn: log_push_batch_fn(),
            abort_reply_fn: abort_reply_fn(),
            success_reply_fn: success_reply_fn(),
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            sequencer_notify_fn: sequencer_notify_fn(),
            timeout: non_neg_integer()
          ]
        ) ::
          {:ok, n_aborts :: non_neg_integer(), n_oks :: non_neg_integer()}
          | {:error, finalization_error()}
  def finalize_batch(batch, opts) do
    trace_commit_proxy_batch_started(batch.commit_version, length(batch.buffer), Time.now_in_ms())

    epoch = Keyword.get(opts, :epoch) || raise "Missing epoch in finalization opts"
    sequencer = Keyword.get(opts, :sequencer) || raise "Missing sequencer in finalization opts"
    resolver_layout = Keyword.get(opts, :resolver_layout) || raise "Missing resolver_layout in finalization opts"

    if Keyword.get(opts, :metadata_apply_fn) == nil, do: raise("Missing metadata_apply_fn in finalization opts")

    fn ->
      batch
      |> create_finalization_plan()
      |> reject_invalid_transactions()
      |> resolve_conflicts(epoch, resolver_layout, opts)
      |> prepare_for_logging()
      |> push_to_logs(opts)
      |> notify_sequencer(sequencer, opts)
      |> notify_successes(opts)
      |> extract_result_or_handle_error(opts)
    end
    |> :timer.tc()
    |> case do
      {n_usec, {:ok, n_aborts, n_oks}} ->
        trace_commit_proxy_batch_finished(batch.commit_version, n_aborts, n_oks, n_usec)
        {:ok, n_aborts, n_oks}

      {n_usec, {:error, reason}} ->
        trace_commit_proxy_batch_failed(batch, reason, n_usec)
        {:error, reason}
    end
  end

  # ============================================================================
  # Pipeline Initialization
  # ============================================================================

  @spec create_finalization_plan(Batch.t()) :: FinalizationPlan.t()
  def create_finalization_plan(batch) do
    # Routing data arrives after resolution: the commit proxy server applies
    # this batch's committed metadata in commit-version order and returns the
    # snapshot the batch routes with (see apply_committed_metadata/4).
    %FinalizationPlan{
      transactions: Map.new(batch.buffer, &{elem(&1, 0), &1}),
      transaction_count: Batch.transaction_count(batch),
      commit_version: batch.commit_version,
      last_commit_version: batch.last_commit_version,
      known_committed_version: batch.known_committed_version,
      stage: :ready_for_resolution
    }
  end

  # ============================================================================
  # Keyspace Validation
  # ============================================================================

  # Per-transaction keyspace validation, in the pipeline rather than at the
  # proxy's serialized accept loop (FDB validates tenant access the same way:
  # per-transaction sendError inside postResolution while the batch
  # proceeds). Each rejected transaction gets its specific error through its
  # own reply and is replaced with an empty transaction, so its conflict
  # ranges never enter resolver history and its mutations never reach a log.
  #
  # The legal write range depends on who is committing - the mode rides each
  # buffer entry: user commits end at the system boundary, system commits at
  # the end of the keyspace. Keys past the commit's bound belong to no shard
  # the caller may touch; before validation existed, single-key mutations
  # were silently routed into the LAST shard and a clear_range past the
  # boundary failed the whole batch. clear_range ends are exclusive, so an
  # end AT the bound is legal.
  #
  # Atomics are bounded like any other mutation, nothing more - FDB parity.
  # The metadata views cannot diverge from an atomic because atomics never
  # enter the metadata stream in the first place: metadata_mutation?/1 admits
  # only sets and clears, exactly FDB's isMetadataMutation.
  @spec reject_invalid_transactions(FinalizationPlan.t()) :: FinalizationPlan.t()
  def reject_invalid_transactions(%FinalizationPlan{transaction_count: 0} = plan), do: plan

  def reject_invalid_transactions(%FinalizationPlan{} = plan) do
    {transactions, replied, n_rejected} =
      Enum.reduce(plan.transactions, {plan.transactions, plan.replied_indices, 0}, fn
        {idx, {idx, reply_fn, transaction, commit_mode}}, {transactions, replied, n_rejected} = acc ->
          case first_rejected_mutation(transaction, commit_mode) do
            nil ->
              acc

            :invalid_transaction ->
              reply_fn.({:error, :invalid_transaction})
              blank = {idx, reply_fn, Transaction.empty_transaction(), commit_mode}
              {Map.put(transactions, idx, blank), MapSet.put(replied, idx), n_rejected + 1}

            {reason, key} ->
              reply_fn.({:error, {reason, key}})
              blank = {idx, reply_fn, Transaction.empty_transaction(), commit_mode}
              {Map.put(transactions, idx, blank), MapSet.put(replied, idx), n_rejected + 1}
          end
      end)

    %{plan | transactions: transactions, replied_indices: replied, aborted_count: plan.aborted_count + n_rejected}
  end

  # Returns nil when valid, {reason, key} for the offending mutation, or
  # :invalid_transaction when the mutation section decodes but its payload is
  # corrupt (the decode stream raises lazily; unguarded, that would crash the
  # finalization task and with it the proxy). No catch-all clause: a mutation
  # shape this validator does not know is caught by the rescue and rejected
  # as :invalid_transaction, so a future mutation type fails closed instead
  # of bypassing the gate.
  @spec first_rejected_mutation(Transaction.encoded(), Batch.commit_mode()) ::
          {:key_out_of_range, Bedrock.key()} | :invalid_transaction | nil
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

  defp rejected_mutation({:atomic, _op, key, _value}, bound), do: key_past_bound(key, bound)

  defp rejected_mutation({:clear_range, start_key, end_key}, bound) do
    key_past_bound(start_key, bound) || if end_key > bound, do: {:key_out_of_range, end_key}
  end

  defp key_past_bound(key, bound), do: if(key >= bound, do: {:key_out_of_range, key})

  # ============================================================================
  # Conflict Resolution
  # ============================================================================

  @spec resolve_conflicts(
          FinalizationPlan.t(),
          Bedrock.epoch(),
          ResolverLayout.t(),
          keyword()
        ) ::
          FinalizationPlan.t()
  # Single-resolver fast path: bypass async_stream overhead
  def resolve_conflicts(
        %FinalizationPlan{stage: :ready_for_resolution, transaction_count: 0} = plan,
        epoch,
        %ResolverLayout.Single{resolver_ref: resolver_ref},
        opts
      ) do
    # Empty batch: call resolver with empty lists
    case call_resolver(
           resolver_ref,
           epoch,
           plan.last_commit_version,
           plan.commit_version,
           [],
           [],
           opts
         ) do
      {:ok, _aborted, metadata_window} ->
        plan = apply_committed_metadata(plan, metadata_window, opts)
        split_and_notify_aborts_with_set(plan, MapSet.new(), opts)

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  def resolve_conflicts(
        %FinalizationPlan{stage: :ready_for_resolution} = plan,
        epoch,
        %ResolverLayout.Single{resolver_ref: resolver_ref},
        opts
      ) do
    # Extract conflict sections and metadata mutations synchronously
    {filtered_transactions, metadata_per_tx} =
      0..(plan.transaction_count - 1)
      |> Enum.map(fn idx ->
        {_idx, _reply_fn, transaction, _commit_mode} = Map.fetch!(plan.transactions, idx)
        conflicts = Transaction.extract_sections!(transaction, [:read_conflicts, :write_conflicts])
        metadata = extract_metadata_mutations(transaction)
        {conflicts, metadata}
      end)
      |> Enum.unzip()

    # Call resolver directly without async_stream
    case call_resolver(
           resolver_ref,
           epoch,
           plan.last_commit_version,
           plan.commit_version,
           filtered_transactions,
           metadata_per_tx,
           opts
         ) do
      {:ok, aborted, metadata_window} ->
        aborted_set = MapSet.new(aborted)
        plan = apply_committed_metadata(plan, metadata_window, opts)
        split_and_notify_aborts_with_set(plan, aborted_set, opts)

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # Sharded multi-resolver path
  #
  # Each resolver only sees its own shard's conflict ranges, so no resolver
  # can know the GLOBAL abort set - and none needs to. Every resolver
  # receives every transaction's metadata mutations and records them with
  # its LOCAL verdict; the merge below ANDs the verdicts positionally across
  # all resolvers' windows. A conflict anywhere vetoes; a resolver holding
  # none of a transaction's ranges contributes a trivially-true verdict - so
  # the AND is exactly the global verdict (FDB's stateMutations relay /
  # applyMetadataEffect). Windows cover through this batch's own version, so
  # the batch's own committed metadata arrives inside the merged window, the
  # same as single-resolver mode.
  def resolve_conflicts(
        %FinalizationPlan{stage: :ready_for_resolution} = plan,
        epoch,
        %ResolverLayout.Sharded{resolver_refs: refs, resolver_ends: ends} = resolver_layout,
        opts
      ) do
    # Build resolvers list from ResolverLayout.Sharded for iteration
    resolvers = Enum.zip(ends, refs)

    {resolver_transaction_map, metadata_per_tx} =
      if plan.transaction_count == 0 do
        {Map.new(resolvers, fn {_key, ref} -> {ref, []} end), []}
      else
        # Create and await resolver tasks within the finalization process
        # Also extract metadata from each transaction
        {maps, metadata_list} =
          0..(plan.transaction_count - 1)
          |> Enum.map(fn idx ->
            {_idx, _reply_fn, transaction, _commit_mode} = Map.fetch!(plan.transactions, idx)
            map = shard_conflicts(transaction, resolver_layout)
            metadata = extract_metadata_mutations(transaction)
            {map, metadata}
          end)
          |> Enum.unzip()

        txn_map =
          Map.new(resolvers, fn {_key, ref} ->
            transactions = Enum.map(maps, &Map.fetch!(&1, ref))
            {ref, transactions}
          end)

        {txn_map, metadata_list}
      end

    case call_all_resolvers_with_map(
           resolver_transaction_map,
           metadata_per_tx,
           epoch,
           plan.last_commit_version,
           plan.commit_version,
           resolvers,
           opts
         ) do
      {:ok, aborted_set, metadata_window} ->
        plan = apply_committed_metadata(plan, metadata_window, opts)
        split_and_notify_aborts_with_set(plan, aborted_set, opts)

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # The commit proxy server applies committed metadata one batch at a time in
  # commit-version order and returns the routing snapshot this batch pushes
  # with - which therefore includes the batch's OWN committed metadata (a
  # shard split in this batch routes this batch's log push). Concurrent
  # batches route from their own immutable snapshots and cannot observe each
  # other mid-application. This is FDB's postResolution ordering: apply
  # metadata, then assign mutations to logs, one batch at a time.
  #
  # The window arrives verdict-carrying (each entry holds {mutations,
  # local_verdict} pairs, already ANDed across resolvers in sharded mode);
  # committed_window/1 keeps only unanimously-committed mutations so the
  # server sees the plain {version, [mutation]} shape.
  @spec apply_committed_metadata(FinalizationPlan.t(), Resolver.metadata_window(), keyword()) ::
          FinalizationPlan.t()
  defp apply_committed_metadata(plan, metadata_window, opts) do
    {_from, _to, entries} = committed = committed_window(metadata_window)

    trace_metadata_updates_received(plan.commit_version, entries)

    metadata_apply_fn = Keyword.fetch!(opts, :metadata_apply_fn)

    case metadata_apply_fn.(plan.commit_version, committed) do
      {:ok, %RoutingData{} = routing_data} ->
        %{plan | stage: :conflicts_resolved, routing_data: routing_data}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # Drops vetoed transactions from a verdict-carrying window and flattens the
  # survivors, yielding plain {version, [mutation]} entries. Entries whose
  # transactions were all vetoed disappear; the window bounds are unchanged
  # (the ack advances on coverage, not content).
  @spec committed_window(Resolver.metadata_window()) ::
          {Bedrock.version() | nil, Bedrock.version(), [{Bedrock.version(), metadata_mutations()}]}
  defp committed_window({from, to, entries}) do
    committed =
      entries
      |> Enum.map(fn {version, transaction_metadata} ->
        mutations =
          transaction_metadata
          |> Enum.filter(fn {_mutations, committed?} -> committed? end)
          |> Enum.flat_map(fn {mutations, true} -> mutations end)

        {version, mutations}
      end)
      |> Enum.reject(fn {_version, mutations} -> mutations == [] end)

    {from, to, committed}
  end

  # Every resolver receives the full metadata_per_tx so each can record
  # verdict-carrying entries (see resolve_conflicts above).
  @spec call_all_resolvers_with_map(
          %{Resolver.ref() => [Transaction.encoded()]},
          [metadata_mutations()],
          Bedrock.epoch(),
          Bedrock.version(),
          Bedrock.version(),
          [{start_key :: Bedrock.key(), Resolver.ref()}],
          keyword()
        ) :: {:ok, MapSet.t(non_neg_integer()), Resolver.metadata_window()} | {:error, term()}
  defp call_all_resolvers_with_map(
         resolver_transaction_map,
         metadata_per_tx,
         epoch,
         last_version,
         commit_version,
         resolvers,
         opts
       ) do
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    timeout = Keyword.get(opts, :timeout, 5_000)

    resolvers
    |> async_stream_fn.(
      fn {_start_key, ref} ->
        # Every resolver must have transactions after task processing
        filtered_transactions = Map.fetch!(resolver_transaction_map, ref)
        call_resolver(ref, epoch, last_version, commit_version, filtered_transactions, metadata_per_tx, opts)
      end,
      timeout: timeout
    )
    |> Enum.reduce_while({:ok, MapSet.new(), nil}, fn
      {:ok, {:ok, aborted, metadata_window}}, {:ok, acc_aborted, acc_window} ->
        {:cont, {:ok, Enum.into(aborted, acc_aborted), merge_metadata_windows(acc_window, metadata_window)}}

      {:ok, {:error, reason}}, _ ->
        {:halt, {:error, reason}}

      {:exit, reason}, _ ->
        {:halt, {:error, {:resolver_exit, reason}}}
    end)
  end

  # Sharded resolvers process every batch in version lockstep and serve the
  # same proxy the same exact window bounds, so merging is pure verdict
  # combination: bounds must MATCH (a mismatch means the resolvers diverged
  # and the batch must fail into recovery rather than guess - FDB's
  # size-consistency ASSERT), and entries at the same version are ANDed
  # positionally into the global verdict.
  @spec merge_metadata_windows(Resolver.metadata_window(), Resolver.metadata_window()) :: Resolver.metadata_window()
  defp merge_metadata_windows(nil, window), do: window

  defp merge_metadata_windows({from, to, entries_a}, {from, to, entries_b}) do
    entries =
      (entries_a ++ entries_b)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {version, metadata_lists} -> {version, and_verdicts(version, metadata_lists)} end)

    {from, to, entries}
  end

  defp merge_metadata_windows({from_a, to_a, _}, {from_b, to_b, _}) do
    raise "resolver metadata windows diverged: (#{inspect(from_a)}, #{inspect(to_a)}] vs " <>
            "(#{inspect(from_b)}, #{inspect(to_b)}]"
  end

  defp and_verdicts(_version, [transaction_metadata]), do: transaction_metadata

  defp and_verdicts(version, [first | _rest] = metadata_lists) do
    if !Enum.all?(metadata_lists, &(length(&1) == length(first))) do
      raise "resolver metadata windows diverged at version #{inspect(version)}"
    end

    Enum.zip_with(metadata_lists, fn pairs ->
      {mutations, _} = hd(pairs)
      {mutations, Enum.all?(pairs, fn {_mutations, committed?} -> committed? end)}
    end)
  end

  @spec call_resolver(
          Resolver.ref(),
          Bedrock.epoch(),
          Bedrock.version(),
          Bedrock.version(),
          [Transaction.encoded()],
          [metadata_mutations()],
          keyword()
        ) :: {:ok, [non_neg_integer()], Resolver.metadata_window()} | {:error, term()}
  # One call, no retries: a resolver that cannot answer means the epoch is
  # over. FDB never retries a resolver (brokenPromiseToNever - the proxy
  # waits; a dead resolver is detected externally and means recovery),
  # because correctness requires every proxy to see an identical metadata
  # stream. The failed batch reports {:resolver_unavailable, reason}, the
  # proxy stops, and the Director recovers the epoch.
  defp call_resolver(ref, epoch, last_version, commit_version, filtered_transactions, metadata_per_tx, opts) do
    resolver_fn = Keyword.get(opts, :resolver_fn, &Resolver.resolve_transactions/7)
    timeout_in_ms = Keyword.get(opts, :resolver_timeout_in_ms, 5_000)

    # The stable proxy identity (the server, not this per-batch task): the
    # resolver keys each proxy's exact window off its served floor.
    proxy_id = Keyword.get(opts, :proxy_id, self())

    case resolver_fn.(ref, epoch, last_version, commit_version, filtered_transactions, metadata_per_tx,
           timeout: timeout_in_ms,
           proxy_id: proxy_id
         ) do
      {:ok, _, _} = success -> success
      {:error, reason} when reason in [:timeout, :unavailable] -> {:error, {:resolver_unavailable, reason}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec extract_metadata_mutations(Transaction.encoded()) :: metadata_mutations()
  defp extract_metadata_mutations(binary_transaction) do
    binary_transaction
    |> Transaction.mutations()
    |> case do
      {:ok, mutations} -> Enum.filter(mutations, &Transaction.metadata_mutation?/1)
      {:error, _} -> []
    end
  end

  @spec shard_conflicts(Transaction.encoded(), ResolverLayout.Sharded.t()) ::
          %{Resolver.ref() => Transaction.encoded()}
  defp shard_conflicts(transaction, %ResolverLayout.Sharded{resolver_refs: refs, resolver_ends: ends}) do
    sections = Transaction.extract_sections!(transaction, [:read_conflicts, :write_conflicts])
    ConflictSharding.shard_conflicts_across_resolvers(sections, ends, refs)
  end

  @spec split_and_notify_aborts_with_set(FinalizationPlan.t(), MapSet.t(non_neg_integer()), keyword()) ::
          FinalizationPlan.t()
  defp split_and_notify_aborts_with_set(%FinalizationPlan{stage: :conflicts_resolved} = plan, aborted_set, opts) do
    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    # Rejected transactions were already answered with their specific error;
    # never let a conflict-abort double-reply them. (Their emptied conflict
    # sections make resolver aborts impossible today - this guard makes the
    # invariant structural rather than incidental.)
    newly_aborted = MapSet.difference(aborted_set, plan.replied_indices)

    newly_aborted
    |> Enum.map(fn idx ->
      {_idx, reply_fn, _binary, _commit_mode} = Map.fetch!(plan.transactions, idx)
      reply_fn
    end)
    |> abort_reply_fn.()

    replied = MapSet.union(plan.replied_indices, newly_aborted)
    %{plan | replied_indices: replied, aborted_count: MapSet.size(replied), stage: :aborts_notified}
  end

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok
  def reply_to_all_clients_with_aborted_transactions(aborts), do: Enum.each(aborts, & &1.({:error, :aborted}))

  # ============================================================================
  # Log Preparation
  # ============================================================================

  @spec prepare_for_logging(FinalizationPlan.t()) :: FinalizationPlan.t()
  def prepare_for_logging(%FinalizationPlan{stage: :failed} = plan), do: plan

  def prepare_for_logging(%FinalizationPlan{stage: :aborts_notified} = plan) do
    log_ids = plan.routing_data.log_map |> Map.values() |> Enum.uniq()

    case build_transactions_for_logs(plan, log_ids) do
      {:ok, transactions_by_log} ->
        %{plan | transactions_by_log: transactions_by_log, stage: :ready_for_logging}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec build_transactions_for_logs(FinalizationPlan.t(), [Log.id()]) ::
          {:ok, %{Log.id() => Transaction.encoded()}} | {:error, term()}
  defp build_transactions_for_logs(plan, log_ids) do
    initial_mutations_by_log = Map.new(log_ids, &{&1, []})

    plan.transactions
    |> Enum.reduce_while(
      {:ok, initial_mutations_by_log},
      fn {idx, entry}, {:ok, acc} ->
        process_transaction_for_logs({idx, entry}, plan, acc)
      end
    )
    |> case do
      {:ok, tagged_mutations_by_log} ->
        result =
          Map.new(tagged_mutations_by_log, fn {log_id, tagged_mutations_list} ->
            encoded = encode_log_transaction(tagged_mutations_list, plan.commit_version)
            {log_id, encoded}
          end)

        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Encode a log transaction with stable sort by shard tag and SHARD_INDEX section
  @spec encode_log_transaction([{term(), non_neg_integer()}], Bedrock.version()) :: Transaction.encoded()
  defp encode_log_transaction(tagged_mutations_list, commit_version) do
    # 1. Stable sort by shard tag (preserves relative order within each shard)
    sorted =
      tagged_mutations_list
      |> Enum.reverse()
      |> Enum.sort_by(fn {_mutation, tag} -> tag_to_integer(tag) end, &<=/2)

    # 2. Build shard index from sorted list
    shard_index = build_shard_index(sorted)

    # 3. Extract just mutations (drop tags)
    mutations = Enum.map(sorted, fn {mutation, _tag} -> mutation end)

    # 4. Encode with shard index
    Transaction.encode(%{
      mutations: mutations,
      commit_version: commit_version,
      shard_index: shard_index
    })
  end

  @spec build_shard_index([{term(), non_neg_integer()}]) :: [{non_neg_integer(), non_neg_integer()}]
  defp build_shard_index([]), do: []

  defp build_shard_index(sorted_tagged_mutations) do
    sorted_tagged_mutations
    |> Enum.chunk_by(fn {_mutation, tag} -> tag_to_integer(tag) end)
    |> Enum.map(fn chunk ->
      {_mutation, tag} = hd(chunk)
      {tag_to_integer(tag), length(chunk)}
    end)
  end

  @spec process_transaction_for_logs(
          {non_neg_integer(), {non_neg_integer(), Batch.reply_fn(), Transaction.encoded(), Batch.commit_mode()}},
          FinalizationPlan.t(),
          %{Log.id() => [term()]}
        ) ::
          {:cont, {:ok, %{Log.id() => [term()]}}}
          | {:halt, {:error, term()}}
  defp process_transaction_for_logs({idx, {_idx, _reply_fn, binary, _commit_mode}}, plan, acc) do
    if MapSet.member?(plan.replied_indices, idx) do
      # Skip transactions that were already replied to (aborted)
      {:cont, {:ok, acc}}
    else
      process_transaction_mutations(binary, plan, acc)
    end
  end

  @spec process_transaction_mutations(binary(), FinalizationPlan.t(), %{Log.id() => [term()]}) ::
          {:cont, {:ok, %{Log.id() => [term()]}}} | {:halt, {:error, term()}}
  defp process_transaction_mutations(binary_transaction, plan, acc) do
    case Transaction.mutations(binary_transaction) do
      {:ok, mutations_stream} ->
        case process_mutations_for_transaction(mutations_stream, plan, acc) do
          {:ok, updated_acc} ->
            {:cont, {:ok, updated_acc}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end

      {:error, :section_not_found} ->
        {:cont, {:ok, acc}}

      {:error, reason} ->
        {:halt, {:error, {:mutation_extraction_failed, reason}}}
    end
  end

  @spec process_mutations_for_transaction(Enumerable.t(), FinalizationPlan.t(), %{Log.id() => [term()]}) ::
          {:ok, %{Log.id() => [term()]}} | {:error, term()}
  defp process_mutations_for_transaction(mutations_stream, plan, acc) do
    Enum.reduce_while(mutations_stream, {:ok, acc}, fn mutation, {:ok, mutations_acc} ->
      distribute_mutation_to_logs_via_shard_router(mutation, plan, mutations_acc)
    end)
  end

  # New routing using ShardRouter with ceiling search and golden ratio
  # Splits cross-shard mutations and stores {mutation, tag} tuples for SHARD_INDEX building
  @spec distribute_mutation_to_logs_via_shard_router(term(), FinalizationPlan.t(), %{Log.id() => [term()]}) ::
          {:cont, {:ok, %{Log.id() => [term()]}}} | {:halt, {:error, term()}}
  defp distribute_mutation_to_logs_via_shard_router(mutation, plan, mutations_acc) do
    %{shards: shards, log_map: log_map, replication_factor: m} = plan.routing_data

    # Split mutation by shards (handles cross-shard clear_range with clamping)
    tagged_mutations = split_mutation_by_shards(mutation, shards)

    if tagged_mutations == [] do
      key_or_range = mutation_to_key_or_range(mutation)
      {:halt, {:error, {:storage_team_coverage_error, key_or_range}}}
    else
      # For each (mutation, tag) pair, find logs and add the tagged mutation
      updated_acc =
        Enum.reduce(tagged_mutations, mutations_acc, fn {split_mutation, tag}, acc ->
          add_tagged_mutation_to_logs({split_mutation, tag}, acc, log_map, m)
        end)

      {:cont, {:ok, updated_acc}}
    end
  end

  # Add a tagged mutation to the appropriate logs
  @spec add_tagged_mutation_to_logs(
          {term(), non_neg_integer()},
          %{Log.id() => [term()]},
          %{non_neg_integer() => Log.id()},
          non_neg_integer()
        ) :: %{Log.id() => [term()]}
  defp add_tagged_mutation_to_logs({mutation, tag}, acc, log_map, m) do
    log_ids = tag |> tag_to_integer() |> ShardRouter.log_ids_for_tag(log_map, m)

    Enum.reduce(log_ids, acc, fn log_id, acc_inner ->
      Map.update!(acc_inner, log_id, &[{mutation, tag} | &1])
    end)
  end

  # Convert tag to integer for golden ratio algorithm
  # Production code uses integer tags, but tests may use strings
  @spec tag_to_integer(term()) :: non_neg_integer()
  defp tag_to_integer(tag) when is_integer(tag), do: tag

  defp tag_to_integer(tag) when is_binary(tag) do
    # Hash string tags to integers
    :erlang.phash2(tag)
  end

  defp tag_to_integer(tag), do: :erlang.phash2(tag)

  @spec mutation_to_key_or_range(
          {:set, Bedrock.key(), Bedrock.value()}
          | {:clear, Bedrock.key()}
          | {:clear_range, Bedrock.key(), Bedrock.key()}
          | {:atomic, atom(), Bedrock.key(), Bedrock.value()}
        ) ::
          Bedrock.key() | {Bedrock.key(), Bedrock.key()}
  def mutation_to_key_or_range({:set, key, _value}), do: key
  def mutation_to_key_or_range({:clear, key}), do: key
  def mutation_to_key_or_range({:clear_range, start_key, end_key}), do: {start_key, end_key}
  def mutation_to_key_or_range({:atomic, _op, key, _value}), do: key

  # ============================================================================
  # Mutation Splitting and Tagging
  # ============================================================================

  # Split mutation by shards (handles cross-shard clear_range)
  # Returns list of {mutation, tag} tuples
  @spec split_mutation_by_shards(term(), RoutingData.shard_tree()) :: [{term(), non_neg_integer()}]
  defp split_mutation_by_shards({:clear_range, start_key, end_key}, shards) do
    overlapping = ShardRouter.lookup_shards_with_ranges(shards, start_key, end_key)

    Enum.map(overlapping, fn {tag, shard_start, shard_end} ->
      # Clamp range to shard boundaries
      clamped_start = max_binary(start_key, shard_start)
      clamped_end = min_binary(end_key, shard_end)
      {{:clear_range, clamped_start, clamped_end}, tag}
    end)
  end

  # Single-key mutations don't split
  defp split_mutation_by_shards({:set, key, _value} = mutation, shards) do
    [{mutation, ShardRouter.lookup_shard(shards, key)}]
  end

  defp split_mutation_by_shards({:clear, key} = mutation, shards) do
    [{mutation, ShardRouter.lookup_shard(shards, key)}]
  end

  defp split_mutation_by_shards({:atomic, _op, key, _value} = mutation, shards) do
    [{mutation, ShardRouter.lookup_shard(shards, key)}]
  end

  # Binary comparison helpers for clamping ranges
  defp max_binary(a, b) when a >= b, do: a
  defp max_binary(_a, b), do: b

  defp min_binary(a, b) when a <= b, do: a
  defp min_binary(_a, b), do: b

  # ============================================================================
  # Log Distribution
  # ============================================================================

  @spec push_to_logs(FinalizationPlan.t(), keyword()) :: FinalizationPlan.t()
  def push_to_logs(%FinalizationPlan{stage: :failed} = plan, _opts), do: plan

  def push_to_logs(%FinalizationPlan{stage: :ready_for_logging} = plan, opts) do
    batch_log_push_fn = Keyword.get(opts, :batch_log_push_fn, &push_transaction_to_logs_direct/4)

    opts_with_log_services =
      opts
      |> Keyword.put(:log_services, plan.routing_data.log_services)
      |> Keyword.put(:known_committed_version, plan.known_committed_version)

    case batch_log_push_fn.(
           plan.last_commit_version,
           plan.transactions_by_log,
           plan.commit_version,
           opts_with_log_services
         ) do
      :ok ->
        %{plan | stage: :logged}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec try_to_push_transaction_to_log(ServiceDescriptor.t(), binary(), Bedrock.version(), Bedrock.version() | nil) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(descriptor, transaction, last_commit_version, known_committed_version \\ nil)

  def try_to_push_transaction_to_log(
        %{kind: :log, status: {:up, log_server}},
        transaction,
        last_commit_version,
        known_committed_version
      ) do
    Log.push(log_server, transaction, last_commit_version, known_committed_version: known_committed_version)
  end

  def try_to_push_transaction_to_log(_, _, _, _), do: {:error, :unavailable}

  @doc """
  Pushes transactions directly to logs and waits for acknowledgement from ALL log servers.

  This function takes transactions that have already been built per log and pushes them
  to the appropriate log servers. Each log receives its pre-built transaction.
  All logs must acknowledge to maintain durability guarantees.

  ## Parameters

    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transactions_by_log`: Map of log_id to transaction for that log.
      May be empty transactions if all transactions were aborted.
    - `commit_version`: The version assigned by the sequencer for this batch.
    - `opts`: Optional configuration for testing and customization.

  ## Options
    - `:log_services` - Map of log_id to service ref (pid or {name, node}) - REQUIRED
    - `:async_stream_fn` - Function for parallel processing (default: Task.async_stream/3)
    - `:log_push_fn` - Function for pushing to individual logs (default: try_to_push_transaction_to_log_direct/3)
    - `:timeout` - Timeout for log push operations (default: 5_000ms)

  ## Returns
    - `:ok` if acknowledgements have been received from ALL log servers.
    - `{:error, log_push_error()}` if any log has not successfully acknowledged the
      push within the timeout period or another non-fatal error occurs.
    - `{:error, recovery_required_error()}` if a log reports that the current
      transaction-system epoch cannot safely continue.
  """
  @spec push_transaction_to_logs_direct(
          last_commit_version :: Bedrock.version(),
          %{Log.id() => Transaction.encoded()},
          commit_version :: Bedrock.version(),
          opts :: [
            log_services: %{Log.id() => pid() | {atom(), node()}},
            async_stream_fn: async_stream_fn(),
            log_push_fn: (pid() | {atom(), node()}, binary(), Bedrock.version() -> :ok | {:error, term()}),
            timeout: non_neg_integer()
          ]
        ) :: :ok | {:error, log_push_error() | recovery_required_error()}
  def push_transaction_to_logs_direct(last_commit_version, transactions_by_log, _commit_version, opts) do
    log_services = Keyword.fetch!(opts, :log_services)
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    known_committed_version = Keyword.get(opts, :known_committed_version)

    log_push_fn =
      Keyword.get(opts, :log_push_fn, fn service_ref, transaction, last_version ->
        try_to_push_transaction_to_log_direct(service_ref, transaction, last_version, known_committed_version)
      end)

    timeout = Keyword.get(opts, :timeout, 5_000)

    required_acknowledgments = map_size(log_services)

    # Task.async_stream is ordered by default, so zipping results with the
    # input log_ids preserves the log_id association even for {:exit, reason}
    # results, whose reason carries no log identity.
    log_ids = Enum.map(log_services, fn {log_id, _service_ref} -> log_id end)

    log_services
    |> async_stream_fn.(
      fn {log_id, service_ref} ->
        encoded_transaction = Map.get(transactions_by_log, log_id)
        result = log_push_fn.(service_ref, encoded_transaction, last_commit_version)
        {log_id, result}
      end,
      timeout: timeout
    )
    |> Enum.zip(log_ids)
    |> Enum.reduce_while({0, []}, fn
      {{:ok, {log_id, {:error, reason}}}, _input_log_id}, {_count, errors} ->
        {:halt, {:error, [{log_id, reason} | errors]}}

      {{:ok, {_log_id, :ok}}, _input_log_id}, {count, errors} ->
        count = 1 + count

        if count == required_acknowledgments do
          {:halt, {:ok, count}}
        else
          {:cont, {count, errors}}
        end

      # Exit reason already tagged with this position's log_id (repeated
      # variable enforces equality) - avoid double-tagging.
      {{:exit, {input_log_id, reason}}, input_log_id}, {_count, errors} ->
        {:halt, {:error, [{input_log_id, reason} | errors]}}

      # Task.async_stream exit shape: {:exit, reason} - attribute the failure
      # to the log at this position in the input order.
      {{:exit, reason}, input_log_id}, {_count, errors} ->
        {:halt, {:error, [{input_log_id, reason} | errors]}}
    end)
    |> case do
      {:ok, ^required_acknowledgments} ->
        :ok

      {:error, errors} ->
        classify_log_failures(errors)

      # The reduce halts on the first error or on reaching the full count, so
      # leaving the loop cleanly means a degenerate push: zero log services,
      # or a stream that ended early. Neither is a per-log failure; both must
      # fail rather than succeed vacuously.
      {_count, _errors} ->
        {:error, :log_push_failed}
    end
  end

  # A log can refuse a version only before the global commit point, but the
  # sequencer and resolvers have already incorporated that scheduled version.
  # Promote the log's signal to an explicit epoch-fatal reason so the commit
  # proxy stops and its Director monitor starts coordinated recovery.
  defp classify_log_failures(errors) do
    log_failures = {:log_failures, errors}

    if Enum.any?(errors, fn
         {_log_id, {:recovery_required, _reason}} -> true
         _error -> false
       end) do
      {:error, {:recovery_required, log_failures}}
    else
      {:error, log_failures}
    end
  end

  @spec try_to_push_transaction_to_log_direct(
          pid() | {atom(), node()},
          binary(),
          Bedrock.version(),
          Bedrock.version() | nil
        ) ::
          :ok | {:error, term()}
  def try_to_push_transaction_to_log_direct(service_ref, transaction, last_commit_version, known_committed_version) do
    Log.push(service_ref, transaction, last_commit_version, known_committed_version: known_committed_version)
  end

  # ============================================================================
  # Sequencer Notification
  # ============================================================================

  @spec notify_sequencer(FinalizationPlan.t(), Sequencer.ref(), keyword()) :: FinalizationPlan.t()
  def notify_sequencer(%FinalizationPlan{stage: :failed} = plan, _sequencer, _opts), do: plan

  def notify_sequencer(%FinalizationPlan{stage: :logged} = plan, sequencer, opts) do
    epoch = Keyword.fetch!(opts, :epoch)
    sequencer_notify_fn = Keyword.get(opts, :sequencer_notify_fn, &Sequencer.report_successful_commit/4)

    case sequencer_notify_fn.(sequencer, epoch, plan.commit_version, []) do
      :ok ->
        %{plan | stage: :sequencer_notified}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # ============================================================================
  # Success Notification
  # ============================================================================

  @spec notify_successes(FinalizationPlan.t(), keyword()) :: FinalizationPlan.t()
  def notify_successes(%FinalizationPlan{stage: :failed} = plan, _opts), do: plan

  def notify_successes(%FinalizationPlan{stage: :sequencer_notified} = plan, opts) do
    success_reply_fn = Keyword.get(opts, :success_reply_fn, &send_reply_with_commit_version_and_index/2)

    successful_entries =
      plan.transactions
      |> Enum.reject(fn {idx, _entry} -> MapSet.member?(plan.replied_indices, idx) end)
      |> Enum.map(fn {idx, {tx_idx, reply_fn, _binary, _commit_mode}} -> {reply_fn, tx_idx, idx} end)

    successful_indices = Enum.map(successful_entries, fn {_reply_fn, _tx_idx, idx} -> idx end)

    success_reply_fn.(successful_entries, plan.commit_version)

    %{plan | replied_indices: MapSet.union(plan.replied_indices, MapSet.new(successful_indices)), stage: :completed}
  end

  @spec send_reply_with_commit_version_and_index(
          [{Batch.reply_fn(), non_neg_integer(), non_neg_integer()}],
          Bedrock.version()
        ) :: :ok
  def send_reply_with_commit_version_and_index(entries, commit_version) do
    Enum.each(entries, fn {reply_fn, tx_idx, _plan_idx} ->
      reply_fn.({:ok, commit_version, tx_idx})
    end)
  end

  # ============================================================================
  # Result Extraction and Error Handling
  # ============================================================================

  # The plan's window-updated routing fields are deliberately NOT returned:
  # the server folds windows into its own routing data (version-guarded, in
  # the same step that advances its ack), so a post-push replacement from a
  # racing task could only clobber newer state (bedrock-q67.24).
  @spec extract_result_or_handle_error(FinalizationPlan.t(), keyword()) ::
          {:ok, non_neg_integer(), non_neg_integer()}
          | {:error, finalization_error()}
  def extract_result_or_handle_error(%FinalizationPlan{stage: :completed} = plan, _opts) do
    n_aborts = plan.aborted_count
    n_successes = plan.transaction_count - n_aborts

    {:ok, n_aborts, n_successes}
  end

  def extract_result_or_handle_error(%FinalizationPlan{stage: :failed} = plan, opts), do: handle_error(plan, opts)

  @spec handle_error(FinalizationPlan.t(), keyword()) :: {:error, finalization_error()}
  defp handle_error(%FinalizationPlan{error: error} = plan, opts) when not is_nil(error) do
    # Table is managed by commit proxy server - no cleanup needed here

    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    # Notify all transactions that haven't been replied to yet
    pending_reply_fns =
      plan.transactions
      |> Enum.reject(fn {idx, _entry} -> MapSet.member?(plan.replied_indices, idx) end)
      |> Enum.map(fn {_idx, {_tx_idx, reply_fn, _binary, _task}} -> reply_fn end)

    abort_reply_fn.(pending_reply_fns)

    {:error, plan.error}
  end
end
