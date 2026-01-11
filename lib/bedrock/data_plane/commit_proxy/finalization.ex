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
      trace_metadata_updates_received: 2
    ]

  import Bitwise, only: [<<<: 2]

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.ConflictSharding
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.CommitProxy.Tracing
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Time

  @type metadata_mutations :: [Bedrock.Internal.TransactionBuilder.Tx.mutation()]

  @type resolver_fn() :: (Resolver.ref(),
                          Bedrock.epoch(),
                          Bedrock.version(),
                          Bedrock.version(),
                          [Transaction.encoded()],
                          [metadata_mutations()],
                          keyword() ->
                            {:ok, [non_neg_integer()], [MetadataAccumulator.entry()]}
                            | {:error, term()}
                            | {:failure, :timeout, Resolver.ref()}
                            | {:failure, :unavailable, Resolver.ref()})

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
                                  :ok | {:error, log_push_error()})

  @type log_push_single_fn() :: (ServiceDescriptor.t(), binary(), Bedrock.version() ->
                                   :ok | {:error, :unavailable})

  @type async_stream_fn() :: (enumerable :: Enumerable.t(), fun :: (term() -> term()), opts :: keyword() ->
                                Enumerable.t())

  @type abort_reply_fn() :: ([Batch.reply_fn()] -> :ok)

  @type success_reply_fn() :: ([{Batch.reply_fn(), non_neg_integer(), non_neg_integer()}], Bedrock.version() -> :ok)

  @type timeout_fn() :: (non_neg_integer() -> non_neg_integer())

  @type sequencer_notify_fn() :: (Sequencer.ref(), Bedrock.version() -> :ok | {:error, term()})

  @type resolution_error() ::
          :timeout
          | :unavailable
          | {:resolver_unavailable, term()}

  @type storage_coverage_error() ::
          {:storage_team_coverage_error, binary()}

  @type log_push_error() ::
          {:log_failures, [{Log.id(), term()}]}
          | {:insufficient_acknowledgments, non_neg_integer(), non_neg_integer(), [{Log.id(), term()}]}
          | :log_push_failed

  @type finalization_error() ::
          resolution_error()
          | storage_coverage_error()
          | log_push_error()

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
      :last_commit_version,
      :shard_table,
      :log_map,
      :log_services,
      :replication_factor
    ]
    defstruct [
      :transactions,
      :transaction_count,
      :commit_version,
      :last_commit_version,
      :shard_table,
      :log_map,
      :replication_factor,
      log_services: %{},
      transactions_by_log: %{},
      replied_indices: MapSet.new(),
      aborted_count: 0,
      stage: :initialized,
      error: nil,
      metadata_updates: []
    ]

    @type t :: %__MODULE__{
            transactions: %{
              non_neg_integer() => {non_neg_integer(), Batch.reply_fn(), Transaction.encoded(), Task.t() | nil}
            },
            transaction_count: non_neg_integer(),
            commit_version: Bedrock.version(),
            last_commit_version: Bedrock.version(),
            shard_table: :ets.table(),
            log_map: %{non_neg_integer() => Log.id()},
            log_services: %{Log.id() => {atom(), node()} | pid()},
            replication_factor: pos_integer(),
            transactions_by_log: %{Log.id() => Transaction.encoded()},
            replied_indices: MapSet.t(non_neg_integer()),
            aborted_count: non_neg_integer(),
            stage: atom(),
            error: term() | nil,
            metadata_updates: [MetadataAccumulator.entry()]
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

  During conflict resolution, metadata mutations (keys with \\xFF prefix) are extracted
  from each transaction and sent to the resolver. The resolver returns differential
  metadata updates that should be merged into the caller's metadata state.

  ## Parameters

    - `batch`: Transaction batch with commit version details from the sequencer
    - `transaction_system_layout`: System configuration including resolvers and log servers
    - `metadata`: Current metadata state (list of accumulated metadata entries)
    - `opts`: Optional functions for testing and configuration overrides

  ## Returns

    - `{:ok, n_aborts, n_successes, updated_routing_data}` - Pipeline completed with updated routing
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
            routing_data: RoutingData.t(),
            resolver_fn: resolver_fn(),
            batch_log_push_fn: log_push_batch_fn(),
            abort_reply_fn: abort_reply_fn(),
            success_reply_fn: success_reply_fn(),
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            sequencer_notify_fn: sequencer_notify_fn(),
            timeout: non_neg_integer()
          ]
        ) ::
          {:ok, n_aborts :: non_neg_integer(), n_oks :: non_neg_integer(), updated_routing_data :: RoutingData.t()}
          | {:error, finalization_error()}
  def finalize_batch(batch, opts) do
    trace_commit_proxy_batch_started(batch.commit_version, length(batch.buffer), Time.now_in_ms())

    epoch = Keyword.get(opts, :epoch) || raise "Missing epoch in finalization opts"
    sequencer = Keyword.get(opts, :sequencer) || raise "Missing sequencer in finalization opts"
    resolver_layout = Keyword.get(opts, :resolver_layout) || raise "Missing resolver_layout in finalization opts"
    routing_data = Keyword.get(opts, :routing_data) || raise "Missing routing_data in finalization opts"

    fn ->
      batch
      |> create_finalization_plan(routing_data)
      |> resolve_conflicts(epoch, resolver_layout, opts)
      |> prepare_for_logging()
      |> push_to_logs(opts)
      |> notify_sequencer(sequencer, opts)
      |> notify_successes(opts)
      |> extract_result_or_handle_error(opts)
    end
    |> :timer.tc()
    |> case do
      {n_usec, {:ok, n_aborts, n_oks, updated_routing_data}} ->
        trace_commit_proxy_batch_finished(batch.commit_version, n_aborts, n_oks, n_usec)
        {:ok, n_aborts, n_oks, updated_routing_data}

      {n_usec, {:error, reason}} ->
        trace_commit_proxy_batch_failed(batch, reason, n_usec)
        {:error, reason}
    end
  end

  # ============================================================================
  # Pipeline Initialization
  # ============================================================================

  @spec create_finalization_plan(Batch.t(), RoutingData.t()) :: FinalizationPlan.t()
  def create_finalization_plan(batch, routing_data) do
    # Routing data is passed in from the commit proxy server
    # Table is created once on recovery and kept up-to-date as metadata changes
    %RoutingData{
      shard_table: shard_table,
      log_map: log_map,
      log_services: log_services,
      replication_factor: replication_factor
    } = routing_data

    %FinalizationPlan{
      transactions: Map.new(batch.buffer, &{elem(&1, 0), &1}),
      transaction_count: Batch.transaction_count(batch),
      commit_version: batch.commit_version,
      last_commit_version: batch.last_commit_version,
      shard_table: shard_table,
      log_map: log_map,
      log_services: log_services,
      replication_factor: replication_factor,
      stage: :ready_for_resolution
    }
  end

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
    case call_resolver_with_retry(
           resolver_ref,
           epoch,
           plan.last_commit_version,
           plan.commit_version,
           [],
           [],
           opts
         ) do
      {:ok, _aborted, metadata_updates} ->
        plan = apply_metadata_updates(plan, metadata_updates, opts)
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
        {_idx, _reply_fn, transaction, _task} = Map.fetch!(plan.transactions, idx)
        conflicts = Transaction.extract_sections!(transaction, [:read_conflicts, :write_conflicts])
        metadata = extract_metadata_mutations(transaction)
        {conflicts, metadata}
      end)
      |> Enum.unzip()

    # Call resolver directly without async_stream
    case call_resolver_with_retry(
           resolver_ref,
           epoch,
           plan.last_commit_version,
           plan.commit_version,
           filtered_transactions,
           metadata_per_tx,
           opts
         ) do
      {:ok, aborted, metadata_updates} ->
        aborted_set = MapSet.new(aborted)
        plan = apply_metadata_updates(plan, metadata_updates, opts)
        split_and_notify_aborts_with_set(plan, aborted_set, opts)

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # Sharded multi-resolver path
  # Note: In sharded mode, metadata is extracted but each resolver only sees
  # the metadata relevant to its key range. For simplicity, we pass empty
  # metadata lists to sharded resolvers and don't aggregate metadata updates.
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
            {_idx, _reply_fn, transaction, _task} = Map.fetch!(plan.transactions, idx)
            task = create_resolver_task_in_finalization(transaction, resolver_layout)
            map = Task.await(task, 5000)
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
      {:ok, aborted_set, metadata_updates} ->
        plan = apply_metadata_updates(plan, metadata_updates, opts)
        split_and_notify_aborts_with_set(plan, aborted_set, opts)

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec apply_metadata_updates(FinalizationPlan.t(), [MetadataAccumulator.entry()], keyword()) ::
          FinalizationPlan.t()
  defp apply_metadata_updates(plan, metadata_updates, _opts) do
    trace_metadata_updates_received(plan.commit_version, metadata_updates)

    # Build routing_data from plan fields
    routing_data = %RoutingData{
      shard_table: plan.shard_table,
      log_map: plan.log_map,
      log_services: plan.log_services,
      replication_factor: plan.replication_factor
    }

    # Apply routing mutations (shard_key, layout_log) to routing_data
    updated_routing_data = RoutingData.apply_mutations(routing_data, metadata_updates)

    %{
      plan
      | stage: :conflicts_resolved,
        metadata_updates: metadata_updates,
        log_map: updated_routing_data.log_map,
        log_services: updated_routing_data.log_services
    }
  end

  @spec call_all_resolvers_with_map(
          %{Resolver.ref() => [Transaction.encoded()]},
          [metadata_mutations()],
          Bedrock.epoch(),
          Bedrock.version(),
          Bedrock.version(),
          [{start_key :: Bedrock.key(), Resolver.ref()}],
          keyword()
        ) :: {:ok, MapSet.t(non_neg_integer()), [MetadataAccumulator.entry()]} | {:error, term()}
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
        call_resolver_with_retry(ref, epoch, last_version, commit_version, filtered_transactions, metadata_per_tx, opts)
      end,
      timeout: timeout
    )
    |> Enum.reduce_while({:ok, MapSet.new(), []}, fn
      {:ok, {:ok, aborted, metadata_updates}}, {:ok, acc_aborted, acc_metadata} ->
        {:cont, {:ok, Enum.into(aborted, acc_aborted), acc_metadata ++ metadata_updates}}

      {:ok, {:error, reason}}, _ ->
        {:halt, {:error, reason}}

      {:exit, reason}, _ ->
        {:halt, {:error, {:resolver_exit, reason}}}
    end)
  end

  @spec call_resolver_with_retry(
          Resolver.ref(),
          Bedrock.epoch(),
          Bedrock.version(),
          Bedrock.version(),
          [Transaction.encoded()],
          [metadata_mutations()],
          keyword(),
          non_neg_integer()
        ) :: {:ok, [non_neg_integer()], [MetadataAccumulator.entry()]} | {:error, term()}
  defp call_resolver_with_retry(
         ref,
         epoch,
         last_version,
         commit_version,
         filtered_transactions,
         metadata_per_tx,
         opts,
         attempts_used \\ 0
       ) do
    timeout_fn = Keyword.get(opts, :timeout_fn, &default_timeout_fn/1)
    resolver_fn = Keyword.get(opts, :resolver_fn, &Resolver.resolve_transactions/7)
    max_attempts = Keyword.get(opts, :max_attempts, 3)

    timeout_in_ms = timeout_fn.(attempts_used)

    case resolver_fn.(ref, epoch, last_version, commit_version, filtered_transactions, metadata_per_tx,
           timeout: timeout_in_ms
         ) do
      {:ok, _, _} = success ->
        success

      {:error, reason} when reason in [:timeout, :unavailable] and attempts_used < max_attempts - 1 ->
        Tracing.emit_resolver_retry(max_attempts - attempts_used - 2, attempts_used + 1, reason)

        call_resolver_with_retry(
          ref,
          epoch,
          last_version,
          commit_version,
          filtered_transactions,
          metadata_per_tx,
          opts,
          attempts_used + 1
        )

      {:failure, reason, _ref} when reason in [:timeout, :unavailable] and attempts_used < max_attempts - 1 ->
        Tracing.emit_resolver_retry(max_attempts - attempts_used - 2, attempts_used + 1, reason)

        call_resolver_with_retry(
          ref,
          epoch,
          last_version,
          commit_version,
          filtered_transactions,
          metadata_per_tx,
          opts,
          attempts_used + 1
        )

      {:error, reason} when reason in [:timeout, :unavailable] ->
        Tracing.emit_resolver_max_retries_exceeded(attempts_used + 1, reason)
        {:error, {:resolver_unavailable, reason}}

      {:failure, reason, _ref} when reason in [:timeout, :unavailable] ->
        Tracing.emit_resolver_max_retries_exceeded(attempts_used + 1, reason)
        {:error, {:resolver_unavailable, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec default_timeout_fn(non_neg_integer()) :: non_neg_integer()
  def default_timeout_fn(attempts_used), do: 500 * (1 <<< attempts_used)

  @spec extract_metadata_mutations(Transaction.encoded()) :: metadata_mutations()
  defp extract_metadata_mutations(binary_transaction) do
    binary_transaction
    |> Transaction.mutations()
    |> case do
      {:ok, mutations} -> Enum.filter(mutations, &Transaction.metadata_mutation?/1)
      {:error, _} -> []
    end
  end

  @spec create_resolver_task_in_finalization(Transaction.encoded(), ResolverLayout.Sharded.t()) :: Task.t()
  defp create_resolver_task_in_finalization(transaction, %ResolverLayout.Sharded{
         resolver_refs: refs,
         resolver_ends: ends
       }) do
    Task.async(fn ->
      sections = Transaction.extract_sections!(transaction, [:read_conflicts, :write_conflicts])
      ConflictSharding.shard_conflicts_across_resolvers(sections, ends, refs)
    end)
  end

  @spec split_and_notify_aborts_with_set(FinalizationPlan.t(), MapSet.t(non_neg_integer()), keyword()) ::
          FinalizationPlan.t()
  defp split_and_notify_aborts_with_set(%FinalizationPlan{stage: :conflicts_resolved} = plan, aborted_set, opts) do
    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    # Reply to aborted transactions
    aborted_set
    |> Enum.map(fn idx ->
      {_idx, reply_fn, _binary, _task} = Map.fetch!(plan.transactions, idx)
      reply_fn
    end)
    |> abort_reply_fn.()

    # Track that we've replied to these transactions and count them as aborted
    %{plan | replied_indices: aborted_set, aborted_count: MapSet.size(aborted_set), stage: :aborts_notified}
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
    # Get unique log_ids from log_map
    log_ids = plan.log_map |> Map.values() |> Enum.uniq()

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

  # Build shard index from sorted tagged mutations
  # Returns list of {tag, count} tuples
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
          {non_neg_integer(), {non_neg_integer(), Batch.reply_fn(), Transaction.encoded(), Task.t() | nil}},
          FinalizationPlan.t(),
          %{Log.id() => [term()]}
        ) ::
          {:cont, {:ok, %{Log.id() => [term()]}}}
          | {:halt, {:error, term()}}
  defp process_transaction_for_logs({idx, {_idx, _reply_fn, binary, _task}}, plan, acc) do
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
    %{shard_table: shard_table, log_map: log_map, replication_factor: m} = plan

    # Split mutation by shards (handles cross-shard clear_range with clamping)
    tagged_mutations = split_mutation_by_shards(mutation, shard_table)

    if tagged_mutations == [] do
      key_or_range = mutation_to_key_or_range(mutation)
      {:halt, {:error, {:storage_team_coverage_error, key_or_range}}}
    else
      # For each (mutation, tag) pair, find logs and add the tagged mutation
      n = map_size(log_map)

      updated_acc =
        Enum.reduce(tagged_mutations, mutations_acc, fn {split_mutation, tag}, acc ->
          add_tagged_mutation_to_logs({split_mutation, tag}, acc, log_map, n, m)
        end)

      {:cont, {:ok, updated_acc}}
    end
  end

  # Add a tagged mutation to the appropriate logs
  @spec add_tagged_mutation_to_logs(
          {term(), non_neg_integer()},
          %{Log.id() => [term()]},
          %{non_neg_integer() => Log.id()},
          non_neg_integer(),
          non_neg_integer()
        ) :: %{Log.id() => [term()]}
  defp add_tagged_mutation_to_logs({mutation, tag}, acc, log_map, n, m) do
    numeric_tag = tag_to_integer(tag)

    log_ids =
      numeric_tag
      |> ShardRouter.get_log_indices(n, m)
      |> Enum.map(&Map.fetch!(log_map, &1))

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
  @spec split_mutation_by_shards(term(), :ets.table()) :: [{term(), non_neg_integer()}]
  defp split_mutation_by_shards({:clear_range, start_key, end_key}, shard_table) do
    shards = ShardRouter.lookup_shards_with_ranges(shard_table, start_key, end_key)

    Enum.map(shards, fn {tag, shard_start, shard_end} ->
      # Clamp range to shard boundaries
      clamped_start = max_binary(start_key, shard_start)
      clamped_end = min_binary(end_key, shard_end)
      {{:clear_range, clamped_start, clamped_end}, tag}
    end)
  end

  # Single-key mutations don't split
  defp split_mutation_by_shards({:set, key, _value} = mutation, shard_table) do
    [{mutation, ShardRouter.lookup_shard(shard_table, key)}]
  end

  defp split_mutation_by_shards({:clear, key} = mutation, shard_table) do
    [{mutation, ShardRouter.lookup_shard(shard_table, key)}]
  end

  defp split_mutation_by_shards({:atomic, _op, key, _value} = mutation, shard_table) do
    [{mutation, ShardRouter.lookup_shard(shard_table, key)}]
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

    # Pass log_services from the plan
    opts_with_log_services = Keyword.put(opts, :log_services, plan.log_services)

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

  @spec resolve_log_descriptors(%{Log.id() => term()}, %{term() => ServiceDescriptor.t()}) :: %{
          Log.id() => ServiceDescriptor.t()
        }
  def resolve_log_descriptors(log_descriptors, services) do
    log_descriptors
    |> Map.keys()
    |> Enum.map(&{&1, Map.get(services, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Map.new()
  end

  @spec try_to_push_transaction_to_log(ServiceDescriptor.t(), binary(), Bedrock.version()) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(%{kind: :log, status: {:up, log_server}}, transaction, last_commit_version) do
    Log.push(log_server, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log(_, _, _), do: {:error, :unavailable}

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
       push within the timeout period or other errors occur.
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
        ) :: :ok | {:error, log_push_error()}
  def push_transaction_to_logs_direct(last_commit_version, transactions_by_log, _commit_version, opts) do
    log_services = Keyword.fetch!(opts, :log_services)
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    log_push_fn = Keyword.get(opts, :log_push_fn, &try_to_push_transaction_to_log_direct/3)
    timeout = Keyword.get(opts, :timeout, 5_000)

    required_acknowledgments = map_size(log_services)

    log_services
    |> async_stream_fn.(
      fn {log_id, service_ref} ->
        encoded_transaction = Map.get(transactions_by_log, log_id)
        result = log_push_fn.(service_ref, encoded_transaction, last_commit_version)
        {log_id, result}
      end,
      timeout: timeout
    )
    |> Enum.reduce_while({0, []}, fn
      {:ok, {log_id, {:error, reason}}}, {_count, errors} ->
        {:halt, {:error, [{log_id, reason} | errors]}}

      {:ok, {_log_id, :ok}}, {count, errors} ->
        count = 1 + count

        if count == required_acknowledgments do
          {:halt, {:ok, count}}
        else
          {:cont, {count, errors}}
        end

      {:exit, {log_id, reason}}, {_count, errors} ->
        {:halt, {:error, [{log_id, reason} | errors]}}
    end)
    |> case do
      {:ok, ^required_acknowledgments} ->
        :ok

      {:error, errors} ->
        {:error, {:log_failures, errors}}

      {count, errors} when count < required_acknowledgments ->
        {:error, {:insufficient_acknowledgments, count, required_acknowledgments, errors}}

      _other ->
        {:error, :log_push_failed}
    end
  end

  @spec try_to_push_transaction_to_log_direct(pid() | {atom(), node()}, binary(), Bedrock.version()) ::
          :ok | {:error, term()}
  def try_to_push_transaction_to_log_direct(service_ref, transaction, last_commit_version) when is_pid(service_ref) do
    Log.push(service_ref, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log_direct({name, node}, transaction, last_commit_version) do
    Log.push({name, node}, transaction, last_commit_version)
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
      |> Enum.map(fn {idx, {tx_idx, reply_fn, _binary, _task}} -> {reply_fn, tx_idx, idx} end)

    successful_indices = Enum.map(successful_entries, fn {_reply_fn, _tx_idx, idx} -> idx end)

    success_reply_fn.(successful_entries, plan.commit_version)

    %{plan | replied_indices: MapSet.union(plan.replied_indices, MapSet.new(successful_indices)), stage: :completed}
  end

  @spec send_reply_with_commit_version([Batch.reply_fn()], Bedrock.version()) :: :ok
  def send_reply_with_commit_version(oks, commit_version), do: Enum.each(oks, & &1.({:ok, commit_version}))

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

  @spec extract_result_or_handle_error(FinalizationPlan.t(), keyword()) ::
          {:ok, non_neg_integer(), non_neg_integer(), RoutingData.t()}
          | {:error, finalization_error()}
  def extract_result_or_handle_error(%FinalizationPlan{stage: :completed} = plan, _opts) do
    # Table is managed by commit proxy server - no cleanup needed here
    n_aborts = plan.aborted_count
    n_successes = plan.transaction_count - n_aborts

    # Build updated routing_data from plan fields
    updated_routing_data = %RoutingData{
      shard_table: plan.shard_table,
      log_map: plan.log_map,
      log_services: plan.log_services,
      replication_factor: plan.replication_factor
    }

    {:ok, n_aborts, n_successes, updated_routing_data}
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
