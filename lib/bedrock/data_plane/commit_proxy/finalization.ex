defmodule Bedrock.DataPlane.CommitProxy.Finalization do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Transaction
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.Service.Worker

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [transactions_in_order: 1]

  # Type declarations for function parameters
  @type resolver_fn() :: (resolvers :: [{start_key :: Bedrock.key(), Resolver.ref()}],
                          last_version :: Bedrock.version(),
                          commit_version :: Bedrock.version(),
                          transaction_summaries :: [Resolver.transaction()],
                          resolver_opts :: keyword() ->
                            {:ok, aborted :: [index :: integer()]}
                            | {:error, :timeout}
                            | {:error, :unavailable})

  @type log_push_batch_fn() :: (TransactionSystemLayout.t(),
                                last_commit_version :: Bedrock.version(),
                                transactions_by_tag :: %{Bedrock.range_tag() => Transaction.t()},
                                commit_version :: Bedrock.version(),
                                majority_reached :: (Bedrock.version() -> :ok),
                                opts :: keyword() ->
                                  :ok)

  @type log_push_single_fn() :: (ServiceDescriptor.t(),
                                 EncodedTransaction.t(),
                                 Bedrock.version() ->
                                   :ok | {:error, :unavailable})

  @type async_stream_fn() :: (enumerable :: Enumerable.t(),
                              fun :: (term() -> term()),
                              opts :: keyword() ->
                                Enumerable.t())

  @type abort_reply_fn() :: ([Batch.reply_fn()] -> :ok)

  @type success_reply_fn() :: ([Batch.reply_fn()], Bedrock.version() -> :ok)

  @type timeout_fn() :: (non_neg_integer() -> non_neg_integer())

  @type exit_fn() :: (term() -> no_return())

  @doc """
  Finalizes a batch of transactions by resolving conflicts, separating
  successful transactions from aborts, and pushing them to the log servers.

  This function processes a batch of transactions, first ensuring that any
  conflicts are resolved. After conflict resolution, it organizes the
  transactions into those that will be committed and those that will be aborted.

  Clients with aborted transactions are notified of the abort immediately.
  Successful transactions are pushed to the system's logs, and clients that
  submitted the transactions are notified when a majority of the log servers
  have acknowledged.

  ## Parameters

    - `batch`: A `Batch.t()` struct that contains the transactions to be finalized,
      along with the commit version details.
    - `transaction_system_layout`: Provides configuration and systemic details,
      including the available resolver and log servers.

  ## Returns
    - `:ok` when the batch has been processed, and all clients have been
      notified about the status of their transactions.
  """
  @spec finalize_batch(
          Batch.t(),
          TransactionSystemLayout.t(),
          opts :: [
            resolver_fn: resolver_fn(),
            batch_log_push_fn: log_push_batch_fn(),
            abort_reply_fn: abort_reply_fn(),
            success_reply_fn: success_reply_fn(),
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            timeout: non_neg_integer()
          ]
        ) ::
          {:ok, n_aborts :: non_neg_integer(), n_oks :: non_neg_integer()} | {:error, term()}
  def finalize_batch(batch, transaction_system_layout, opts \\ []) do
    resolver_fn = Keyword.get(opts, :resolver_fn, &resolve_transactions/5)

    batch_log_push_fn =
      Keyword.get(opts, :batch_log_push_fn, &push_transaction_to_logs_with_opts/6)

    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    success_reply_fn = Keyword.get(opts, :success_reply_fn, &send_reply_with_commit_version/2)

    transactions_in_order = transactions_in_order(batch)
    commit_version = batch.commit_version

    with {:ok, aborted_indices} <-
           resolver_fn.(
             transaction_system_layout.resolvers,
             batch.last_commit_version,
             commit_version,
             transform_transactions_for_resolution(transactions_in_order),
             timeout: 1_000
           ),
         # Immediately notify aborted transactions and extract successful ones
         {oks, n_aborts} <-
           notify_aborts_and_extract_oks(
             transactions_in_order,
             aborted_indices,
             abort_reply_fn
           ),
         # Prepare successful transactions for logging
         transactions_by_tag <-
           prepare_successful_transactions_for_log(
             oks,
             commit_version,
             transaction_system_layout.storage_teams
           ),
         # Extract reply functions for success notification
         ok_reply_fns <- Enum.map(oks, fn {reply_fn, _transaction} -> reply_fn end),
         :ok <-
           batch_log_push_fn.(
             transaction_system_layout,
             batch.last_commit_version,
             transactions_by_tag,
             commit_version,
             fn version -> success_reply_fn.(ok_reply_fns, version) end,
             opts
           ) do
      {:ok, n_aborts, length(oks)}
    else
      {:error, _reason} = error ->
        batch
        |> Batch.all_callers()
        |> abort_reply_fn.()

        error

      :error ->
        batch
        |> Batch.all_callers()
        |> abort_reply_fn.()

        {:error, :log_push_failed}
    end
  end

  @spec resolve_transactions(
          resolvers :: [{start_key :: Bedrock.key(), Resolver.ref()}],
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [Resolver.transaction()],
          opts :: [
            timeout: :infinity | non_neg_integer(),
            timeout_fn: timeout_fn(),
            exit_fn: exit_fn(),
            attempts_remaining: non_neg_integer()
          ]
        ) ::
          {:ok, aborted :: [index :: integer()]}
          | {:error, :timeout}
          | {:error, :unavailable}
  def resolve_transactions(
        resolvers,
        last_version,
        commit_version,
        transaction_summaries,
        opts
      ) do
    # Set defaults for all options in one place
    timeout_fn = Keyword.get(opts, :timeout_fn, &default_timeout_fn/1)
    exit_fn = Keyword.get(opts, :exit_fn, &default_exit_fn/1)
    attempts_remaining = Keyword.get(opts, :attempts_remaining, 2)
    # Calculate timeout for this attempt using injected function (pass attempts used)
    attempts_used = 2 - attempts_remaining
    timeout = Keyword.get(opts, :timeout, timeout_fn.(attempts_used))

    ranges =
      resolvers
      |> Enum.map(&elem(&1, 0))
      |> Enum.concat([:end])
      |> Enum.chunk_every(2, 1, :discard)

    transaction_summaries_by_start_key =
      ranges
      |> Enum.map(fn
        [start_key, end_key] ->
          filtered_summaries =
            filter_transaction_summaries(
              transaction_summaries,
              filter_fn(start_key, end_key)
            )

          {start_key, filtered_summaries}
      end)
      |> Enum.into(%{})

    result =
      resolvers
      |> Enum.map(fn {start_key, ref} ->
        Resolver.resolve_transactions(
          ref,
          last_version,
          commit_version,
          Map.get(transaction_summaries_by_start_key, start_key, []),
          timeout: timeout
        )
      end)
      |> Enum.reduce({:ok, []}, fn
        {:ok, aborted}, {:ok, acc} ->
          {:ok, Enum.uniq(acc ++ aborted)}

        {:error, reason}, _ ->
          {:error, reason}
      end)

    case result do
      {:ok, _} = success ->
        success

      {:error, reason} when attempts_remaining > 0 ->
        # Emit telemetry for retry attempt (after this failure, before next retry)
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :retry],
          %{attempts_remaining: attempts_remaining - 1, attempts_used: attempts_used + 1},
          %{reason: reason}
        )

        # Retry with decremented attempts
        updated_opts = Keyword.put(opts, :attempts_remaining, attempts_remaining - 1)

        resolve_transactions(
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          updated_opts
        )

      {:error, reason} ->
        # Emit telemetry for final failure
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
          %{total_attempts: attempts_used + 1},
          %{reason: reason}
        )

        # Max retries exceeded, exit commit proxy to trigger recovery
        exit_fn.(reason)
    end
  end

  # Default timeout function: 500ms * 2^attempts_used (exponential backoff)
  @spec default_timeout_fn(non_neg_integer()) :: non_neg_integer()
  def default_timeout_fn(attempts_used), do: (500 * :math.pow(2, attempts_used)) |> round()

  # Default exit function: exit with resolver unavailable
  @spec default_exit_fn(term()) :: no_return()
  defp default_exit_fn(reason), do: exit({:resolver_unavailable, reason})

  defp filter_fn(start_key, :end), do: &(&1 >= start_key)
  defp filter_fn(start_key, end_key), do: &(&1 >= start_key and &1 < end_key)

  defp filter_transaction_summaries(transaction_summaries, filter_fn),
    do: Enum.map(transaction_summaries, &filter_transaction_summary(&1, filter_fn))

  defp filter_transaction_summary({nil, writes}, filter_fn),
    do: {nil, Enum.filter(writes, filter_fn)}

  defp filter_transaction_summary({{read_version, reads}, writes}, filter_fn),
    do: {{read_version, Enum.filter(reads, filter_fn)}, Enum.filter(writes, filter_fn)}

  @doc """
  Maps a key to its corresponding storage team tag.

  Searches through storage teams to find which team's key range contains
  the given key. Uses lexicographic ordering where a key belongs to a team
  if it falls within [start_key, end_key) or [start_key, :end).

  ## Parameters
    - `key`: The key to map to a storage team
    - `storage_teams`: List of storage team descriptors

  ## Returns
    - `{:ok, tag}` if a matching storage team is found
    - `{:error, :no_matching_team}` if no team covers the key
  """
  @spec key_to_tag(Bedrock.key(), [StorageTeamDescriptor.t()]) ::
          {:ok, Bedrock.range_tag()} | {:error, :no_matching_team}
  def key_to_tag(key, storage_teams) do
    Enum.find_value(storage_teams, {:error, :no_matching_team}, fn
      %{tag: tag, key_range: {start_key, end_key}} ->
        if key_in_range?(key, start_key, end_key) do
          {:ok, tag}
        else
          nil
        end
    end)
  end

  @spec key_in_range?(Bedrock.key(), Bedrock.key(), Bedrock.key() | :end) :: boolean()
  defp key_in_range?(key, start_key, :end), do: key >= start_key
  defp key_in_range?(key, start_key, end_key), do: key >= start_key and key < end_key

  @doc """
  Groups a map of writes by their target storage team tags.

  For each key-value pair in the writes map, determines which storage team
  tag the key belongs to and groups the writes accordingly.

  ## Parameters
    - `writes`: Map of key -> value pairs to be written
    - `storage_teams`: List of storage team descriptors for tag mapping

  ## Returns
    - Map of tag -> %{key => value} for writes belonging to each tag

  ## Failure Behavior
    - Exits with `{:storage_team_coverage_error, key}` if any key doesn't
      match any storage team. This indicates a critical configuration error
      where storage teams don't cover the full keyspace, triggering recovery.
  """
  @spec group_writes_by_tag(%{Bedrock.key() => term()}, [StorageTeamDescriptor.t()]) ::
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}} | no_return()
  def group_writes_by_tag(writes, storage_teams) do
    writes
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      case key_to_tag(key, storage_teams) do
        {:ok, tag} ->
          Map.update(acc, tag, %{key => value}, &Map.put(&1, key, value))

        {:error, :no_matching_team} ->
          # This indicates a critical configuration error - storage teams don't cover full keyspace
          # Exit to trigger recovery which will rebuild the storage team layout
          exit({:storage_team_coverage_error, key})
      end
    end)
  end

  @doc """
  Merges two maps of writes grouped by tag.

  Takes two maps where keys are tags and values are write maps,
  and merges the write maps for each tag.

  ## Parameters
    - `acc`: Accumulator map of tag -> writes
    - `new_writes`: New writes map to merge

  ## Returns
    - Merged map of tag -> combined writes
  """
  @spec merge_writes_by_tag(
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}},
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}}
        ) :: %{Bedrock.range_tag() => %{Bedrock.key() => term()}}
  def merge_writes_by_tag(acc, new_writes) do
    Map.merge(acc, new_writes, fn _tag, existing_writes, new_writes ->
      Map.merge(existing_writes, new_writes)
    end)
  end

  @doc """
  Builds the transaction that each log should receive based on tag coverage.

  Each log receives a transaction containing writes for all tags it covers.
  Logs that don't cover any tags in the transaction get an empty transaction
  to maintain version consistency.

  ## Parameters
    - `logs_by_id`: Map of log_id -> list of tags covered by that log
    - `transactions_by_tag`: Map of tag -> transaction shard for that tag
    - `commit_version`: The commit version for empty transactions

  ## Returns
    - Map of log_id -> transaction that log should receive
  """
  @spec build_log_transactions(
          %{Log.id() => [Bedrock.range_tag()]},
          %{Bedrock.range_tag() => Transaction.t()},
          Bedrock.version()
        ) :: %{Log.id() => Transaction.t()}
  def build_log_transactions(logs_by_id, transactions_by_tag, commit_version) do
    logs_by_id
    |> Enum.map(fn {log_id, tags_covered} ->
      # Collect writes for all tags this log covers
      combined_writes =
        tags_covered
        |> Enum.filter(&Map.has_key?(transactions_by_tag, &1))
        |> Enum.reduce(%{}, fn tag, acc ->
          transaction = Map.get(transactions_by_tag, tag)
          writes = Transaction.key_values(transaction)
          Map.merge(acc, writes)
        end)

      transaction = Transaction.new(commit_version, combined_writes)
      {log_id, transaction}
    end)
    |> Map.new()
  end

  @spec determine_majority(n :: non_neg_integer()) :: pos_integer()
  defp determine_majority(n), do: 1 + div(n, 2)

  @doc """
  Pushes transaction shards to logs based on tag coverage and waits for
  acknowledgement from a majority of log servers.

  This function takes transaction shards grouped by storage team tags and
  routes them efficiently to logs. Each log receives only the transaction
  shards for tags it covers, plus empty transactions for version consistency.
  All logs must acknowledge to maintain durability guarantees.

  ## Parameters

    - `transaction_system_layout`: Contains configuration information about the
      transaction system, including available log servers and their tag coverage.
    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transactions_by_tag`: Map of storage team tag to transaction shard.
      May be empty if all transactions were aborted.
    - `commit_version`: The version assigned by the sequencer for this batch.
    - `majority_reached`: Callback function to notify when majority is reached.

  ## Returns
    - `:ok` if enough acknowledgements have been received from the log servers.
    - `:error` if a majority of logs have not successfully acknowledged the
       push within the timeout period.
  """
  @spec push_transaction_to_logs(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          %{Bedrock.range_tag() => Transaction.t()},
          commit_version :: Bedrock.version(),
          majority_reached :: (Bedrock.version() -> :ok)
        ) :: :ok
  def push_transaction_to_logs(
        transaction_system_layout,
        last_commit_version,
        transactions_by_tag,
        commit_version,
        majority_reached
      ) do
    push_transaction_to_logs_with_opts(
      transaction_system_layout,
      last_commit_version,
      transactions_by_tag,
      commit_version,
      majority_reached,
      []
    )
  end

  @doc """
  Testable version of push_transaction_to_logs with configurable options.

  This version accepts an opts parameter that allows injecting custom behavior
  for testing scenarios, including custom async stream implementations and
  log push functions.
  """
  @spec push_transaction_to_logs_with_opts(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          %{Bedrock.range_tag() => Transaction.t()},
          commit_version :: Bedrock.version(),
          majority_reached :: (Bedrock.version() -> :ok),
          opts :: [
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            timeout: non_neg_integer()
          ]
        ) :: :ok
  def push_transaction_to_logs_with_opts(
        transaction_system_layout,
        last_commit_version,
        transactions_by_tag,
        commit_version,
        majority_reached,
        opts \\ []
      ) do
    # Extract configurable functions for testability
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    log_push_fn = Keyword.get(opts, :log_push_fn, &try_to_push_transaction_to_log/3)
    timeout = Keyword.get(opts, :timeout, 5_000)

    # When transactions_by_tag is empty (all transactions aborted),
    # we still need to push empty transactions to all logs for version consistency
    # Use the provided commit_version in this case

    logs_by_id = transaction_system_layout.logs
    n = map_size(logs_by_id)
    m = determine_majority(n)

    # Build the transaction each log should receive
    log_transactions =
      build_log_transactions(logs_by_id, transactions_by_tag, commit_version)

    resolved_logs = resolve_log_descriptors(logs_by_id, transaction_system_layout.services)

    # Use configurable async stream function
    stream_result =
      async_stream_fn.(
        resolved_logs,
        fn {log_id, service_descriptor} ->
          transaction_for_log = Map.get(log_transactions, log_id)
          encoded_transaction = EncodedTransaction.encode(transaction_for_log)

          result = log_push_fn.(service_descriptor, encoded_transaction, last_commit_version)
          {log_id, result}
        end,
        timeout: timeout
      )

    stream_result
    |> Enum.reduce_while(0, fn
      {:ok, {_log_id, {:error, _reason}}}, count ->
        {:cont, count}

      {:ok, {_log_id, :ok}}, count ->
        count = 1 + count

        if count == m do
          :ok = majority_reached.(commit_version)
        end

        {:cont, count}
    end)
    |> case do
      ^n -> :ok
      # If we haven't received enough responses, we need to abort
      _ -> :error
    end
  end

  @spec resolve_log_descriptors(
          %{Log.id() => LogDescriptor.t()},
          %{Worker.id() => ServiceDescriptor.t()}
        ) ::
          %{Worker.id() => ServiceDescriptor.t()}
  def resolve_log_descriptors(log_descriptors, services) do
    log_descriptors
    |> Map.keys()
    |> Enum.map(&{&1, Map.get(services, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Map.new()
  end

  @spec try_to_push_transaction_to_log(
          ServiceDescriptor.t(),
          EncodedTransaction.t(),
          Bedrock.version()
        ) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(
        %{kind: :log, status: {:up, log_server}},
        transaction,
        last_commit_version
      ) do
    Log.push(log_server, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log(_, _, _), do: {:error, :unavailable}

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok

  def reply_to_all_clients_with_aborted_transactions(aborts),
    do: Enum.each(aborts, & &1.({:error, :aborted}))

  @spec send_reply_with_commit_version([Batch.reply_fn()], Bedrock.version()) ::
          :ok
  def send_reply_with_commit_version(oks, commit_version),
    do: Enum.each(oks, & &1.({:ok, commit_version}))

  @doc """
  Immediately notify aborted transactions and extract successful ones.

  This function takes the list of transactions and aborted indices,
  immediately sends abort notifications to the aborted transactions,
  and returns only the successful transactions for further processing.

  Returns a tuple of {successful_transactions, number_of_aborts}.
  """
  @spec notify_aborts_and_extract_oks(
          transactions :: [{Batch.reply_fn(), Bedrock.transaction()}],
          aborted_indices :: [integer()],
          abort_reply_fn :: ([Batch.reply_fn()] -> :ok)
        ) ::
          {successful_transactions :: [{Batch.reply_fn(), Bedrock.transaction()}],
           n_aborts :: non_neg_integer()}
  def notify_aborts_and_extract_oks(transactions, aborted_indices, abort_reply_fn) do
    aborted_set = MapSet.new(aborted_indices)

    {oks, aborts} =
      transactions
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {{reply_fn, transaction}, idx}, {oks_acc, aborts_acc} ->
        if MapSet.member?(aborted_set, idx) do
          {oks_acc, [reply_fn | aborts_acc]}
        else
          {[{reply_fn, transaction} | oks_acc], aborts_acc}
        end
      end)

    # Immediately notify aborted clients
    abort_reply_fn.(aborts)

    # Return successful transactions (reversed to maintain order) and abort count
    {Enum.reverse(oks), length(aborts)}
  end

  @doc """
  Prepare successful transactions for logging by grouping writes by storage team tags.

  This function takes only successful transactions (aborts have already been handled)
  and groups their writes by storage team tags for efficient log routing.

  Returns a map of tag -> Transaction.t() containing writes grouped by storage team.
  """
  @spec prepare_successful_transactions_for_log(
          successful_transactions :: [{Batch.reply_fn(), Bedrock.transaction()}],
          commit_version :: Bedrock.version(),
          storage_teams :: [StorageTeamDescriptor.t()]
        ) :: %{Bedrock.range_tag() => Transaction.t()} | no_return()
  def prepare_successful_transactions_for_log(
        successful_transactions,
        commit_version,
        storage_teams
      ) do
    # Process only successful transactions
    combined_writes_by_tag =
      successful_transactions
      |> Enum.reduce(%{}, fn {_reply_fn, {_reads, writes}}, acc ->
        tag_grouped_writes = group_writes_by_tag(writes, storage_teams)
        merge_writes_by_tag(acc, tag_grouped_writes)
      end)

    # Convert writes to transactions by tag
    combined_writes_by_tag
    |> Enum.map(fn {tag, writes} -> {tag, Transaction.new(commit_version, writes)} end)
    |> Map.new()
  end

  @doc """
  Prepare transactions for logging by separating successful transactions
  from aborted ones and grouping writes by storage team tags.

  This function groups writes by their target storage team tags, enabling
  efficient routing to logs that cover specific tags. Each storage team
  gets its own transaction shard containing only the writes for keys
  in that team's range.

  Returns a tuple with:
    - A list of reply functions for successful transactions.
    - A list of reply functions for aborted transactions.
    - A map of tag -> Transaction.t() containing writes grouped by storage team.

  ## Parameters

    - `transactions`: A list of transactions, each containing the reply
      function, read/write data, and other necessary details.
    - `aborts`: A list of integer indices indicating which transactions were
      aborted.
    - `commit_version`: The current commit version.
    - `storage_teams`: List of storage team descriptors for key-to-tag mapping.

  ## Returns
    - A tuple: `{oks, aborts, transactions_by_tag}`
  """
  @spec prepare_transaction_to_log(
          transactions :: [{Batch.reply_fn(), Bedrock.transaction()}],
          aborts :: [integer()],
          commit_version :: Bedrock.version(),
          storage_teams :: [StorageTeamDescriptor.t()]
        ) ::
          {oks :: [Batch.reply_fn()], aborts :: [Batch.reply_fn()],
           %{Bedrock.range_tag() => Transaction.t()}}
          | no_return()
  def prepare_transaction_to_log(transactions, aborts, commit_version, storage_teams) do
    {oks, aborted_fns, combined_writes_by_tag} =
      separate_transactions_and_group_writes(transactions, aborts, storage_teams)

    transactions_by_tag =
      convert_writes_to_transactions_by_tag(combined_writes_by_tag, commit_version)

    {oks, aborted_fns, transactions_by_tag}
  end

  # Extract common logic for separating transactions and grouping writes
  @spec separate_transactions_and_group_writes(
          [{Batch.reply_fn(), Bedrock.transaction()}],
          [integer()],
          [StorageTeamDescriptor.t()]
        ) ::
          {[Batch.reply_fn()], [Batch.reply_fn()],
           %{Bedrock.range_tag() => %{Bedrock.key() => term()}}}
  defp separate_transactions_and_group_writes(transactions, [], storage_teams) do
    # Fast path: no aborted transactions
    {oks, writes_by_tag} =
      transactions
      |> Enum.reduce({[], %{}}, fn {from, {_reads, writes}}, {oks, writes_by_tag} ->
        tag_grouped_writes = group_writes_by_tag(writes, storage_teams)
        {[from | oks], merge_writes_by_tag(writes_by_tag, tag_grouped_writes)}
      end)

    {Enum.reverse(oks), [], writes_by_tag}
  end

  defp separate_transactions_and_group_writes(transactions, aborts, storage_teams) do
    # Slow path: need to filter out aborted transactions
    aborted_set = MapSet.new(aborts)

    {oks, aborted_fns, writes_by_tag} =
      transactions
      |> Enum.with_index()
      |> Enum.reduce({[], [], %{}}, fn
        {{from, {_reads, writes}}, idx}, {oks, aborts_acc, writes_by_tag} ->
          if MapSet.member?(aborted_set, idx) do
            {oks, [from | aborts_acc], writes_by_tag}
          else
            tag_grouped_writes = group_writes_by_tag(writes, storage_teams)
            {[from | oks], aborts_acc, merge_writes_by_tag(writes_by_tag, tag_grouped_writes)}
          end
      end)

    {Enum.reverse(oks), Enum.reverse(aborted_fns), writes_by_tag}
  end

  # Extract common logic for converting writes to transactions by tag
  @spec convert_writes_to_transactions_by_tag(
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}},
          Bedrock.version()
        ) :: %{Bedrock.range_tag() => Transaction.t()}
  defp convert_writes_to_transactions_by_tag(writes_by_tag, commit_version) do
    writes_by_tag
    |> Enum.map(fn {tag, writes} -> {tag, Transaction.new(commit_version, writes)} end)
    |> Map.new()
  end

  @doc """
  Transforms the list of transactions for resolution.

  Converts the transaction data to the format expected by the conflict
  resolution logic. For each transaction, it extracts the read version,
  the reads, and the keys of the writes, discarding the values of the writes
  as they are not needed for resolution.
  """
  @spec transform_transactions_for_resolution([{Batch.reply_fn(), Bedrock.transaction()}]) :: [
          Resolver.transaction()
        ]
  def transform_transactions_for_resolution(transactions) do
    transactions
    |> Enum.map(fn
      {_from, {nil, writes}} ->
        {nil, writes |> Map.keys()}

      {_from, {{read_version, reads}, writes}} ->
        {{read_version, reads |> Enum.uniq()}, writes |> Map.keys()}
    end)
  end
end
