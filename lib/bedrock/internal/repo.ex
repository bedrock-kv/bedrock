defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls, only: [cast: 2]
  import Bitwise

  alias Bedrock.Cluster.Link
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.Internal.Repo.TransactionContext
  alias Bedrock.Internal.TransactionBuilder
  alias Bedrock.KeySelector

  @default_timeout_in_ms 5_000

  # Read failures that never resolve without new information, only with a
  # retry: version window misses resolve with a fresh read version, and the
  # rest are routing- or liveness-shaped (FDB's wrong_shard_server model:
  # they cost the caller a retry, never surface to the user).
  @retryable_read_failures [
    :timeout,
    :unavailable,
    :version_too_new,
    :version_too_old,
    :layout_lookup_failed,
    :no_servers_to_race,
    :locked
  ]

  # Of those, the ones that look like stale routing: an unroutable key, no
  # servers to race, or a server that stopped answering. A dead pid is
  # exactly what a stale snapshot looks like, so the retry drops the node's
  # routing cache and refetches from a proxy. Version window misses are not
  # routing's fault and keep the cache.
  @routing_invalidating_failures [:layout_lookup_failed, :no_servers_to_race, :unavailable, :timeout, :locked]

  @type transaction :: pid()
  @type key :: term()
  @type value :: term()

  defp txn(repo_module), do: TransactionContext.builder(repo_module)
  defp txn!(repo_module), do: txn(repo_module) || raise("No active transaction")

  @spec add_read_conflict_key(module(), key()) :: :ok
  def add_read_conflict_key(repo_module, key), do: cast(txn!(repo_module), {:add_read_conflict_key, key})

  @spec add_write_conflict_range(module(), key(), key()) :: :ok
  def add_write_conflict_range(repo_module, start_key, end_key),
    do: cast(txn!(repo_module), {:add_write_conflict_range, start_key, end_key})

  @spec get(module(), key()) :: nil | value()
  def get(repo_module, key), do: get(repo_module, key, [])

  @spec get(module(), key(), opts :: keyword()) :: nil | value()
  def get(repo_module, key, opts) do
    t = txn!(repo_module)

    case call_transaction(repo_module, t, {:get, key, opts}) do
      {:ok, value} ->
        value

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in @retryable_read_failures ->
        throw({__MODULE__, t, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, t, :transaction_error, reason, :get, key})
    end
  end

  @spec select(module(), KeySelector.t()) :: nil | {resolved_key :: key(), value()}
  def select(repo_module, %KeySelector{} = key_selector), do: select(repo_module, key_selector, [])

  @spec select(module(), KeySelector.t(), opts :: keyword()) :: nil | {resolved_key :: key(), value()}
  def select(repo_module, %KeySelector{} = key_selector, opts) do
    t = txn!(repo_module)

    case call_transaction(repo_module, t, {:get_key_selector, key_selector, opts}) do
      {:ok, {_key, _value} = result} ->
        result

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in @retryable_read_failures ->
        throw({__MODULE__, t, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, t, :transaction_error, reason, :select, key_selector})
    end
  end

  # Streaming

  @doc """
  Create a lazy stream for a range query.

  ## Options

  - `:batch_size` - Number of items to fetch per batch (default: 100)
  - `:timeout` - Timeout per batch request (default: 5000)
  - `:limit` - Maximum total items to return
  """
  @spec get_range(
          module(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            batch_size: pos_integer(),
            timeout: pos_integer(),
            limit: pos_integer(),
            mode: :individual | :batch,
            snapshot: boolean()
          ]
        ) :: Enumerable.t({any(), any()})
  @spec get_range(module(), start_key :: key(), end_key :: key()) :: Enumerable.t({any(), any()})
  def get_range(repo_module, start_key, end_key), do: get_range(repo_module, start_key, end_key, [])

  def get_range(repo_module, start_key, end_key, opts) do
    txn = txn!(repo_module)
    batch_size = Keyword.get(opts, :batch_size, 100)
    timeout = Keyword.get(opts, :timeout, 5_000)

    # Filter out stream-specific options, keep only TransactionBuilder options
    txn_opts = Keyword.drop(opts, [:batch_size, :timeout])

    # Initial state tracks the current position in the range
    initial_state = %{
      repo: repo_module,
      txn: txn,
      current_key: start_key,
      end_key: end_key,
      txn_opts: txn_opts,
      finished: false,
      items_returned: 0,
      limit: opts[:limit],
      current_batch: [],
      has_more: true
    }

    Stream.resource(
      fn -> initial_state end,
      fn state ->
        if state.finished or limit_reached?(state) do
          {:halt, state}
        else
          emit_next_row(state, batch_size, timeout)
        end
      end,
      fn _state -> :ok end
    )
  end

  defp limit_reached?(%{limit: nil}), do: false
  defp limit_reached?(%{limit: limit, items_returned: returned}), do: returned >= limit

  defp emit_next_row(state, batch_size, timeout) do
    case state.current_batch do
      [] -> fetch_and_emit_first_row(state, batch_size, timeout)
      [row | remaining_rows] -> emit_row_from_buffer(state, row, remaining_rows)
    end
  end

  defp emit_row_from_buffer(state, {key, _} = row, remaining_rows) do
    new_state = %{
      state
      | current_batch: remaining_rows,
        current_key: Bedrock.Key.key_after(key),
        items_returned: state.items_returned + 1,
        finished: remaining_rows == [] and not state.has_more
    }

    {[row], new_state}
  end

  defp fetch_and_emit_first_row(state, batch_size, timeout) do
    effective_batch_size =
      case state.limit do
        nil -> batch_size
        limit -> min(batch_size, limit - state.items_returned)
      end

    case call_transaction(
           state.repo,
           state.txn,
           {:get_range, state.current_key, state.end_key, effective_batch_size, state.txn_opts},
           timeout
         ) do
      {:ok, {[], _}} ->
        {:halt, %{state | finished: true}}

      {:ok, {[first_row | rest], has_more}} ->
        emit_row_from_buffer(%{state | current_batch: rest, has_more: has_more}, first_row, rest)

      {:failure, reason} when reason in @retryable_read_failures ->
        throw({__MODULE__, state.txn, :retryable_failure, reason})

      {:error, reason} ->
        raise "Range query failed: #{inspect(reason)}"
    end
  end

  # Clearing

  @spec clear_range(
          module(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            no_write_conflict: boolean()
          ]
        ) :: :ok
  def clear_range(repo_module, start_key, end_key, opts \\ []),
    do: cast(txn!(repo_module), {:clear_range, start_key, end_key, opts})

  @spec clear(module(), key()) :: :ok
  @spec clear(module(), key(), opts :: [no_write_conflict: boolean()]) :: :ok
  def clear(repo_module, key, opts \\ []), do: cast(txn!(repo_module), {:clear, key, opts})

  # Mutation

  @spec put(module(), key(), value(), opts :: [no_write_conflict: boolean()]) :: :ok
  def put(repo_module, key, value, opts \\ []) when is_binary(key) and is_binary(value),
    do: cast(txn!(repo_module), {:set_key, key, value, opts})

  @spec atomic(module(), atom(), key(), binary()) :: :ok
  def atomic(repo_module, op, key, value) when is_atom(op) and is_binary(key) and is_binary(value),
    do: cast(txn!(repo_module), {:atomic, op, key, value})

  # Transaction Control

  @spec rollback(reason :: term()) :: no_return()
  def rollback(reason), do: throw({__MODULE__, :rollback, reason})

  @doc """
  Executes a function within a transaction context with automatic retry logic.

  ## Options

  - `:retry_limit` - Maximum number of retry attempts (default: unlimited)
  - `:timeout_in_ms` - End-to-end transaction timeout, including retries
    (default: 5000; use `:infinity` to disable)
  - `:transaction_system_layout` - Use a specific TSL instead of fetching from coordinator
  """
  @spec transact(cluster :: module(), repo :: module(), (-> result) | (module() -> result), opts :: keyword()) :: result
        when result: any()
  def transact(cluster, repo, fun, opts \\ []) do
    case txn(repo) do
      nil ->
        run_new_transaction(cluster, repo, fun, opts)

      existing_txn ->
        run_nested_transaction(repo, existing_txn, fun, opts)
    end
  end

  defp run_new_transaction(cluster, repo, fun, opts) do
    retry_limit = Keyword.get(opts, :retry_limit)
    provided_tsl = Keyword.get(opts, :transaction_system_layout)
    link = if provided_tsl, do: nil, else: cluster.link!()
    timeout_in_ms = Keyword.get(opts, :timeout_in_ms, @default_timeout_in_ms)

    TransactionContext.with_deadline(repo, timeout_in_ms, fn ->
      run_retryable_transaction(repo, fun, 0, retry_limit, fn last_reason ->
        maybe_invalidate_routing(link, last_reason)
        tsl = fetch_tsl_for_transaction(repo, provided_tsl, link)
        start_transaction_builder(tsl, routing_fn_for_transaction(cluster, provided_tsl, link), repo)
      end)
    end)
  after
    TransactionContext.clear_builder(repo)
  end

  defp run_nested_transaction(repo, txn, fun, opts) do
    timeout_in_ms = Keyword.get(opts, :timeout_in_ms, @default_timeout_in_ms)

    TransactionContext.with_deadline(repo, timeout_in_ms, fn ->
      run_retryable_transaction(repo, fun, 0, nil, fn _last_reason ->
        call_transaction(repo, txn, :nested_transaction)
        txn
      end)
    end)
  end

  defp maybe_invalidate_routing(nil, _last_reason), do: :ok

  defp maybe_invalidate_routing(link, last_reason) when last_reason in @routing_invalidating_failures,
    do: Link.invalidate_routing(link)

  defp maybe_invalidate_routing(_link, _last_reason), do: :ok

  defp fetch_tsl_for_transaction(repo, nil, link) do
    case Link.fetch_transaction_system_layout(link,
           timeout_in_ms: TransactionContext.remaining_timeout!(repo, :timeout)
         ) do
      {:ok, tsl} -> tsl
      {:error, reason} -> throw({__MODULE__, nil, :retryable_failure, reason})
    end
  end

  defp fetch_tsl_for_transaction(_repo, tsl, _link), do: tsl

  # A caller-provided TSL is wiring only and gets no routing_fn: the
  # builder's index stays empty and reads fail as layout_lookup_failed
  # (the escape hatch serves commit-only flows and tests). Otherwise
  # routing is proxy-served and lazy: the builder calls this on its first
  # read - Link cache first (the node's locationCache), fetch-through to a
  # random commit proxy on a miss, caching the raw projection for the next
  # transaction on this node. Runs in the builder process, so failures are
  # returned (they surface as read failures), never thrown.
  defp routing_fn_for_transaction(_cluster, provided_tsl, _link) when not is_nil(provided_tsl), do: nil

  defp routing_fn_for_transaction(cluster, nil, link) do
    fn ->
      case Link.fetch_cached_routing(link) do
        {:ok, routing} -> {:ok, callable_routing(cluster, routing)}
        {:error, _not_cached} -> fetch_and_cache_routing(cluster, link)
      end
    end
  end

  defp fetch_and_cache_routing(cluster, link) do
    with {:ok, tsl} <- Link.fetch_transaction_system_layout(link),
         [_ | _] = proxies <- Map.get(tsl, :proxies, []),
         {:ok, routing} <- proxies |> Enum.random() |> CommitProxy.fetch_routing() do
      Link.cache_routing(link, routing)
      {:ok, callable_routing(cluster, routing)}
    else
      [] -> {:error, :unavailable}
      {:error, reason} -> {:error, reason}
    end
  end

  # Turn the proxy's string-encoded materializer refs into callable
  # `{otp_name, node}` refs. Worker OTP names are deterministic in the
  # worker id; the node atom conversion is the documented exception to the
  # no-atoms-on-decode rule - the values come from system-mode-gated
  # writers and the atom count is bounded by cluster membership.
  defp callable_routing(cluster, %{shard_layout: shard_layout, materializers: materializers}) do
    callable =
      Map.new(materializers, fn {tag, {worker_id, node}} ->
        # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
        {tag, {cluster.otp_name_for_worker(worker_id), String.to_atom(node)}}
      end)

    %{shard_layout: shard_layout, materializers: callable}
  end

  defp start_transaction_builder(tsl, routing_fn, repo) do
    case TransactionBuilder.start_link(transaction_system_layout: tsl, routing_fn: routing_fn) do
      {:ok, txn} ->
        TransactionContext.put_builder(repo, txn)
        txn

      {:error, reason} ->
        throw({__MODULE__, nil, :retryable_failure, reason})
    end
  end

  defp run_retryable_transaction(repo, fun, retry_count, retry_limit, restart_fn, last_reason \\ nil) do
    TransactionContext.remaining_timeout!(repo, :timeout)
    run_transaction(repo, restart_fn.(last_reason), fun)
  catch
    {__MODULE__, failed_txn, :retryable_failure, reason} ->
      try_to_rollback(failed_txn)
      enforce_retry_limit(retry_count, retry_limit, reason)
      wait_before_retry(repo, retry_count, reason)
      run_retryable_transaction(repo, fun, retry_count + 1, retry_limit, restart_fn, reason)

    {__MODULE__, :rollback, reason} ->
      try_to_rollback(txn(repo))
      {:error, reason}

    {__MODULE__, failed_txn, :transaction_error, reason, operation, key} ->
      try_to_rollback(failed_txn)
      raise RuntimeError, "Transaction operation #{operation} failed for key #{inspect(key)}: #{inspect(reason)}"
  end

  defp run_transaction(repo, txn, fun) do
    fun
    |> Function.info(:arity)
    |> case do
      {:arity, 1} -> fun.(repo)
      {:arity, 0} -> fun.()
    end
    |> then(&try_to_commit(repo, txn, &1))
  rescue
    exception ->
      try_to_rollback(txn)
      reraise exception, __STACKTRACE__
  end

  defp wait_before_retry(repo, retry_count, last_reason) do
    base_delay = 1 <<< retry_count
    jitter = :rand.uniform(3)
    wait_time_in_ms = min(base_delay + jitter, 1000)
    remaining = TransactionContext.remaining_timeout!(repo, last_reason)

    Process.sleep(min_timeout(wait_time_in_ms, remaining))
    TransactionContext.remaining_timeout!(repo, last_reason)
  end

  defp enforce_retry_limit(_retry_count, nil, _reason), do: :ok
  defp enforce_retry_limit(retry_count, retry_limit, _reason) when retry_count < retry_limit, do: :ok

  defp enforce_retry_limit(_retry_count, retry_limit, reason) do
    raise RuntimeError,
          "Transaction retry limit exceeded after #{retry_limit} attempts. Last error: #{inspect(reason)}"
  end

  defp try_to_commit(repo, txn, result) do
    case call_transaction(repo, txn, :commit) do
      :ok ->
        result

      {:ok, _commit_version} ->
        result

      # A key outside the caller's legal range or an undecodable transaction
      # is a permanent client error - retrying cannot change the outcome, so
      # surface it instead of burning the transaction deadline.
      {:error, {:key_out_of_range, _key} = reason} ->
        throw({__MODULE__, :rollback, reason})

      {:error, :invalid_transaction = reason} ->
        throw({__MODULE__, :rollback, reason})

      {:error, reason} ->
        throw({__MODULE__, txn, :retryable_failure, reason})
    end
  end

  defp try_to_rollback(nil), do: :ok
  defp try_to_rollback(txn), do: GenServer.cast(txn, :rollback)

  defp call_transaction(repo, txn, message, timeout \\ :transaction_deadline) do
    timeout = bounded_timeout(repo, timeout)
    GenServer.call(txn, message, timeout)
  catch
    :exit, {:timeout, _} -> throw({__MODULE__, txn, :retryable_failure, :timeout})
  end

  defp bounded_timeout(repo, :transaction_deadline), do: TransactionContext.remaining_timeout!(repo, :timeout)

  defp bounded_timeout(repo, timeout) do
    min_timeout(timeout, TransactionContext.remaining_timeout!(repo, :timeout))
  end

  defp min_timeout(:infinity, timeout), do: timeout
  defp min_timeout(timeout, :infinity), do: timeout
  defp min_timeout(a, b), do: min(a, b)
end
