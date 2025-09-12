defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls, only: [cast: 2]

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Internal.RangeQuery
  alias Bedrock.KeySelector

  @type transaction :: pid()
  @type key :: term()
  @type value :: term()

  @spec add_read_conflict_key(transaction(), key()) :: transaction()
  def add_read_conflict_key(t, key) do
    cast(t, {:add_read_conflict_key, key})
    t
  end

  @spec add_write_conflict_range(transaction(), key(), key()) :: transaction()
  def add_write_conflict_range(t, start_key, end_key) do
    cast(t, {:add_write_conflict_range, start_key, end_key})
    t
  end

  @spec get(transaction(), key(), opts :: keyword()) :: nil | value()
  def get(t, key, opts \\ []) do
    case GenServer.call(t, {:get, key, opts}, :infinity) do
      {:ok, value} ->
        value

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in [:timeout, :unavailable, :version_too_new] ->
        throw({__MODULE__, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, :transaction_error, reason, :get, key})
    end
  end

  @spec select(transaction(), KeySelector.t()) :: nil | {resolved_key :: key(), value()}
  @spec select(transaction(), KeySelector.t(), opts :: keyword()) :: nil | {resolved_key :: key(), value()}
  def select(t, %KeySelector{} = key_selector, opts \\ []) do
    case GenServer.call(t, {:get_key_selector, key_selector, opts}, :infinity) do
      {:ok, {_key, _value} = result} ->
        result

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in [:timeout, :unavailable, :version_too_new] ->
        throw({__MODULE__, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, :transaction_error, reason, :select, key_selector})
    end
  end

  @spec range(
          transaction(),
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
  def range(t, start_key, end_key, opts \\ []), do: RangeQuery.stream(t, start_key, end_key, opts)

  @spec clear_range(
          transaction(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            no_write_conflict: boolean()
          ]
        ) :: transaction()
  def clear_range(t, start_key, end_key, opts \\ []) do
    cast(t, {:clear_range, start_key, end_key, opts})
    t
  end

  @spec clear(transaction(), key()) :: transaction()
  @spec clear(transaction(), key(), opts :: [no_write_conflict: boolean()]) :: transaction()
  def clear(t, key, opts \\ []) do
    cast(t, {:clear, key, opts})
    t
  end

  @spec put(transaction(), key(), value(), opts :: [no_write_conflict: boolean()]) :: transaction()
  def put(t, key, value, opts \\ []) when is_binary(key) and is_binary(value) do
    cast(t, {:set_key, key, value, opts})
    t
  end

  @spec atomic(transaction(), atom(), key(), binary()) :: transaction()
  def atomic(t, op, key, value) when is_atom(op) and is_binary(key) and is_binary(value) do
    cast(t, {:atomic, op, key, value})
    t
  end

  @spec rollback(reason :: term()) :: no_return()
  def rollback(reason), do: throw({__MODULE__, :rollback, reason})

  @spec transaction(cluster :: module(), (transaction() -> result), opts :: keyword()) :: result when result: any()
  def transaction(cluster, fun, _opts \\ []) do
    tx_key = tx_key(cluster)

    case Process.get(tx_key) do
      nil ->
        run_new_transaction(cluster, fun, tx_key)

      existing_txn ->
        run_nested_transaction(existing_txn, fun)
    end
  end

  defp run_new_transaction(cluster, fun, tx_key) do
    restart_fn = fn ->
      {:ok, gateway} = cluster.fetch_gateway()
      {:ok, txn} = Gateway.begin_transaction(gateway)
      Process.put(tx_key, txn)
      txn
    end

    run_transaction(restart_fn.(), fun, restart_fn)
  after
    Process.delete(tx_key)
  end

  defp run_nested_transaction(txn, fun) do
    restart_fn = fn ->
      GenServer.call(txn, :nested_transaction, :infinity)
      txn
    end

    run_transaction(restart_fn.(), fun, restart_fn)
  end

  defp run_transaction(txn, fun, restart_fn) do
    result = fun.(txn)

    case GenServer.call(txn, :commit) do
      :ok ->
        result

      {:ok, _} ->
        result

      {:error, reason} ->
        throw({__MODULE__, :retryable_failure, reason})
    end
  rescue
    exception ->
      GenServer.cast(txn, :rollback)
      reraise exception, __STACKTRACE__
  catch
    {__MODULE__, :rollback, reason} ->
      GenServer.cast(txn, :rollback)
      {:error, reason}

    {__MODULE__, :retryable_failure, _reason} ->
      GenServer.cast(txn, :rollback)
      run_transaction(restart_fn.(), fun, restart_fn)

    {__MODULE__, :transaction_error, reason, operation, key} ->
      GenServer.cast(txn, :rollback)
      raise Bedrock.TransactionError, reason: reason, operation: operation, key: key
  end

  defp tx_key(cluster), do: {:transaction, cluster}
end
