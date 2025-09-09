defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls

  alias Bedrock.Internal.RangeQuery
  alias Bedrock.KeySelector

  @opaque transaction :: pid()
  @type key :: term()
  @type value :: term()

  @spec nested_transaction(transaction(), function()) :: term()
  def nested_transaction(txn, fun) do
    call(txn, :nested_transaction, :infinity)
    fun.(txn)
  rescue
    exception ->
      rollback(txn)
      reraise exception, __STACKTRACE__
  end

  @spec rollback(transaction()) :: :ok
  def rollback(t), do: cast(t, :rollback)

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
    case call(t, {:get, key, opts}, :infinity) do
      {:ok, value} -> value
      {:error, :not_found} -> nil
    end
  end

  @spec select(transaction(), KeySelector.t()) :: nil | {resolved_key :: key(), value()}
  @spec select(transaction(), KeySelector.t(), opts :: keyword()) :: nil | {resolved_key :: key(), value()}
  def select(t, %KeySelector{} = key_selector, opts \\ []),
    do: call(t, {:get_key_selector, key_selector, opts}, :infinity)

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
  def put(t, key, value, opts \\ []) do
    cast(t, {:set_key, key, value, opts})
    t
  end

  @spec atomic(transaction(), atom(), key(), binary()) :: transaction()
  def atomic(t, op, key, value) do
    cast(t, {:atomic, op, key, value})
    t
  end

  @spec commit(transaction(), opts :: [timeout_in_ms :: Bedrock.timeout_in_ms()]) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown}
  def commit(t, opts \\ []), do: call(t, :commit, opts[:timeout_in_ms] || default_timeout_in_ms())

  @spec default_timeout_in_ms() :: pos_integer()
  def default_timeout_in_ms, do: 1_000
end
