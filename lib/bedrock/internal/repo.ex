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

  @spec fetch(transaction(), key()) :: {:ok, value()} | {:error, atom()} | :error
  def fetch(t, key), do: call(t, {:get, key}, :infinity)

  @spec fetch!(transaction(), key()) :: value()
  def fetch!(t, key) do
    case fetch(t, key) do
      {:error, _} -> raise "Key not found: #{inspect(key)}"
      {:ok, value} -> value
    end
  end

  @spec get(transaction(), key()) :: nil | value()
  def get(t, key) do
    case fetch(t, key) do
      {:error, _} -> nil
      {:ok, value} -> value
    end
  end

  @spec fetch_key_selector(transaction(), KeySelector.t()) ::
          {:ok, {resolved_key :: key(), value()}}
          | {:error, atom()}
  def fetch_key_selector(t, %KeySelector{} = key_selector) do
    call(t, {:get_key_selector, key_selector}, :infinity)
  end

  @spec fetch_key_selector!(transaction(), KeySelector.t()) :: {resolved_key :: key(), value()}
  def fetch_key_selector!(t, %KeySelector{} = key_selector) do
    case fetch_key_selector(t, key_selector) do
      {:error, _} -> raise "KeySelector not found: #{inspect(key_selector)}"
      {:ok, {resolved_key, value}} -> {resolved_key, value}
    end
  end

  @spec get_key_selector(transaction(), KeySelector.t()) ::
          nil | {resolved_key :: key(), value()}
  def get_key_selector(t, %KeySelector{} = key_selector) do
    case fetch_key_selector(t, key_selector) do
      {:error, _} -> nil
      {:ok, {resolved_key, value}} -> {resolved_key, value}
    end
  end

  @spec range_fetch(transaction(), start_key :: key(), end_key :: key(), opts :: [limit: pos_integer()]) ::
          {:ok, [{key(), value()}]} | {:error, :not_supported | :unavailable | :timeout}
  def range_fetch(t, start_key, end_key, opts \\ []) do
    {:ok, t |> range_stream(start_key, end_key, opts) |> Enum.to_list()}
  rescue
    RuntimeError -> {:error, :unavailable}
  end

  @spec range_stream(
          transaction(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            batch_size: pos_integer(),
            timeout: pos_integer(),
            limit: pos_integer(),
            mode: :individual | :batch
          ]
        ) :: Enumerable.t({any(), any()})
  def range_stream(t, start_key, end_key, opts \\ []), do: RangeQuery.stream(t, start_key, end_key, opts)

  @spec range(
          transaction(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            batch_size: pos_integer(),
            timeout: pos_integer(),
            limit: pos_integer(),
            mode: :individual | :batch
          ]
        ) :: Enumerable.t({any(), any()})
  def range(t, start_key, end_key, opts \\ []), do: RangeQuery.stream(t, start_key, end_key, opts)

  @spec range_fetch_key_selectors(
          transaction(),
          start_selector :: KeySelector.t(),
          end_selector :: KeySelector.t(),
          opts :: [limit: pos_integer()]
        ) ::
          {:ok, [{key(), value()}]} | {:error, :not_supported | :unavailable | :timeout}
  def range_fetch_key_selectors(t, %KeySelector{} = start_selector, %KeySelector{} = end_selector, opts \\ []) do
    call(t, {:get_range_selectors, start_selector, end_selector, opts}, :infinity)
  end

  @spec range_stream_key_selectors(
          transaction(),
          start_selector :: KeySelector.t(),
          end_selector :: KeySelector.t(),
          opts :: [
            batch_size: pos_integer(),
            timeout: pos_integer(),
            limit: pos_integer(),
            mode: :individual | :batch
          ]
        ) :: Enumerable.t({any(), any()})
  def range_stream_key_selectors(t, %KeySelector{} = start_selector, %KeySelector{} = end_selector, opts \\ []) do
    # For now, resolve and delegate to normal range_stream
    # A more sophisticated implementation would handle KeySelector streaming directly
    with {:ok, {resolved_start, _}} <- fetch_key_selector(t, start_selector),
         {:ok, {resolved_end, _}} <- fetch_key_selector(t, end_selector) do
      range_stream(t, resolved_start, resolved_end, opts)
    else
      _ -> raise RuntimeError, "Failed to resolve KeySelectors for streaming"
    end
  end

  @spec put(transaction(), key(), value()) :: transaction()
  def put(t, key, value) do
    cast(t, {:set_key, key, value})
    t
  end

  @spec commit(transaction(), opts :: [timeout_in_ms :: Bedrock.timeout_in_ms()]) ::
          {:ok, Bedrock.version()}
          | {:error, :unavailable | :timeout | :unknown}
  def commit(t, opts \\ []), do: call(t, :commit, opts[:timeout_in_ms] || default_timeout_in_ms())

  @spec rollback(transaction()) :: :ok
  def rollback(t), do: cast(t, :rollback)

  @spec default_timeout_in_ms() :: pos_integer()
  def default_timeout_in_ms, do: 1_000
end
