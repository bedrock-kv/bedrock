defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls

  alias Bedrock.Internal.RangeQuery

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
  def fetch(t, key), do: call(t, {:fetch, key}, :infinity)

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

  @spec put(transaction(), key(), value()) :: transaction()
  def put(t, key, value) do
    cast(t, {:put, key, value})
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
