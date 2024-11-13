defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls

  alias Bedrock.Cluster.Gateway

  @opaque transaction :: pid()
  @type key :: term()
  @type value :: term()

  @spec transaction(
          cluster :: module(),
          (transaction() -> result),
          opts :: [
            retry_count: pos_integer(),
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: result
        when result: term()
  def transaction(cluster, fun, opts \\ []) do
    with {:ok, gateway} <- cluster.fetch_gateway(),
         {:ok, txn} <- Gateway.begin_transaction(gateway, opts) do
      result =
        try do
          fun.(txn)
        rescue
          exception ->
            rollback(txn)
            reraise exception, __STACKTRACE__
        end

      if :ok == result || (is_tuple(result) and :ok == elem(result, 0)) do
        case commit(txn) do
          :ok ->
            result

          {:error, reason} when reason in [:timeout, :aborted, :unavailable] ->
            retry_count = opts[:retry_count] || 0

            if retry_count > 0 do
              opts = Keyword.put(opts, :retry_count, retry_count - 1)
              transaction(cluster, fun, opts)
            else
              raise "Transaction failed: #{inspect(reason)}"
            end
        end
      else
        rollback(txn)
        result
      end
    end
  end

  @spec nested_transaction(transaction()) :: transaction()
  def nested_transaction(t), do: call(t, :nested_transaction, :infinity)

  @spec fetch(transaction(), key()) :: {:ok, value()} | :error
  def fetch(t, key) do
    case get(t, key) do
      nil -> :error
      value -> {:ok, value}
    end
  end

  @spec fetch!(transaction(), key()) :: value()
  def fetch!(t, key) do
    case get(t, key) do
      nil -> raise "Key not found: #{inspect(key)}"
      value -> value
    end
  end

  @spec get(transaction(), key()) :: nil | value()
  def get(t, key),
    do: call(t, {:get, key}, :infinity)

  @spec put(transaction(), key(), value()) :: transaction()
  def put(t, key, value) do
    cast(t, {:put, key, value})
    t
  end

  @spec commit(transaction(), opts :: [timeout_in_ms :: pos_integer()]) ::
          :ok | {:error, :aborted}
  def commit(t, opts \\ []),
    do: call(t, :commit, opts[:timeout_in_ms] || default_timeout_in_ms())

  @spec rollback(transaction()) :: :ok
  def rollback(t),
    do: cast(t, :rollback)

  def default_timeout_in_ms(), do: 1_000
end
