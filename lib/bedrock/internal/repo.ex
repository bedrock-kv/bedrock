defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls

  alias Bedrock.Internal.Transaction

  @opaque transaction :: pid()

  @spec transaction(
          cluster :: module(),
          (transaction() ->
             :ok | {:ok, result} | :error | {:error, reason}),
          opts :: [
            retry_count: pos_integer(),
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          :ok | {:ok, result} | :error | {:error, reason}
        when result: term(), reason: term()
  def transaction(cluster, fun, opts \\ []) do
    with {:ok, txn} <- Transaction.start_link(cluster, opts) do
      result = fun.(txn)

      case result do
        :ok -> commit(txn)
        {:ok, _result} -> commit(txn)
        _ -> rollback(txn)
      end

      result
    end
  end

  @spec get(transaction(), Bedrock.key()) :: nil | Bedrock.value()
  def get(t, key) when is_binary(key),
    do: call(t, {:get, key}, :infinity)

  @spec put(transaction(), Bedrock.key(), Bedrock.value()) :: :ok
  def put(t, key, value) when is_binary(key) and is_binary(value),
    do: cast(t, {:put, key, value})

  @spec commit(transaction(), opts :: [timeout_in_ms :: pos_integer()]) ::
          :ok | {:error, :aborted}
  def commit(t, opts \\ []),
    do: call(t, :commit, opts[:timeout_in_ms] || default_timeout_in_ms())

  @spec rollback(transaction()) :: :ok
  def rollback(t),
    do: :ok = GenServer.stop(t, :normal)

  def default_timeout_in_ms(), do: 1_000
end
