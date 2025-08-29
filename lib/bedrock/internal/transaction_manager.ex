defmodule Bedrock.Internal.TransactionManager do
  @moduledoc """
  Manages the complete lifecycle of transactions including process dictionary
  management, retry logic, and coordination with the underlying transaction system.

  This module contains all the transaction business logic that was previously
  embedded in the Bedrock.Repo macro, making it testable and maintainable.
  """

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Internal.Repo

  @spec transaction(cluster :: module(), (Repo.transaction() -> result), opts :: keyword()) :: result when result: any()
  def transaction(cluster, fun, opts) do
    tx_key = tx_key(cluster)

    case Process.get(tx_key) do
      nil ->
        start_new_transaction(cluster, fun, opts, tx_key)

      existing_txn ->
        Repo.nested_transaction(existing_txn, fun)
    end
  end

  defp start_new_transaction(cluster, fun, opts, tx_key) do
    with {:ok, gateway} <- cluster.fetch_gateway(),
         {:ok, txn} <- Gateway.begin_transaction(gateway, opts) do
      Process.put(tx_key, txn)

      try do
        result = run_transaction_fun(txn, fun)

        if ok_result?(result) do
          handle_commit_result(txn, result, cluster, fun, opts, tx_key)
        else
          Repo.rollback(txn)
          result
        end
      after
        Process.delete(tx_key)
      end
    end
  end

  defp run_transaction_fun(txn, fun) do
    fun.(txn)
  rescue
    exception ->
      Repo.rollback(txn)
      reraise exception, __STACKTRACE__
  end

  defp ok_result?(:ok), do: true
  defp ok_result?(tuple) when is_tuple(tuple), do: elem(tuple, 0) == :ok
  defp ok_result?(_), do: false

  defp handle_commit_result(txn, result, cluster, fun, opts, tx_key) do
    txn
    |> Repo.commit()
    |> case do
      {:ok, _} ->
        result

      {:error, reason} when reason in [:timeout, :aborted, :unavailable] ->
        handle_commit_retry(cluster, fun, opts, reason, tx_key)
    end
  end

  defp handle_commit_retry(cluster, fun, opts, reason, tx_key) do
    retry_count = opts[:retry_count] || 0

    if retry_count > 0 do
      start_new_transaction(cluster, fun, Keyword.put(opts, :retry_count, retry_count - 1), tx_key)
    else
      raise "Transaction failed: #{inspect(reason)}"
    end
  end

  @spec tx_key(module()) :: {:transaction, module()}
  def tx_key(cluster), do: {:transaction, cluster}
end
