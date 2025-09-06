defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeReads do
  @moduledoc """
  Range read operations for the Transaction Builder.

  This module handles all range-related fetching operations, including
  version management, storage coordination, and result processing for
  both regular key ranges and key selector ranges.
  """

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [ensure_read_version!: 2]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.StorageRacing
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Key
  alias Bedrock.KeySelector

  @type next_read_version_fn() :: (State.t() ->
                                     {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
                                     | {:error, atom()})
  @type time_fn() :: (-> integer())

  @doc """
  Execute a range batch query against storage servers.

  This function handles version initialization, storage team coordination,
  and result processing for range queries with regular keys.
  """
  @spec get_range(State.t(), Bedrock.key_range(), batch_size :: pos_integer(), opts :: keyword()) ::
          {State.t(), {:ok, {[{binary(), Bedrock.value()}], more :: boolean()}} | {:error, atom()}}
  def get_range(state, {min_key, max_key_ex} = range, batch_size, opts \\ []) do
    storage_get_range_fn = Keyword.get(opts, :storage_get_range_fn, &Storage.get_range/5)

    state
    |> ensure_read_version!(opts)
    |> execute_range_query(
      min_key,
      &storage_get_range_fn.(&1, min_key, max_key_ex, &2, limit: batch_size, timeout: &3),
      fn _results -> range end
    )
  end

  @doc """
  Fetch a KeySelector range within the transaction context.

  This handles the transaction state management in addition to KeySelector range resolution.
  """
  @spec get_range_selectors(
          State.t(),
          KeySelector.t(),
          KeySelector.t(),
          batch_size :: pos_integer(),
          opts :: keyword()
        ) ::
          {State.t(), {:ok, {[Bedrock.key_value()], more :: boolean()}} | {:error, atom()}}
  def get_range_selectors(state, start_selector, end_selector, batch_size, opts \\ []) do
    storage_get_range_fn = Keyword.get(opts, :storage_get_range_fn, &Storage.get_range/5)

    state
    |> ensure_read_version!(opts)
    |> execute_range_query(
      start_selector.key,
      &storage_get_range_fn.(&1, start_selector, end_selector, &2, limit: batch_size, timeout: &3),
      &range_from_batch/1
    )
  end

  # Private helper functions

  @spec execute_range_query(
          State.t(),
          racing_key :: binary(),
          operation_fn :: (pid(), Bedrock.version(), Bedrock.timeout_in_ms() -> {:ok, any()} | {:error, any()}),
          range_fn :: ([Bedrock.key_value()] -> Bedrock.key_range())
        ) ::
          {State.t(), {:ok, {[Bedrock.key_value()], more :: boolean()}} | {:error, atom()}}
  defp execute_range_query(state, racing_key, operation_fn, range_fn) do
    state
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      {state, {:ok, {{[], false}, _shard_range}}} ->
        {state, {:ok, {[], false}}}

      {state, {:ok, {{results, has_more}, shard_range}}} ->
        {updated_tx, batch_results} =
          Tx.merge_storage_range_with_writes(
            state.tx,
            results,
            has_more,
            range_fn.(results),
            shard_range
          )

        {%{state | tx: updated_tx}, {:ok, {batch_results, has_more}}}

      {state, {:error, reason}} ->
        {state, {:error, reason}}
    end
  end

  defp range_from_batch([{min_key, _value} | rest]),
    do: {min_key, rest |> List.last() |> elem(0) |> Key.next_key_after()}

  defp range_from_batch(_), do: raise("Batch results cannot be empty")
end
