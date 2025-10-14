defmodule Bedrock.Internal.TransactionBuilder.RangeReads do
  @moduledoc """
  Range read operations for the Transaction Builder.

  This module handles all range-related fetching operations, including
  version management, storage coordination, and result processing for
  both regular key ranges and key selector ranges.
  """

  import Bedrock.Internal.TransactionBuilder.ReadVersions, only: [ensure_read_version!: 2]

  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.Key
  alias Bedrock.KeySelector

  @type storage_get_range_fn() :: (pid(), binary(), binary(), Bedrock.version(), keyword() ->
                                     {:ok, {[Bedrock.key_value()], more :: boolean()}} | {:error, atom()})

  @type storage_get_range_selector_fn() :: (pid(), KeySelector.t(), KeySelector.t(), Bedrock.version(), keyword() ->
                                              {:ok, {[Bedrock.key_value()], more :: boolean()}} | {:error, atom()})

  @type range_fn :: ([Bedrock.key_value()] -> Bedrock.key_range())

  @doc """
  Execute a range batch query against storage servers.

  This function handles version initialization, storage team coordination,
  and result processing for range queries with regular keys.
  """
  @spec get_range(
          State.t(),
          Bedrock.key_range(),
          batch_size :: pos_integer(),
          opts :: [storage_get_range_fn: storage_get_range_fn(), snapshot: boolean()]
        ) ::
          {State.t(),
           {:ok, {[{binary(), Bedrock.value()}], more :: boolean()}}
           | {:error, :timeout | :unavailable | :version_too_new}
           | {:failure,
              %{(:timeout | :unavailable | :version_too_old | :no_servers_to_race | :layout_lookup_failed) => [pid()]}}}
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
          opts :: [storage_get_range_fn: storage_get_range_selector_fn(), snapshot: boolean()]
        ) ::
          {State.t(),
           {:ok, {[Bedrock.key_value()], more :: boolean()}}
           | {:error, :timeout | :unavailable | :version_too_new}
           | {:failure,
              %{(:timeout | :unavailable | :version_too_old | :no_servers_to_race | :layout_lookup_failed) => [pid()]}}}
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

  defp execute_range_query(state, racing_key, operation_fn, range_fn) do
    state
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      {state, {:ok, {{[], false}, _shard_range}}} ->
        {state, {:ok, {[], false}}}

      {state, {:ok, {{results, has_more}, shard_range}}} ->
        {updated_tx, merged_batch_results} =
          Tx.merge_storage_range_with_writes(
            state.tx,
            results,
            has_more,
            range_fn.(results),
            shard_range
          )

        {%{state | tx: updated_tx}, {:ok, {merged_batch_results, has_more}}}

      {state, {:failure, failures_by_reason}} ->
        {state, {:failure, failures_by_reason}}
    end
  end

  defp range_from_batch([{min_key, _value} | rest]), do: {min_key, rest |> List.last() |> elem(0) |> Key.key_after()}
  defp range_from_batch(_), do: raise("Batch results cannot be empty")
end
