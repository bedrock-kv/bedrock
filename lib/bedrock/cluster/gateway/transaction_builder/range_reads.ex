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
  @spec get_range(
          State.t(),
          Bedrock.key_range(),
          batch_size :: pos_integer(),
          opts :: keyword()
        ) ::
          {State.t(), {:ok, {[{binary(), Bedrock.value()}], more :: boolean()}} | {:error, atom()}}
  def get_range(state, {min_key, max_key_ex} = _range, batch_size, opts \\ []) do
    storage_get_range_fn = Keyword.get(opts, :storage_get_range_fn, &Storage.get_range/5)

    operation_fn = fn storage_server, state ->
      storage_get_range_fn.(
        storage_server,
        min_key,
        max_key_ex,
        state.read_version,
        limit: batch_size,
        timeout: state.fetch_timeout_in_ms
      )
    end

    state
    |> ensure_read_version!(opts)
    |> StorageRacing.race_storage_servers(min_key, operation_fn)
    |> case do
      {:ok, {{results, has_more}, shard_range}, state} ->
        {updated_tx, batch_results} =
          Tx.merge_storage_range_with_writes(
            state.tx,
            results,
            has_more,
            {min_key, max_key_ex},
            shard_range
          )

        {%{state | tx: updated_tx}, {:ok, {batch_results, has_more}}}

      {:error, reason, state} ->
        {state, {:error, reason}}
    end
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
  def get_range_selectors(state, start_selector, end_selector, batch_size, opts) do
    storage_get_range_fn = Keyword.get(opts, :storage_get_range_fn, &Storage.get_range/5)

    operation_fn = fn storage_server, state ->
      storage_get_range_fn.(
        storage_server,
        start_selector,
        end_selector,
        state.read_version,
        limit: batch_size,
        timeout: state.fetch_timeout_in_ms
      )
    end

    state
    |> ensure_read_version!(opts)
    |> StorageRacing.race_storage_servers(start_selector.key, operation_fn)
    |> case do
      {:ok, {{results, has_more}, shard_range}, state} ->
        # For KeySelectors, we use the selector keys as the query range bounds
        # since the actual range is resolved at the storage level
        {updated_tx, batch_results} =
          Tx.merge_storage_range_with_writes(
            state.tx,
            results,
            has_more,
            {start_selector.key, end_selector.key},
            shard_range
          )

        {%{state | tx: updated_tx}, {:ok, {batch_results, has_more}}}

      {:error, reason, state} ->
        {state, {:error, reason}}
    end
  end
end
