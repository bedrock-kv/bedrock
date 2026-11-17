defmodule Bedrock.Internal.TransactionBuilder.RangeReads do
  @moduledoc """
  Range read operations for the Transaction Builder.

  This module handles all range-related fetching operations, including
  version management, storage coordination, and result processing for
  both regular key ranges and key selector ranges.
  """

  import Bedrock.Internal.TransactionBuilder.ReadVersions, only: [ensure_read_version: 2]

  alias Bedrock.DataPlane.Materializer
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
  def get_range(state, range, batch_size, opts \\ [])

  def get_range(state, {min_key, max_key}, _batch_size, _opts) when min_key >= max_key, do: {state, {:ok, {[], false}}}

  def get_range(state, {min_key, max_key}, batch_size, opts) do
    source = Keyword.get(opts, :storage_get_range_fn, &Materializer.get_range/5)

    case ensure_read_version(state, opts) do
      {:ok, state} -> scan(state, min_key, max_key, batch_size, source, opts, [])
      {:failure, reasons} -> {state, {:failure, reasons}}
    end
  end

  defp scan(state, cursor, end_key, remaining, source, opts, accumulated) do
    operation = &source.(&1, cursor, end_key, &2, limit: remaining, timeout: &3)

    case StorageRacing.race_storage_servers(state, cursor, operation) do
      {state, {:ok, {{rows, source_more}, {shard_start, shard_end}}}}
      when shard_start <= cursor and cursor < shard_end ->
        proof_end = proof_end(rows, source_more, cursor, min(end_key, shard_end))

        if proof_end <= cursor do
          {state, {:failure, %{unavailable: []}}}
        else
          consume_page(state, rows, proof_end, {cursor, end_key, remaining}, source, opts, accumulated)
        end

      {state, {:ok, _invalid_coverage}} ->
        {state, {:failure, %{unavailable: []}}}

      {state, {:failure, reasons}} ->
        {state, {:failure, reasons}}
    end
  end

  defp proof_end([], true, cursor, _end_key), do: cursor
  defp proof_end(_rows, false, _cursor, end_key), do: end_key
  defp proof_end(rows, true, _cursor, end_key), do: min(end_key, rows |> List.last() |> elem(0) |> Key.key_after())

  defp consume_page(state, rows, proof_end, {cursor, end_key, remaining}, source, opts, accumulated) do
    visible = Tx.range_view(state.tx, rows, {cursor, proof_end})
    {page, excess} = Enum.split(visible, remaining)
    consumed_end = if excess == [], do: proof_end, else: page |> List.last() |> elem(0) |> Key.key_after()

    tx =
      if Keyword.get(opts, :snapshot, false),
        do: state.tx,
        else: Tx.add_read_conflict_range(state.tx, cursor, consumed_end)

    state = %{state | tx: tx}
    accumulated = Enum.reverse(page, accumulated)
    remaining = remaining - length(page)

    cond do
      excess != [] -> {state, {:ok, {Enum.reverse(accumulated), true}}}
      proof_end >= end_key -> {state, {:ok, {Enum.reverse(accumulated), false}}}
      remaining == 0 -> {state, {:ok, {Enum.reverse(accumulated), true}}}
      true -> scan(state, proof_end, end_key, remaining, source, opts, accumulated)
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
          opts :: [storage_get_range_fn: storage_get_range_selector_fn(), snapshot: boolean()]
        ) ::
          {State.t(),
           {:ok, {[Bedrock.key_value()], more :: boolean()}}
           | {:error, :timeout | :unavailable | :version_too_new}
           | {:failure,
              %{(:timeout | :unavailable | :version_too_old | :no_servers_to_race | :layout_lookup_failed) => [pid()]}}}
  def get_range_selectors(state, start_selector, end_selector, batch_size, opts \\ [])

  def get_range_selectors(
        state,
        %KeySelector{key: start_key, or_equal: true, offset: 0},
        %KeySelector{key: end_key, or_equal: true, offset: 0},
        batch_size,
        opts
      ), do: get_range(state, {start_key, end_key}, batch_size, opts)

  def get_range_selectors(state, start_selector, end_selector, batch_size, opts) do
    storage_get_range_fn = Keyword.get(opts, :storage_get_range_fn, &Materializer.get_range/5)

    case ensure_read_version(state, opts) do
      {:ok, state} ->
        execute_range_query(
          state,
          start_selector.key,
          &storage_get_range_fn.(&1, start_selector, end_selector, &2, limit: batch_size, timeout: &3),
          &range_from_batch/1,
          opts
        )

      {:failure, failures_by_reason} ->
        {state, {:failure, failures_by_reason}}
    end
  end

  # Private helper functions

  defp execute_range_query(state, racing_key, operation_fn, range_fn, opts) do
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

        updated_tx =
          if Keyword.get(opts, :snapshot, false),
            do: %{updated_tx | reads: state.tx.reads, range_reads: state.tx.range_reads},
            else: updated_tx

        {%{state | tx: updated_tx}, {:ok, {merged_batch_results, has_more}}}

      {state, {:failure, failures_by_reason}} ->
        {state, {:failure, failures_by_reason}}
    end
  end

  defp range_from_batch([{min_key, _value} | _] = rows),
    do: {min_key, rows |> List.last() |> elem(0) |> Key.key_after()}
end
