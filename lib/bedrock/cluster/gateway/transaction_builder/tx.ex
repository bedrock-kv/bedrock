defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx do
  @moduledoc """
  Opaque transaction type for building and committing database operations.

  This module provides an immutable transaction structure that accumulates
  reads, writes, and range operations. Transactions can be committed to
  produce the final mutation list and conflict ranges for resolution.
  """
  @type op ::
          :set
          | :clear
          | :clear_range
  @type mutation :: {op(), binary(), binary() | nil}

  @type t :: %__MODULE__{
          mutations: [mutation()],
          writes: %{binary() => binary() | :clear},
          reads: %{binary() => binary() | :clear},
          range_writes: [{binary(), binary()}],
          range_reads: [{binary(), binary()}]
        }
  defstruct mutations: [],
            writes: %{},
            reads: %{},
            range_writes: [],
            range_reads: []

  def new, do: %__MODULE__{}

  @spec get(
          t(),
          k :: binary(),
          fetch_fn :: (binary(), state :: any() -> {{:ok, binary()} | {:error, reason}, any()}),
          state :: any()
        ) :: {t(), {:ok, binary()} | {:error, reason}, state :: any()}
        when reason: term()
  def get(t, k, fetch_fn, state) when is_binary(k) do
    # First check writes cache (highest priority)
    case get_write(t, k) do
      nil ->
        # Not in writes, check reads cache
        case get_read(t, k) do
          nil ->
            # Not in reads either, fetch from storage
            fetch(t, k, fetch_fn, state)

          :clear ->
            # Previously fetched but not found
            {t, {:error, :not_found}, state}

          value ->
            # Found in reads cache
            {t, {:ok, value}, state}
        end

      :clear ->
        # Cleared in writes (takes precedence over reads)
        {t, {:error, :not_found}, state}

      value ->
        # Found in writes cache (highest priority)
        {t, {:ok, value}, state}
    end
  end

  @spec get_range(
          t(),
          binary(),
          binary(),
          (binary(), binary(), keyword(), any() -> {[{binary(), binary()}], any()}),
          any(),
          keyword()
        ) :: {t(), [{binary(), binary()}], any()}
  def get_range(t, s, e, read_range_fn, state, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)

    # Your t.writes already has the current state!
    tx_visible =
      t.writes
      |> Enum.filter(fn {k, v} ->
        k >= s and k < e and v != :clear
      end)
      |> Map.new()

    # Check what ranges were cleared (need to exclude from DB fetch)
    cleared_ranges =
      t.mutations
      |> Enum.filter(fn
        {:clear_range, _, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {:clear_range, cs, ce} -> {cs, ce} end)

    # Smart DB fetch: only get data we haven't modified
    # and that isn't in a cleared range
    {db_results, new_state} =
      fetch_missing_data_if_needed(
        tx_visible,
        limit,
        t,
        cleared_ranges,
        read_range_fn,
        state,
        s,
        e
      )

    # Merge (tx takes precedence)
    merged = Map.merge(db_results, tx_visible)

    result =
      merged
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.take(limit)

    # Update tracking
    new_t = %{
      t
      | range_reads: add_or_merge(t.range_reads, s, e),
        reads: Map.merge(t.reads, Map.new(result))
    }

    {new_t, result, new_state}
  end

  def set(t, k, v) when is_binary(k) and is_binary(v) do
    t
    |> record_mutation({:set, k, v})
    |> put_write(k, v)
  end

  def clear(t, k) when is_binary(k) do
    t
    |> record_mutation({:clear_range, k, next_key(k)})
    |> put_clear(k)
  end

  def clear_range(t, s, e) when is_binary(s) and is_binary(e) do
    t
    |> remove_ops_in_range(s, e)
    |> remove_writes_in_range(s, e)
    |> clear_reads_in_range(s, e)
    |> add_write_range(s, e)
    |> record_mutation({:clear_range, s, e})
  end

  def commit(t) do
    # Single pass to build everything
    {mutations, range_writes, range_reads} =
      build_commit_data(t)

    %{
      mutations: mutations,
      write_conflicts: coalesce_ranges(range_writes),
      read_conflicts: coalesce_ranges(range_reads ++ t.range_reads)
    }
  end

  defp build_commit_data(t) do
    # Single pass through mutations
    {final_mutations, range_writes} =
      t.mutations
      |> Enum.reduce({[], []}, fn
        {:set, k, v}, {muts, ranges} ->
          {[{:set, k, v} | muts], [{k, next_key(k)} | ranges]}

        {:clear, k, nil}, {muts, ranges} ->
          {[{:clear_range, k, next_key(k)} | muts], [{k, next_key(k)} | ranges]}

        {:clear_range, s, e}, {muts, ranges} ->
          {[{:clear_range, s, e} | muts], [{s, e} | ranges]}
      end)

    # Build read ranges from reads map
    range_reads =
      t.reads
      |> Map.keys()
      |> Enum.map(fn k -> {k, next_key(k)} end)

    {final_mutations, range_writes, range_reads}
  end

  defp remove_ops_in_range(t, s, e) do
    %{
      t
      | mutations:
          Enum.reject(t.mutations, fn
            {:set, k, _} -> k >= s && k < e
            {:clear, k} -> k >= s && k < e
            _ -> false
          end)
    }
  end

  defp remove_writes_in_range(t, s, e) do
    %{
      t
      | writes:
          t.writes
          |> Enum.reject(fn {k, _} -> k >= s && k < e end)
          |> Map.new()
    }
  end

  defp clear_reads_in_range(t, s, e) do
    %{
      t
      | reads:
          t.reads
          |> Map.new(fn
            {k, _} when k >= s and k < e -> {k, :clear}
            kv -> kv
          end)
    }
  end

  defp add_write_range(t, s, e) do
    %{
      t
      | range_writes: t.range_writes |> add_or_merge(s, e)
    }
  end

  def add_or_merge([], s, e), do: [{s, e}]
  def add_or_merge([{hs, he} | rest], s, e) when e < hs, do: [{s, e}, {hs, he} | rest]
  def add_or_merge([{hs, he} | rest], s, e) when he < s, do: [{hs, he} | add_or_merge(rest, s, e)]
  def add_or_merge([{hs, he} | rest], s, e), do: add_or_merge(rest, min(hs, s), max(he, e))

  defp coalesce_ranges(ranges) do
    ranges
    |> Enum.sort()
    |> Enum.reduce([], fn {s, e}, acc ->
      case acc do
        [] ->
          [{s, e}]

        [{last_s, last_e} | rest] when s <= last_e ->
          # Overlapping or adjacent - merge
          [{last_s, max(e, last_e)} | rest]

        _ ->
          [{s, e} | acc]
      end
    end)
    |> Enum.reverse()
  end

  @spec get_write(t(), k :: binary()) :: binary() | :clear | nil
  defp get_write(t, k), do: Map.get(t.writes, k)

  @spec get_read(t(), k :: binary()) :: binary() | :clear | nil
  defp get_read(t, k), do: Map.get(t.reads, k)

  @spec put_clear(t(), k :: binary()) :: t()
  defp put_clear(t, k), do: %{t | writes: Map.put(t.writes, k, :clear)}

  @spec put_write(t(), k :: binary(), v :: binary()) :: t()
  defp put_write(t, k, v), do: %{t | writes: Map.put(t.writes, k, v)}

  defp fetch(t, k, fetch_fn, state) do
    {result, new_state} = fetch_fn.(k, state)

    case result do
      {:ok, v} ->
        {%{t | reads: Map.put(t.reads, k, v)}, result, new_state}

      {:error, :not_found} ->
        {%{t | reads: Map.put(t.reads, k, :clear)}, result, new_state}

      result ->
        {t, result, new_state}
    end
  end

  @spec record_mutation(t(), mutation()) :: t()
  defp record_mutation(t, op) do
    %{t | mutations: [op | t.mutations]}
  end

  defp next_key(k), do: k <> <<0>>

  # Helper function to reduce nesting in get_range
  defp fetch_missing_data_if_needed(
         tx_visible,
         limit,
         t,
         cleared_ranges,
         read_range_fn,
         state,
         s,
         e
       )
       when map_size(tx_visible) < limit do
    fetch_and_filter_range_data(t, cleared_ranges, read_range_fn, state, s, e, limit)
  end

  defp fetch_missing_data_if_needed(
         _tx_visible,
         _limit,
         _t,
         _cleared_ranges,
         _read_range_fn,
         state,
         _s,
         _e
       ) do
    {%{}, state}
  end

  defp fetch_and_filter_range_data(t, cleared_ranges, read_range_fn, state, s, e, limit) do
    {range_data, updated_state} = read_range_fn.(state, s, e, limit: limit)

    filtered_data =
      range_data
      |> Enum.reject(&should_skip_key?(&1, t.writes, cleared_ranges))
      |> Map.new()

    {filtered_data, updated_state}
  end

  defp should_skip_key?({k, _v}, writes, cleared_ranges) do
    # Skip if we already have it in writes or if it's in a cleared range
    Map.has_key?(writes, k) or
      Enum.any?(cleared_ranges, fn {cs, ce} -> k >= cs and k < ce end)
  end
end
