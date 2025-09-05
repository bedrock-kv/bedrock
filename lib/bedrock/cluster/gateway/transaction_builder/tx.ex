defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx do
  @moduledoc """
  Opaque transaction type for building and committing database operations.

  This module provides an immutable transaction structure that accumulates
  reads, writes, and range operations. Transactions can be committed to
  produce the final mutation list and conflict ranges for resolution.
  """

  alias Bedrock.DataPlane.Transaction

  @type key :: binary()
  @type value :: binary()
  @type range :: {start :: binary(), end_ex :: binary()}

  @type mutation ::
          {:set, key(), value()}
          | {:clear, key()}
          | {:clear_range, start :: binary(), end_ex :: binary()}

  @type t :: %__MODULE__{
          mutations: [mutation()],
          writes: :gb_trees.tree(key(), value() | :clear),
          reads: %{key() => value() | :clear},
          range_writes: [range()],
          range_reads: [range()]
        }
  defstruct mutations: [],
            writes: :gb_trees.empty(),
            reads: %{},
            range_writes: [],
            range_reads: []

  def new, do: %__MODULE__{}

  @doc """
  Merge a storage read result into the transaction state for conflict tracking.

  This function is used after KeySelector resolution to merge the resolved key
  and value into the transaction's read state, ensuring proper conflict detection.
  """
  @spec merge_storage_read(t(), key(), value() | :not_found) :: t()
  def merge_storage_read(t, key, :not_found) when is_binary(key), do: %{t | reads: Map.put(t.reads, key, :clear)}

  def merge_storage_read(t, key, value) when is_binary(key) and is_binary(value),
    do: %{t | reads: Map.put(t.reads, key, value)}

  @doc """
  Merge storage range read results into the transaction state for conflict tracking.

  This function is used after KeySelector range resolution to merge resolved keys
  and values into the transaction's read state, and add the range to range_reads.
  """
  @spec merge_storage_range_read(t(), key(), key(), [{key(), value()}]) :: t()
  def merge_storage_range_read(t, resolved_start_key, resolved_end_key, key_values)
      when is_binary(resolved_start_key) and is_binary(resolved_end_key) do
    # Add all individual key-value pairs to reads for conflict tracking
    updated_reads =
      Enum.reduce(key_values, t.reads, fn {key, value}, acc ->
        Map.put(acc, key, value)
      end)

    # Add the resolved range to range_reads for conflict tracking
    updated_range_reads = add_or_merge(t.range_reads, resolved_start_key, resolved_end_key)

    %{t | reads: updated_reads, range_reads: updated_range_reads}
  end

  @doc """
  Enhanced version of merge_storage_range_with_writes that handles pending writes
  correctly based on shard boundaries and has_more flag.

  When has_more = false, this indicates the storage server has given us all data
  in its authoritative range, so we should include pending writes beyond the
  storage results up to the boundary of the query range and shard range.
  """
  @spec merge_storage_range_with_writes(
          t(),
          [{key(), value()}],
          has_more :: boolean(),
          query_range :: {key(), key()},
          shard_range :: Bedrock.key_range()
        ) :: {t(), [{key(), value()}]}
  def merge_storage_range_with_writes(tx, storage_results, has_more, query_range, shard_range) do
    {query_start, query_end} = query_range
    {shard_start, shard_end} = shard_range

    # Calculate the effective range to scan for pending writes
    effective_start = max(query_start, shard_start)

    effective_end =
      case shard_end do
        # Unbounded shard, use query end
        :end -> query_end
        shard_end_key -> min(query_end, shard_end_key)
      end

    case {storage_results, has_more} do
      {[], false} ->
        # Empty storage and no more data - scan all pending writes in effective range
        scan_pending_writes(tx, effective_start, effective_end)

      {[], true} ->
        # Empty storage but more data available - just return empty with conflict tracking
        {add_range_conflict(tx, effective_start, effective_end), []}

      {[{_first_key, _} | _], false} ->
        # Have storage data and no more data - merge storage with writes within storage bounds,
        # then scan for additional writes beyond storage up to effective_end
        {last_key, _} = List.last(storage_results)

        # First merge storage results with overlapping writes (bounded)
        {tx_after_merge, merged_results} = merge_storage_with_bounded_writes(tx, storage_results, effective_end)

        # Then scan for additional pending writes beyond the merged range
        scan_start = next_key(last_key)

        {tx_final, additional_writes} =
          if scan_start < effective_end do
            scan_pending_writes(tx_after_merge, scan_start, effective_end)
          else
            {tx_after_merge, []}
          end

        final_results = merged_results ++ additional_writes
        {tx_final, final_results}

      {storage_results, true} ->
        # Have storage data and more available - only merge overlapping writes within storage bounds
        # Cannot include ANY writes beyond the last storage key because storage might have
        # more data between last storage key and our next write
        [{_first_key, _} | _] = storage_results
        {last_key, _} = List.last(storage_results)
        # Use exact next_key boundary - no writes beyond this point
        merge_storage_with_bounded_writes(tx, storage_results, next_key(last_key))
    end
  end

  # Helper functions for the enhanced merge_storage_range_with_writes

  defp scan_pending_writes(tx, start_key, end_key) when start_key >= end_key do
    # Empty range - just add conflict tracking
    {add_range_conflict(tx, start_key, end_key), []}
  end

  defp scan_pending_writes(tx, start_key, end_key) do
    # Scan transaction writes in the specified range
    tx_iterator = :gb_trees.iterator_from(start_key, tx.writes)
    writes_in_range = collect_writes_in_range(tx_iterator, end_key, [])

    # Add read conflict tracking
    updated_tx =
      case writes_in_range do
        [] ->
          # No writes found, still need to track the range for conflicts
          add_range_conflict(tx, start_key, end_key)

        [{first_write_key, _} | _] ->
          # Add individual writes to reads and track range
          updated_reads =
            Enum.reduce(writes_in_range, tx.reads, fn {key, value}, acc ->
              Map.put(acc, key, value)
            end)

          {last_write_key, _} = List.last(writes_in_range)
          updated_range_reads = add_or_merge(tx.range_reads, first_write_key, next_key(last_write_key))
          %{tx | reads: updated_reads, range_reads: updated_range_reads}
      end

    {updated_tx, writes_in_range}
  end

  defp collect_writes_in_range(iterator, end_key, acc) do
    case :gb_trees.next(iterator) do
      {key, value, next_iterator} when key < end_key ->
        collect_writes_in_range(next_iterator, end_key, [{key, value} | acc])

      _ ->
        Enum.reverse(acc)
    end
  end

  defp add_range_conflict(tx, start_key, end_key) when start_key < end_key do
    updated_range_reads = add_or_merge(tx.range_reads, start_key, end_key)
    %{tx | range_reads: updated_range_reads}
  end

  # Empty range
  defp add_range_conflict(tx, _start_key, _end_key), do: tx

  # Helper function to check if ranges overlap
  defp ranges_overlap?({start1, end1}, {start2, end2}) do
    start1 < end2 and start2 < end1
  end

  # Merge storage data with transaction writes, but only within the specified boundary
  defp merge_storage_with_bounded_writes(tx, storage_results, max_boundary) do
    [{first_key, _} | _] = storage_results
    {last_key, _} = List.last(storage_results)

    # Boundary for merging is the minimum of next_key(last_storage) and max_boundary
    merge_boundary = min(next_key(last_key), max_boundary)

    # Get overlapping clear ranges
    data_range = {first_key, merge_boundary}

    tx_clear_ranges =
      Enum.filter(tx.mutations, fn
        {:clear_range, s, e} -> ranges_overlap?(data_range, {s, e})
        _ -> false
      end)

    # Merge storage with transaction writes only up to the boundary
    tx_iterator = :gb_trees.iterator_from(first_key, tx.writes)

    {acc, _tx_iterator} =
      merge_ordered_results_bounded(storage_results, tx_iterator, tx_clear_ranges, [], merge_boundary)

    merged_results =
      acc
      |> filter_cleared_keys(tx_clear_ranges)
      |> Enum.reverse()

    # Add conflict tracking
    updated_tx =
      case merged_results do
        [] ->
          tx

        [{actual_first_key, _} | _] ->
          {actual_last_key, _} = List.last(merged_results)

          updated_reads =
            Enum.reduce(merged_results, tx.reads, fn {key, value}, acc ->
              Map.put(acc, key, value)
            end)

          updated_range_reads = add_or_merge(tx.range_reads, actual_first_key, next_key(actual_last_key))

          %{tx | reads: updated_reads, range_reads: updated_range_reads}
      end

    {updated_tx, merged_results}
  end

  # Bounded version of merge_ordered_results that stops at a boundary
  defp merge_ordered_results_bounded([], tx_iterator, clear_ranges, acc, boundary) do
    case :gb_trees.next(tx_iterator) do
      {tx_key, tx_value, iterator} when tx_key < boundary ->
        merge_ordered_results_bounded([], iterator, clear_ranges, [{tx_key, tx_value} | acc], boundary)

      _ ->
        {acc, tx_iterator}
    end
  end

  defp merge_ordered_results_bounded(storage_list, tx_iterator, clear_ranges, acc, boundary) do
    case :gb_trees.next(tx_iterator) do
      {tx_key, tx_value, iterator} when tx_key < boundary ->
        merge_with_tx_write_bounded({tx_key, tx_value}, iterator, storage_list, clear_ranges, acc, boundary)

      _ ->
        {Enum.reverse(storage_list, acc), tx_iterator}
    end
  end

  defp merge_with_tx_write_bounded(
         {tx_key, _tx_value} = tx_kv,
         iterator,
         [{storage_key, _storage_value} = storage_kv | storage_rest] = storage_list,
         clear_ranges,
         acc,
         boundary
       ) do
    cond do
      tx_key < storage_key ->
        merge_ordered_results_bounded(storage_list, iterator, clear_ranges, [tx_kv | acc], boundary)

      tx_key == storage_key ->
        merge_ordered_results_bounded(storage_rest, iterator, clear_ranges, [tx_kv | acc], boundary)

      true ->
        merge_with_tx_write_bounded(tx_kv, iterator, storage_rest, clear_ranges, [storage_kv | acc], boundary)
    end
  end

  defp merge_with_tx_write_bounded({_tx_key, _tx_value} = tx_kv, iterator, [], clear_ranges, acc, boundary),
    do: merge_ordered_results_bounded([], iterator, clear_ranges, [tx_kv | acc], boundary)

  @doc """
  Get the repeatable read value for a key within the transaction.

  Checks both writes and reads, returning the value if the key has been
  accessed in this transaction, or nil if the key is unknown to the transaction.
  This ensures repeatable read semantics - the same key returns the same value
  throughout the transaction.
  """
  @spec repeatable_read(t(), key()) :: value() | :clear | nil
  def repeatable_read(t, key) do
    case :gb_trees.lookup(key, t.writes) do
      {:value, v} -> v
      :none -> Map.get(t.reads, key)
    end
  end

  @doc """
  Record a successful read in the transaction cache.
  """
  @spec record_read(t(), key(), value()) :: t()
  def record_read(t, key, value) do
    %{t | reads: Map.put(t.reads, key, value)}
  end

  @doc """
  Record a not_found read in the transaction cache.
  """
  @spec record_not_found(t(), key()) :: t()
  def record_not_found(t, key) do
    %{t | reads: Map.put(t.reads, key, :clear)}
  end

  @doc """
  Perform a transactional fetch with caching and recording.

  If the key is already cached (read or written), returns the cached value.
  Otherwise, calls the fetch function and records the result appropriately.
  """
  @spec fetch_with_cache(t(), key(), (key() -> {:ok, value()} | {:error, atom()})) ::
          {t(), {:ok, value()} | {:error, atom()}}
  def fetch_with_cache(t, key, fetch_fn) do
    case repeatable_read(t, key) do
      nil ->
        case fetch_fn.(key) do
          {:ok, value} ->
            {record_read(t, key, value), {:ok, value}}

          {:error, :not_found} ->
            {record_not_found(t, key), {:error, :not_found}}

          {:error, reason} ->
            {t, {:error, reason}}
        end

      :clear ->
        {t, {:error, :not_found}}

      value ->
        {t, {:ok, value}}
    end
  end

  @doc """
  Perform a transactional fetch with caching, recording, and external state management.

  If the key is already cached (read or written), returns the cached value with original state.
  Otherwise, calls the fetch function and records the result, returning the updated state.
  """
  @spec fetch(t(), key(), state, (key(), state -> {state, {:ok, value()} | {:error, atom()}})) ::
          {t(), {:ok, value()} | {:error, atom()}, state}
        when state: any()
  def fetch(t, key, state, fetch_fn) do
    case repeatable_read(t, key) do
      nil ->
        case fetch_fn.(key, state) do
          {new_state, {:ok, value}} ->
            {record_read(t, key, value), {:ok, value}, new_state}

          {new_state, {:error, :not_found}} ->
            {record_not_found(t, key), {:error, :not_found}, new_state}

          {new_state, {:error, reason}} ->
            {t, {:error, reason}, new_state}
        end

      :clear ->
        {t, {:error, :not_found}, state}

      value ->
        {t, {:ok, value}, state}
    end
  end

  @spec get(
          t(),
          key(),
          fetch_fn :: (key(), state -> {{:ok, value()} | {:error, reason}, state}),
          state :: any()
        ) :: {t(), {:ok, value()} | {:error, reason}, state}
        when reason: term(), state: term()
  def get(t, k, fetch_fn, state) when is_binary(k) do
    case get_write(t, k) || get_read(t, k) do
      nil -> fetch_and_record(t, k, fetch_fn, state)
      :clear -> {t, {:error, :not_found}, state}
      value -> {t, {:ok, value}, state}
    end
  end

  def set(t, k, v) when is_binary(k) and is_binary(v) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_write(k, v)
    |> record_mutation({:set, k, v})
  end

  def clear(t, k) when is_binary(k) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_clear(k)
    |> record_mutation({:clear, k})
  end

  def clear_range(t, s, e) when is_binary(s) and is_binary(e) do
    t
    |> remove_ops_in_range(s, e)
    |> remove_writes_in_range(s, e)
    |> clear_reads_in_range(s, e)
    |> add_write_range(s, e)
    |> record_mutation({:clear_range, s, e})
  end

  @doc """
  Commits the transaction and returns the transaction map format.

  This is useful for testing and cases where the raw transaction structure
  is needed without binary encoding.
  """
  def commit(t, read_version \\ nil) do
    write_conflicts =
      t.writes
      |> :gb_trees.keys()
      |> Enum.reduce(t.range_writes, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    read_conflicts =
      t.reads
      |> Map.keys()
      |> Enum.reduce(t.range_reads, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    # Enforce read_version/read_conflicts coupling: if no reads, ignore read_version
    read_conflicts_tuple =
      case read_conflicts do
        [] -> {nil, []}
        non_empty when read_version != nil -> {read_version, non_empty}
        _non_empty when read_version == nil -> {nil, []}
      end

    %{
      mutations: Enum.reverse(t.mutations),
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts_tuple
    }
  end

  @doc """
  Commits the transaction and returns binary encoded format.

  This is useful for commit proxy operations and other cases where
  efficient binary format is preferred.
  """
  def commit_binary(t, read_version \\ nil), do: t |> commit(read_version) |> Transaction.encode()

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
    # Get all keys in range and delete them
    keys_to_remove =
      s
      |> :gb_trees.iterator_from(t.writes)
      |> gb_trees_range_keys(e, [])

    new_writes =
      Enum.reduce(keys_to_remove, t.writes, fn k, tree ->
        :gb_trees.delete_any(k, tree)
      end)

    %{t | writes: new_writes}
  end

  # Helper function to collect keys in range from gb_trees iterator
  defp gb_trees_range_keys(iterator, end_key, acc) do
    case :gb_trees.next(iterator) do
      {key, _value, next_iterator} when key < end_key ->
        gb_trees_range_keys(next_iterator, end_key, [key | acc])

      _ ->
        Enum.reverse(acc)
    end
  end

  defp clear_reads_in_range(t, s, e) do
    %{
      t
      | reads:
          Map.new(t.reads, fn
            {k, _} when k >= s and k < e -> {k, :clear}
            kv -> kv
          end)
    }
  end

  defp add_write_range(t, s, e) do
    %{
      t
      | range_writes: add_or_merge(t.range_writes, s, e)
    }
  end

  def add_or_merge([], s, e), do: [{s, e}]
  def add_or_merge([{hs, he} | rest], s, e) when e < hs, do: [{s, e}, {hs, he} | rest]
  def add_or_merge([{hs, he} | rest], s, e) when he < s, do: [{hs, he} | add_or_merge(rest, s, e)]
  def add_or_merge([{hs, he} | rest], s, e), do: add_or_merge(rest, min(hs, s), max(he, e))

  @spec get_write(t(), k :: binary()) :: binary() | :clear | nil
  defp get_write(t, k) do
    case :gb_trees.lookup(k, t.writes) do
      {:value, v} -> v
      :none -> nil
    end
  end

  @spec get_read(t(), k :: binary()) :: binary() | :clear | nil
  defp get_read(t, k), do: Map.get(t.reads, k)

  @spec put_clear(t(), k :: binary()) :: t()
  defp put_clear(t, k), do: %{t | writes: :gb_trees.enter(k, :clear, t.writes)}

  @spec put_write(t(), k :: binary(), v :: binary()) :: t()
  defp put_write(t, k, v), do: %{t | writes: :gb_trees.enter(k, v, t.writes)}

  defp fetch_and_record(t, k, fetch_fn, state) do
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

  # Old 2-argument version removed - use the 5-argument version instead

  # Helper functions needed by the new implementation

  defp filter_cleared_keys(key_value_pairs, clear_ranges) do
    Enum.reject(key_value_pairs, fn {key, _value} ->
      key_cleared_by_ranges?(key, clear_ranges)
    end)
  end

  defp key_cleared_by_ranges?(key, clear_ranges) do
    Enum.any?(clear_ranges, fn {:clear_range, start_range, end_range} ->
      key >= start_range and key < end_range
    end)
  end

  # Helper function to reduce nesting in get_range
end
