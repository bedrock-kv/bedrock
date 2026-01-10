defmodule Bedrock.DataPlane.ShardRouter do
  @moduledoc """
  Routes keys to shards and shards to logs using ceiling search and golden ratio distribution.

  ## Shard Lookup

  Uses an ETS ordered_set table for O(log n) ceiling search. Each entry is `{end_key, tag}`
  where `end_key` is the exclusive upper bound for that shard.

  To find the shard for a key:
  1. Find the first entry where `end_key >= key`
  2. Return that entry's tag

  ## Log Selection

  Uses the golden ratio algorithm for deterministic, well-distributed log selection.
  Given a shard tag, number of logs, and replication factor, returns the indices
  of logs that should store data for that shard.

  The mapping is deterministic and stable as long as the log list order is preserved.
  """

  # Golden ratio constant (2^64 / phi) for good distribution
  @golden 0x9E3779B97F4A7C15

  @doc """
  Returns `m` log indices for shard tag `x` given `n` total logs.

  Uses golden ratio stepping for uniform distribution. The result is deterministic
  for the same inputs.

  ## Parameters

    - `x` - Shard tag (non-negative integer)
    - `n` - Total number of logs
    - `m` - Replication factor (how many logs to return)

  ## Examples

      iex> ShardRouter.get_log_indices(0, 5, 2)
      [4, 0]  # Two distinct indices in range [0, 5)

      iex> ShardRouter.get_log_indices(0, 5, 0)
      []

  """
  @spec get_log_indices(non_neg_integer(), pos_integer(), non_neg_integer()) :: [non_neg_integer()]
  def get_log_indices(_x, _n, 0), do: []

  def get_log_indices(x, n, m) when is_integer(x) and is_integer(n) and is_integer(m) do
    # Start at position determined by tag
    h = rem(x, n)
    do_get_indices(h, n, m, [])
  end

  defp do_get_indices(_h, _n, 0, acc), do: acc

  defp do_get_indices(h, n, remaining, acc) do
    # Find next free slot (not already in acc)
    h = find_free(h, n, acc)
    # Step by golden ratio for next iteration
    next_h = rem(h + @golden, n)
    do_get_indices(next_h, n, remaining - 1, [h | acc])
  end

  defp find_free(h, n, acc) do
    if h in acc do
      find_free(rem(h + 1, n), n, acc)
    else
      h
    end
  end

  @doc """
  Looks up the shard tag for a key using ETS ceiling search.

  The ETS table must be an ordered_set with entries in one of two formats:
  - `{end_key, tag}` - uncached format
  - `{end_key, {tag, log_indices}}` - cached format (after `get_logs_for_key/4` call)

  Shard ranges are `[min, max)` - start inclusive, end exclusive.

  ## Parameters

    - `table` - ETS table reference
    - `key` - The key to look up

  ## Returns

  The shard tag (non-negative integer) that owns the key.

  ## Examples

      # Table has: {"m", 0}, {"\xff", 1}
      # Shard 0 covers ["", "m"), Shard 1 covers ["m", "\xff")
      iex> lookup_shard(table, "apple")
      0
      iex> lookup_shard(table, "m")
      1  # "m" is the START of shard 1, not end of shard 0
      iex> lookup_shard(table, "zebra")
      1

  """
  @spec lookup_shard(:ets.table(), binary()) :: non_neg_integer()
  def lookup_shard(table, key) when is_binary(key) do
    # Find first end_key > key (strictly greater)
    # With [min, max) ranges, end_key is exclusive, so we want end_key > key
    case :ets.next(table, key) do
      :"$end_of_table" ->
        # Key is beyond all boundaries - fall back to last entry
        lookup_last(table)

      next_key ->
        # Found the shard whose end_key > key, meaning key is in this shard's range
        extract_tag(:ets.lookup_element(table, next_key, 2))
    end
  end

  defp lookup_last(table) do
    case :ets.last(table) do
      :"$end_of_table" -> raise "Empty shard_keys table"
      last_key -> extract_tag(:ets.lookup_element(table, last_key, 2))
    end
  end

  # Extract tag from either cached or uncached format
  # Tags can be integers or strings depending on test setup
  defp extract_tag({tag, _log_indices}), do: tag
  defp extract_tag(tag), do: tag

  @doc """
  Looks up all shard tags that overlap with a key range.

  Used for range mutations (clear_range) that may span multiple shards.
  Returns a list of tags for all shards that intersect the range [start_key, end_key).

  ## Parameters

    - `table` - ETS table reference with `{end_key, tag}` entries
    - `start_key` - Start of the range (inclusive)
    - `end_key` - End of the range (exclusive)

  ## Returns

  List of shard tags that the range intersects with.

  ## Examples

      # Table has: {"d", 0}, {"h", 1}, {"m", 2}, {"\\xff", 3}
      iex> lookup_shards_for_range(table, "a", "c")
      [0]
      iex> lookup_shards_for_range(table, "c", "f")
      [0, 1]

  """
  @spec lookup_shards_for_range(:ets.table(), binary(), binary()) :: [non_neg_integer()]
  def lookup_shards_for_range(table, start_key, end_key) when is_binary(start_key) and is_binary(end_key) do
    # Find the first shard that contains start_key
    first_tag = lookup_shard(table, start_key)

    # If start_key == end_key (empty range), just return the start shard
    if start_key >= end_key do
      [first_tag]
    else
      # Collect all shards from first_tag until we find one that contains end_key
      collect_shards_in_range(table, start_key, end_key, first_tag)
    end
  end

  # Collect all shard tags that overlap with the range [start_key, end_key)
  @spec collect_shards_in_range(:ets.table(), binary(), binary(), non_neg_integer()) ::
          [non_neg_integer()]
  defp collect_shards_in_range(table, start_key, end_key, _first_tag) do
    # Get all entries from the table
    entries = :ets.tab2list(table)

    # Sort by end_key
    sorted = Enum.sort_by(entries, fn {ek, _tag_or_cached} -> ek end)

    # Find shards that overlap with [start_key, end_key)
    # With [min, max) semantics:
    # - A shard with end_key E covers [prev_end, E)
    # - Shard overlaps [start_key, end_key) if: shard_end_key > start_key
    sorted
    |> Enum.filter(fn {shard_end_key, _tag_or_cached} ->
      # Shard must extend past start_key (strictly greater because end is exclusive)
      shard_end_key > start_key
    end)
    |> Enum.reduce_while([], fn {shard_end_key, tag_or_cached}, acc ->
      tag = extract_tag(tag_or_cached)
      acc = [tag | acc]

      # If this shard's end_key >= end_key, we've covered the whole range
      if shard_end_key >= end_key do
        {:halt, acc}
      else
        {:cont, acc}
      end
    end)
    |> Enum.reverse()
  end

  @doc """
  Routes a key to its logs using shard lookup and golden ratio log selection.

  Uses lazy caching: first call computes log_indices and caches in ETS,
  subsequent calls use the cached value. The ETS entry format changes from
  `{end_key, tag}` to `{end_key, {tag, log_indices}}` after first access.

  ## Parameters

    - `shard_table` - ETS table with `{end_key, tag}` or `{end_key, {tag, log_indices}}` entries
    - `key` - The key to route
    - `log_map` - Map from log index to log_id (`%{0 => "log-a", 1 => "log-b", ...}`)
    - `replication_factor` - How many logs to return

  ## Returns

  List of log_ids that should store data for this key.

  """
  @spec get_logs_for_key(:ets.table(), binary(), %{non_neg_integer() => binary()}, non_neg_integer()) ::
          [binary()]
  def get_logs_for_key(shard_table, key, log_map, replication_factor) do
    n = map_size(log_map)
    end_key = find_end_key(shard_table, key)

    log_indices =
      case :ets.lookup(shard_table, end_key) do
        [{_, {_tag, cached_indices}}] ->
          # Already cached - use it
          cached_indices

        [{_, tag}] when is_integer(tag) ->
          # Not cached - compute, cache, return
          indices = get_log_indices(tag, n, replication_factor)
          :ets.insert(shard_table, {end_key, {tag, indices}})
          indices
      end

    Enum.map(log_indices, &Map.fetch!(log_map, &1))
  end

  # Find the end_key for the shard containing key
  defp find_end_key(table, key) do
    case :ets.next(table, key) do
      :"$end_of_table" ->
        case :ets.last(table) do
          :"$end_of_table" -> raise "Empty shard_keys table"
          last_key -> last_key
        end

      next_key ->
        next_key
    end
  end
end
