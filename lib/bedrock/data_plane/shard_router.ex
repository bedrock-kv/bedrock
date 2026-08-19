defmodule Bedrock.DataPlane.ShardRouter do
  @moduledoc """
  Routes keys to shards and shards to logs using ceiling search and golden ratio distribution.

  ## Shard Lookup

  Uses an ETS ordered_set table for O(log n) ceiling search. Each entry is
  `{end_key, tag, version}` where `end_key` is the exclusive upper bound for
  that shard and `version` is the commit version that wrote the row.

  A cleared boundary is kept as a `{end_key, :deleted, version}` tombstone
  rather than removed: the shard table is written by concurrent, unordered
  writers (finalization tasks and the commit proxy server), and the per-row
  version is what lets a late write of an older version lose. Every
  navigation step here treats tombstones as absent.

  To find the shard for a key:
  1. Find the first live entry where `end_key > key`
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

  @doc ~S"""
  Looks up the shard tag for a key using ETS ceiling search.

  The ETS table must be an ordered_set with `{end_key, tag, version}` entries;
  tombstoned rows (`tag == :deleted`) are skipped.

  Shard ranges are `[min, max)` - start inclusive, end exclusive.

  ## Parameters

    - `table` - ETS table reference
    - `key` - The key to look up

  ## Returns

  The shard tag (non-negative integer) that owns the key.

  ## Examples

      # Table has: {"m", 0, v}, {"\xff", 1, v}
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
    # Find first live end_key > key (strictly greater)
    # With [min, max) ranges, end_key is exclusive, so we want end_key > key
    case next_live(table, key) do
      :none ->
        # Key is beyond all live boundaries - fall back to the last live entry
        case last_live(table) do
          :none -> raise_no_live_boundaries(table)
          {_end_key, tag} -> tag
        end

      {_end_key, tag} ->
        tag
    end
  end

  # An all-tombstones table is distinct from an empty one: it is transiently
  # reachable while a concurrent writer is between a rewrite's clear_range
  # and its sets, and the crash lands there in the routing batch task.
  defp raise_no_live_boundaries(table) do
    if :ets.info(table, :size) == 0 do
      raise "Empty shard_keys table"
    else
      raise "No live shard boundaries in shard_keys table (all tombstoned)"
    end
  end

  @doc ~S"""
  Returns shards overlapping a key range, with their boundaries.

  Returns `{tag, shard_start, shard_end}` tuples to enable clamping range
  mutations to shard boundaries. Tombstoned boundaries are treated as absent:
  the shard structure is defined by live rows only.

  ## Parameters

    - `table` - ETS table reference with `{end_key, tag, version}` entries
    - `start_key` - Start of the range (inclusive)
    - `end_key` - End of the range (exclusive)

  ## Returns

  List of `{tag, shard_start, shard_end}` tuples for all shards that
  overlap [start_key, end_key). Returns `[]` when the range lies entirely
  beyond shard coverage (start_key >= every live shard's exclusive end_key).

  ## Examples

      # Table has: {"d", 0, v}, {"h", 1, v}, {"m", 2, v}, {"\xff", 3, v}
      # Shards: 0 = ["", "d"), 1 = ["d", "h"), 2 = ["h", "m"), 3 = ["m", "\xff")
      iex> lookup_shards_with_ranges(table, "a", "c")
      [{0, "", "d"}]
      iex> lookup_shards_with_ranges(table, "c", "j")
      [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}]

  """
  @spec lookup_shards_with_ranges(:ets.table(), binary(), binary()) ::
          [{non_neg_integer(), binary(), binary()}]
  def lookup_shards_with_ranges(table, start_key, end_key) when is_binary(start_key) and is_binary(end_key) do
    case next_live(table, start_key) do
      :none ->
        # start_key is at or beyond every live shard's exclusive upper bound:
        # the range intersects no shard.
        []

      {first_end, first_tag} ->
        # The first shard starts at the previous live boundary, or "" if none.
        first_start =
          case prev_live(table, first_end) do
            :none -> ""
            {prev_end, _tag} -> prev_end
          end

        collect_shards_with_ranges(table, first_end, first_tag, end_key, first_start, [])
    end
  end

  defp collect_shards_with_ranges(table, shard_end, tag, end_key, shard_start, acc) do
    acc = [{tag, shard_start, shard_end} | acc]

    if shard_end >= end_key do
      Enum.reverse(acc)
    else
      case next_live(table, shard_end) do
        :none -> Enum.reverse(acc)
        # The current live end is the next shard's start.
        {next_end, next_tag} -> collect_shards_with_ranges(table, next_end, next_tag, end_key, shard_end, acc)
      end
    end
  end

  # Live-row navigation: step over tombstones in either direction.

  defp next_live(table, key) do
    case :ets.next(table, key) do
      :"$end_of_table" -> :none
      end_key -> live_or_continue(table, end_key, &next_live/2)
    end
  end

  defp prev_live(table, key) do
    case :ets.prev(table, key) do
      :"$end_of_table" -> :none
      end_key -> live_or_continue(table, end_key, &prev_live/2)
    end
  end

  defp last_live(table) do
    case :ets.last(table) do
      :"$end_of_table" -> :none
      end_key -> live_or_continue(table, end_key, &prev_live/2)
    end
  end

  defp live_or_continue(table, end_key, continue) do
    case :ets.lookup(table, end_key) do
      [{^end_key, :deleted, _version}] -> continue.(table, end_key)
      [{^end_key, tag, _version}] -> {end_key, tag}
      # The row was replaced-in-flight or removed; keep walking.
      [] -> continue.(table, end_key)
    end
  end
end
