defmodule Bedrock.DataPlane.ShardRouter do
  @moduledoc """
  Routes keys to shards and shards to logs using ceiling search and golden ratio distribution.

  ## Shard Lookup

  Operates on an immutable `:gb_trees` shard map of `end_key => {tag, start_key}`
  entries, where `end_key` is the shard's exclusive upper bound (see
  `Bedrock.DataPlane.CommitProxy.RoutingData`). Because the map is a plain
  immutable value, lookups always see one self-consistent snapshot - there is
  no shared table for concurrent writers to race on.

  To find the shard for a key:
  1. Find the first entry where `end_key > key`
  2. Return that entry's tag

  ## Log Selection

  Uses the golden ratio algorithm for deterministic, well-distributed log selection.
  Given a shard tag, number of logs, and replication factor, returns the indices
  of logs that should store data for that shard.

  The mapping is deterministic and stable as long as the log list order is preserved.
  """

  alias Bedrock.DataPlane.CommitProxy.RoutingData

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
  Looks up the shard tag for a key using ceiling search.

  Shard ranges are `[min, max)` - start inclusive, end exclusive.

  ## Parameters

    - `shards` - shard map (`end_key => {tag, start_key}` gb_tree)
    - `key` - The key to look up

  ## Returns

  The shard tag (non-negative integer) that owns the key.

  ## Examples

      # Shards: 0 covers ["", "m"), 1 covers ["m", "\xff")
      iex> lookup_shard(shards, "apple")
      0
      iex> lookup_shard(shards, "m")
      1  # "m" is the START of shard 1, not end of shard 0
      iex> lookup_shard(shards, "zebra")
      1

  """
  @spec lookup_shard(RoutingData.shard_tree(), binary()) :: non_neg_integer()
  def lookup_shard(shards, key) when is_binary(key) do
    # Find first end_key > key (strictly greater)
    # With [min, max) ranges, end_key is exclusive, so we want end_key > key
    case ceiling_entry(shards, key) do
      :none ->
        # A key at or past every boundary belongs to no shard. Commit ingress
        # bounds every mutation below the last boundary, so this firing means
        # the map and the keyspace have diverged - the historical fallback
        # (route into the last shard) turned exactly that into a silent
        # misroute. Fail the batch into director recovery instead.
        if :gb_trees.is_empty(shards) do
          raise "Empty shard map"
        else
          raise "Key beyond all shard boundaries: #{inspect(key)}"
        end

      {_end_key, {tag, _start_key}} ->
        tag
    end
  end

  @doc ~S"""
  Returns shards overlapping a key range, with their boundaries.

  Returns `{tag, shard_start, shard_end}` tuples to enable clamping range
  mutations to shard boundaries.

  ## Parameters

    - `shards` - shard map (`end_key => {tag, start_key}` gb_tree)
    - `start_key` - Start of the range (inclusive)
    - `end_key` - End of the range (exclusive)

  ## Returns

  List of `{tag, shard_start, shard_end}` tuples for all shards that
  overlap [start_key, end_key). Returns `[]` when the range lies entirely
  beyond shard coverage (start_key >= every shard's exclusive end_key).

  ## Examples

      # Shards: 0 = ["", "d"), 1 = ["d", "h"), 2 = ["h", "m"), 3 = ["m", "\xff")
      iex> lookup_shards_with_ranges(shards, "a", "c")
      [{0, "", "d"}]
      iex> lookup_shards_with_ranges(shards, "c", "j")
      [{0, "", "d"}, {1, "d", "h"}, {2, "h", "m"}]

  """
  @spec lookup_shards_with_ranges(RoutingData.shard_tree(), binary(), binary()) ::
          [{non_neg_integer(), binary(), binary()}]
  def lookup_shards_with_ranges(shards, start_key, end_key) when is_binary(start_key) and is_binary(end_key) do
    start_key
    |> :gb_trees.iterator_from(shards)
    |> collect_overlapping(start_key, end_key, [])
  end

  defp collect_overlapping(iter, start_key, end_key, acc) do
    case :gb_trees.next(iter) do
      :none ->
        Enum.reverse(acc)

      # The iterator starts at keys >= start_key; a shard ending exactly AT
      # start_key does not overlap (its end is exclusive) - skip it.
      {shard_end, _value, next_iter} when shard_end <= start_key ->
        collect_overlapping(next_iter, start_key, end_key, acc)

      {shard_end, {tag, shard_start}, next_iter} ->
        acc = [{tag, shard_start, shard_end} | acc]

        if shard_end >= end_key do
          Enum.reverse(acc)
        else
          collect_overlapping(next_iter, start_key, end_key, acc)
        end
    end
  end

  # First entry with end_key strictly greater than key. iterator_from yields
  # keys >= key, so at most the first entry needs skipping.
  defp ceiling_entry(shards, key) do
    iter = :gb_trees.iterator_from(key, shards)

    case :gb_trees.next(iter) do
      :none -> :none
      {end_key, value, _next_iter} when end_key > key -> {end_key, value}
      {_equal_key, _value, next_iter} -> next_entry(next_iter)
    end
  end

  defp next_entry(iter) do
    case :gb_trees.next(iter) do
      :none -> :none
      {end_key, value, _next_iter} -> {end_key, value}
    end
  end
end
