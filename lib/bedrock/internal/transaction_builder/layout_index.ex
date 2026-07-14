defmodule Bedrock.Internal.TransactionBuilder.LayoutIndex do
  @moduledoc """
  Pre-computed index for efficient Transaction System Layout lookups.

  This module builds a gb_tree index from the static TSL configuration by segmenting
  the keyspace into non-overlapping ranges. Each segment shows exactly which PIDs
  are responsible for that portion of the keyspace, enabling O(log n) lookups
  instead of O(n) linear scans through all storage teams.

  ## Segmented Keyspace Example

  Given overlapping storage teams:
  - a-f → [pid1]
  - d-m → [pid2]
  - h-p → [pid3]

  The index creates non-overlapping segments:
  - {a, d} → [pid1]
  - {d, f} → [pid1, pid2]
  - {f, h} → [pid2]
  - {h, m} → [pid2, pid3]
  - {m, p} → [pid3]
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  defstruct [:tree]

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(binary(), {binary(), [pid()]})
        }

  @doc """
  Builds a segmented index from a Transaction System Layout.
  """
  @spec build_index(TransactionSystemLayout.t()) :: t()
  def build_index(transaction_system_layout) do
    tree =
      transaction_system_layout
      |> collect_active_ranges()
      |> create_segments_with_pids()
      |> build_tree_from_segments()

    %__MODULE__{tree: tree}
  end

  @doc """
  Looks up storage servers for a single key using recursive tree traversal.

  Returns a {key_range, [pid]} tuple for the segment containing the key.
  The end key will be the binary sentinel `<<0xFF, 0xFF>>` for unbounded ranges.
  Raises if no segment is found. This is an O(log n) operation.
  """
  @spec lookup_key!(t(), binary()) :: {{binary(), binary()}, [pid()]}
  def lookup_key!(%__MODULE__{tree: tree}, key) do
    case segment_for_key(tree, key) do
      {:ok, {start, end_key}, pids} ->
        {{start, end_key}, pids}

      :not_found ->
        raise "No segment found containing key: #{inspect(key)}"
    end
  end

  @doc """
  Looks up storage servers for a key range.

  Returns a list of {key_range, [pid]} tuples for all segments that overlap
  with the specified range. Each segment shows exactly which PIDs cover
  that portion of the keyspace. End keys will be the binary sentinel `<<0xFF, 0xFF>>`
  for unbounded ranges.
  """
  @spec lookup_range(t(), binary(), binary()) :: [{{binary(), binary()}, [pid()]}]
  def lookup_range(%__MODULE__{tree: tree}, start_key, end_key) do
    tree
    |> :gb_trees.iterator()
    |> collect_overlapping_segments(start_key, end_key, [])
  end

  @doc """
  Finds the next segment after the one containing the given key.

  This is useful for cross-shard KeySelector resolution when we need to
  continue processing in the next shard.

  Contract: the adjacent segment is the one starting exactly at the current
  segment's end key. The index has total coverage (every shard is a segment,
  possibly with `[]` pids), so an adjacent segment always exists until the
  end of the keyspace — uncovered segments are returned, never skipped.
  """
  @spec get_next_segment(t(), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :end_of_keyspace
  def get_next_segment(%__MODULE__{tree: tree}, key) do
    case segment_for_key(tree, key) do
      {:ok, {_current_start, current_end}, _current_pids} ->
        find_segment_starting_at(tree, current_end)

      :not_found ->
        :end_of_keyspace
    end
  end

  @doc """
  Finds the previous segment before the one containing the given key.

  This is useful for cross-shard KeySelector resolution when we need to
  continue processing in the previous shard.

  Contract: mirrors `get_next_segment/2` — the adjacent segment is the one
  ending exactly at the current segment's start key, and uncovered segments
  (`[]` pids) are returned, never skipped.
  """
  @spec get_previous_segment(t(), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :start_of_keyspace
  def get_previous_segment(%__MODULE__{tree: tree}, key) do
    case segment_for_key(tree, key) do
      {:ok, {current_start, _current_end}, _current_pids} ->
        find_segment_ending_at(tree, current_start)

      :not_found ->
        :start_of_keyspace
    end
  end

  # Private implementation functions

  # Build ranges from shard_layout and available materializers.
  #
  # Total coverage: every shard in shard_layout becomes a range, even when no
  # materializer is known for its tag (pids == []). shard_layout partitions the
  # keyspace, so the resulting index has no gaps — an uncovered shard is a
  # segment with [] pids, which lookup_key! surfaces to callers so they can
  # fail loudly (layout_lookup_failed) while the system heals coverage
  # (bedrock-q67 Phase A: placeholder materializer / Distributor).
  @spec collect_active_ranges(TransactionSystemLayout.t()) ::
          [{binary(), binary(), [pid()]}]
  defp collect_active_ranges(transaction_system_layout) do
    shard_layout = Map.get(transaction_system_layout, :shard_layout) || %{}
    metadata_materializer = Map.get(transaction_system_layout, :metadata_materializer)
    shard_materializers = Map.get(transaction_system_layout, :shard_materializers) || %{}

    # Convert shard_layout to ranges with materializer servers
    shard_layout
    |> Enum.map(fn {end_key, {tag, start_key}} ->
      read_server = get_materializer_for_shard(tag, metadata_materializer, shard_materializers)
      {start_key, end_key, read_server}
    end)
    |> Enum.sort_by(fn {start_key, _end, _pids} -> start_key end)
  end

  # Get materializer for a shard tag
  defp get_materializer_for_shard(0, metadata_materializer, _shard_materializers) when is_pid(metadata_materializer) do
    # System shard (tag 0) uses metadata_materializer
    [metadata_materializer]
  end

  defp get_materializer_for_shard(0, _non_pid_metadata_materializer, _shard_materializers) do
    # System shard with no live metadata materializer: still indexed, uncovered
    []
  end

  defp get_materializer_for_shard(tag, _metadata_materializer, shard_materializers) do
    # Other shards use their assigned materializer from shard_materializers map
    case Map.get(shard_materializers, tag) do
      pid when is_pid(pid) -> [pid]
      _ -> []
    end
  end

  @spec create_segments_with_pids([{binary(), binary(), [pid()]}]) ::
          [{binary(), {binary(), [pid()]}}]
  defp create_segments_with_pids(ranges) do
    boundaries =
      ranges
      |> Enum.flat_map(fn {start_key, end_key, _pids} -> [start_key, end_key] end)
      |> Enum.sort()
      |> Enum.dedup()

    boundaries
    |> Enum.chunk_every(2, 1, :discard)
    # Defensive only: dedup above makes boundaries strictly increasing, so no
    # chunk can be zero-width. Kept as a second guard (bedrock-tn5) because a
    # zero-width segment would put duplicate keys in the orddict, corrupting
    # the gb_tree.
    |> Enum.reject(fn [segment_start, segment_end] -> segment_start == segment_end end)
    |> Enum.map(fn [segment_start, segment_end] ->
      covering_pids =
        ranges
        |> Enum.filter(fn {range_start, range_end, _pids} ->
          range_start <= segment_start and segment_end <= range_end
        end)
        |> Enum.flat_map(fn {_, _, pids} -> pids end)
        |> Enum.uniq()

      {segment_end, {segment_start, covering_pids}}
    end)
  end

  @spec build_tree_from_segments([{binary(), {binary(), [pid()]}}]) ::
          :gb_trees.tree(binary(), {binary(), [pid()]})
  defp build_tree_from_segments(orddict), do: :gb_trees.from_orddict(orddict)

  @spec segment_for_key(:gb_trees.tree(binary(), {binary(), [pid()]}), binary()) ::
          {:ok, {binary(), binary()}, [pid()]} | :not_found
  defp segment_for_key({0, _}, _key), do: :not_found
  defp segment_for_key({_, tree_node}, key), do: node_for_key(tree_node, key)
  defp segment_for_key(_, _key), do: :not_found

  defp node_for_key({tree_end_key, {segment_start, pids}, _left, _right}, key)
       when key >= segment_start and (key < tree_end_key or (key == tree_end_key and tree_end_key == <<0xFF, 0xFF>>)),
       do: {:ok, {segment_start, tree_end_key}, pids}

  defp node_for_key({tree_end_key, _, left, _right}, key) when key < tree_end_key, do: node_for_key(left, key)
  defp node_for_key({_tree_end_key, _, _left, right}, key), do: node_for_key(right, key)
  defp node_for_key(nil, _key), do: :not_found

  defp collect_overlapping_segments(iterator, start_key, end_key, acc) do
    case :gb_trees.next(iterator) do
      {tree_end_key, {segment_start, pids}, next_iter} ->
        if segment_start < end_key and tree_end_key > start_key do
          segment_tuple = {{segment_start, tree_end_key}, pids}
          collect_overlapping_segments(next_iter, start_key, end_key, [segment_tuple | acc])
        else
          collect_overlapping_segments(next_iter, start_key, end_key, acc)
        end

      :none ->
        Enum.reverse(acc)
    end
  end

  @spec find_segment_starting_at(:gb_trees.tree(binary(), {binary(), [pid()]}), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :end_of_keyspace
  defp find_segment_starting_at(tree, boundary_key) do
    iterator = :gb_trees.iterator_from(boundary_key, tree)
    find_first_segment_at_boundary(iterator, boundary_key)
  end

  @spec find_segment_ending_at(:gb_trees.tree(binary(), {binary(), [pid()]}), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :start_of_keyspace
  defp find_segment_ending_at(tree, boundary_key) do
    # The tree is keyed by segment end key, so the previous segment — the one
    # ending exactly at the current segment's start — is the first entry at or
    # after boundary_key that isn't the current segment itself. Exact-boundary
    # semantics: see find_first_segment_at_boundary.
    boundary_key
    |> :gb_trees.iterator_from(tree)
    |> :gb_trees.next()
    |> case do
      {^boundary_key, {segment_start, pids}, _next_iter} -> {:ok, {{segment_start, boundary_key}, pids}}
      _ -> :start_of_keyspace
    end
  end

  # Find the segment that starts exactly at the boundary key.
  #
  # Exact-boundary semantics (bedrock-q67.1, revising the bedrock-dwu gap
  # walk): the index has total coverage — every shard in shard_layout is a
  # segment, including shards with no known materializer (pids == []) — so
  # "gaps" cannot exist between segments. Walking forward past the boundary
  # would skip a shard, and skipping an uncovered shard during cross-shard
  # KeySelector resolution would silently drop live keys. Uncovered segments
  # must be returned so callers fail loudly (layout_lookup_failed) while the
  # system heals coverage (bedrock-q67 Phase A placeholder/Distributor) —
  # coverage is never "skipped" by the client.
  defp find_first_segment_at_boundary(iterator, boundary_key) do
    case :gb_trees.next(iterator) do
      {tree_end_key, {segment_start, pids}, next_iter} ->
        cond do
          segment_start == boundary_key -> {:ok, {{segment_start, tree_end_key}, pids}}
          # The iterator starts from the current segment (its end key equals
          # boundary_key); step past it to reach the adjacent segment.
          segment_start < boundary_key -> find_first_segment_at_boundary(next_iter, boundary_key)
          true -> :end_of_keyspace
        end

      :none ->
        :end_of_keyspace
    end
  end
end
