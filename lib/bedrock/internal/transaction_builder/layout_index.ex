defmodule Bedrock.Internal.TransactionBuilder.LayoutIndex do
  @moduledoc """
  The client's shard-lookup index: a gb_tree of `end_key => {start_key,
  [materializer ref]}` built from the proxy-served routing projection.

  Shards are non-overlapping by construction, so lookups are a single
  O(log n) ceiling search. Shards whose tag has no materializer are
  absent - a key landing in the gap fails the lookup, which the client
  retry loop converts into an invalidate-and-refetch.
  """

  defstruct [:tree]

  @typedoc "A callable materializer ref: a pid or a `{otp_name, node}` tuple."
  @type server_ref :: pid() | {atom(), node()}

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(binary(), {binary(), [server_ref()]})
        }

  @doc """
  Builds a segmented index from shard boundaries and materializer refs.

  `shard_layout` maps each shard's exclusive `end_key` to `{tag, start_key}`;
  `materializers` maps tags to callable refs. Shards whose tag has no
  materializer are dropped - a key landing in one fails the lookup, which
  the client retry loop converts into an invalidate-and-refetch.
  """
  @spec build_index(
          %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}},
          %{Bedrock.range_tag() => server_ref()}
        ) :: t()
  def build_index(shard_layout, materializers) when is_map(shard_layout) and is_map(materializers) do
    # Shards are non-overlapping by construction (the layout is keyed by
    # exclusive end_key), so the index is a direct end_key => {start_key,
    # [ref]} tree - no segmenting pass. Shards without a materializer are
    # dropped: a key landing in the gap fails the lookup, which the client
    # retry loop converts into an invalidate-and-refetch.
    tree =
      shard_layout
      |> Enum.flat_map(fn {end_key, {tag, start_key}} ->
        case Map.get(materializers, tag) do
          nil -> []
          ref -> [{end_key, {start_key, [ref]}}]
        end
      end)
      |> Enum.sort()
      |> :gb_trees.from_orddict()

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
  """
  @spec get_previous_segment(t(), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :start_of_keyspace
  def get_previous_segment(%__MODULE__{tree: tree}, key) do
    case segment_for_key(tree, key) do
      {:ok, {current_start, _current_end}, _current_pids} ->
        find_segment_ending_before(tree, current_start)

      :not_found ->
        :start_of_keyspace
    end
  end

  # Private implementation functions

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

  @spec find_segment_ending_before(:gb_trees.tree(binary(), {binary(), [pid()]}), binary()) ::
          {:ok, {{binary(), binary()}, [pid()]}} | :start_of_keyspace
  defp find_segment_ending_before(tree, boundary_key) do
    iterator = :gb_trees.iterator(tree)
    find_last_segment_before_boundary(iterator, boundary_key, :start_of_keyspace)
  end

  # Find the first segment that starts at the boundary key
  defp find_first_segment_at_boundary(iterator, boundary_key) do
    case :gb_trees.next(iterator) do
      {tree_end_key, {segment_start, pids}, next_iter} ->
        if segment_start == boundary_key do
          {:ok, {{segment_start, tree_end_key}, pids}}
        else
          find_first_segment_at_boundary(next_iter, boundary_key)
        end

      :none ->
        :end_of_keyspace
    end
  end

  # Find the last segment that ends at or before the boundary key
  defp find_last_segment_before_boundary(iterator, boundary_key, current_best) do
    case :gb_trees.next(iterator) do
      {tree_end_key, {segment_start, pids}, next_iter} ->
        if tree_end_key <= boundary_key do
          new_result = {:ok, {{segment_start, tree_end_key}, pids}}
          find_last_segment_before_boundary(next_iter, boundary_key, new_result)
        else
          find_last_segment_before_boundary(next_iter, boundary_key, current_best)
        end

      :none ->
        current_best
    end
  end
end
