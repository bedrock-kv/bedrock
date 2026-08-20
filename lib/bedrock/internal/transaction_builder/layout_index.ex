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
end
