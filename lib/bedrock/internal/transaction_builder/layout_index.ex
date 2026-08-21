defmodule Bedrock.Internal.TransactionBuilder.LayoutIndex do
  @moduledoc """
  A partial, coalescing shard-lookup index: a gb_tree of `end_key =>
  {start_key, value}` accumulated one covering entry at a time from
  proxy-served by-key routing fetches (FDB's locationCache shape - built
  from `GetKeyServerLocations` answers, never a bulk dump).

  Entries are exact shard ranges keyed by exclusive `end_key`, so within
  one routing epoch they never overlap and re-inserting a shard is
  idempotent. Cross-epoch mixing is precluded by coarse invalidation:
  the whole index is dropped on a wiring push or a routing-shaped read
  failure, never patched.

  Lookups are a single O(log n) ceiling search; a key no fetched entry
  covers returns `:not_cached`, which the owner resolves through its
  routing fetch.
  """

  defstruct [:tree]

  @typedoc "A callable materializer ref: a pid or a `{otp_name, node}` tuple."
  @type server_ref :: pid() | {atom(), node()}

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(binary(), {binary(), term()})
        }

  @doc "An empty index; entries accumulate per fetched covering entry."
  @spec new() :: t()
  def new, do: %__MODULE__{tree: :gb_trees.empty()}

  @doc """
  Inserts one covering entry: the shard `[start_key, end_key)` and the
  value cached for it (callable refs in the transaction builder, raw
  keyspace refs in the Link's node-wide cache).
  """
  @spec insert(t(), Bedrock.key(), Bedrock.key(), term()) :: t()
  def insert(%__MODULE__{tree: tree} = t, start_key, end_key, value) do
    %{t | tree: :gb_trees.enter(end_key, {start_key, value}, tree)}
  end

  @doc """
  Looks up the cached covering entry for a key: `{:ok, {key_range, value}}`
  or `:not_cached` when no fetched entry covers it. O(log n).
  """
  @spec lookup_key(t(), binary()) :: {:ok, {Bedrock.key_range(), term()}} | :not_cached
  def lookup_key(%__MODULE__{tree: tree}, key) do
    case segment_for_key(tree, key) do
      {:ok, key_range, value} -> {:ok, {key_range, value}}
      :not_found -> :not_cached
    end
  end

  # Private implementation functions

  @spec segment_for_key(:gb_trees.tree(binary(), {binary(), term()}), binary()) ::
          {:ok, {binary(), binary()}, term()} | :not_found
  defp segment_for_key({0, _}, _key), do: :not_found
  defp segment_for_key({_, tree_node}, key), do: node_for_key(tree_node, key)
  defp segment_for_key(_, _key), do: :not_found

  defp node_for_key({tree_end_key, {segment_start, value}, _left, _right}, key)
       when key >= segment_start and (key < tree_end_key or (key == tree_end_key and tree_end_key == <<0xFF, 0xFF>>)),
       do: {:ok, {segment_start, tree_end_key}, value}

  defp node_for_key({tree_end_key, _, left, _right}, key) when key < tree_end_key, do: node_for_key(left, key)
  defp node_for_key({_tree_end_key, _, _left, right}, key), do: node_for_key(right, key)
  defp node_for_key(nil, _key), do: :not_found
end
