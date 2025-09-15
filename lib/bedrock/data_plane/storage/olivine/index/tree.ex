defmodule Bedrock.DataPlane.Storage.Olivine.Index.Tree do
  @moduledoc """
  Tree operations for the Olivine storage driver.

  ## Structure

  The tree is a gb_trees structure with:
  - **Key**: Page's `last_key` (rightmost key in page)
  - **Value**: `{page_id, first_key}` tuple
  - **Ordering**: Sorted by `last_key` for efficient range queries

  ## Key Operations

  - `page_for_key/2`: Find page containing a specific key
  - `page_for_insertion/2`: Find correct page for inserting a new key
  - `page_ids_in_range/3`: Get pages intersecting a key range
  - `add_page_to_tree/2`: Add page to tree structure
  - `remove_page_from_tree/2`: Remove page from tree structure

  ## Insertion Logic

  For `page_for_insertion/2`:
  1. Try to find existing page containing the key
  2. If not found, find page whose `first_key` is smallest > insertion key
  3. If no such page exists, use rightmost page
  4. This maintains sorted traversal order while handling gaps between pages
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @type t :: :gb_trees.tree()

  @type page_id :: Page.id()
  @type page :: Page.t()

  @doc """
  Builds a page tree from a page map by adding each page to an empty tree.
  """
  @spec from_page_map(page_map :: map()) :: t()
  def from_page_map(page_map) do
    Enum.reduce(page_map, :gb_trees.empty(), fn {_page_id, {page, _next_id}}, tree_acc ->
      add_page_to_tree(tree_acc, page)
    end)
  end

  @doc """
  Finds the page that contains a specific key using the interval tree.
  Returns the page ID if found, or nil if no page contains the key.
  Uses direct pattern matching on GB-trees internal structure.

  Tree structure: key = last_key, value = {page_id, first_key}
  """
  @spec page_for_key(t(), Bedrock.key()) :: page_id() | nil
  def page_for_key({size, tree_node}, key) when size > 0, do: find_page_for_key(tree_node, key)
  def page_for_key(_, _key), do: nil

  defp find_page_for_key({last_key, {page_id, first_key}, _l, _r}, key) when first_key <= key and key <= last_key,
    do: page_id

  defp find_page_for_key({last_key, {_page_id, _first_key}, l, _r}, key) when key < last_key,
    do: find_page_for_key(l, key)

  defp find_page_for_key({_last_key, {_page_id, _first_key}, _l, r}, key), do: find_page_for_key(r, key)
  defp find_page_for_key(nil, _key), do: nil

  @doc """
  Finds the best page for inserting a key. First tries to find a page containing the key,
  then finds the page where inserting the key would maintain sorted order during tree traversal.
  """
  @spec page_for_insertion(t(), Bedrock.key()) :: page_id()
  def page_for_insertion(tree, key) do
    case page_for_key(tree, key) do
      page_id when not is_nil(page_id) -> page_id
      nil -> find_insertion_page(tree, key)
    end
  end

  defp find_insertion_page(tree, key) do
    if :gb_trees.is_empty(tree) do
      0
    else
      sorted_page_entries = :gb_trees.to_list(tree)
      find_page_with_smallest_first_key_greater_than(sorted_page_entries, key)
    end
  end

  defp find_page_with_smallest_first_key_greater_than(sorted_page_entries, key) do
    case Enum.find(sorted_page_entries, fn {_last_key, {_page_id, first_key}} -> first_key > key end) do
      {_last_key, {page_id, _first_key}} ->
        page_id

      nil ->
        {_last_key, {rightmost_page_id, _first_key}} = List.last(sorted_page_entries)
        rightmost_page_id
    end
  end

  @doc """
  Updates the interval tree by adding a new page range.
  """
  @spec add_page_to_tree(t(), page()) :: t()
  def add_page_to_tree(tree, page) do
    case {Page.left_key(page), Page.right_key(page)} do
      {nil, nil} -> tree
      {first_key, last_key} -> :gb_trees.enter(last_key, {Page.id(page), first_key}, tree)
    end
  end

  @doc """
  Updates the interval tree by removing a page range.
  """
  @spec remove_page_from_tree(t(), page()) :: t()
  def remove_page_from_tree(tree, page) do
    case Page.right_key(page) do
      nil -> tree
      last_key -> :gb_trees.delete_any(last_key, tree)
    end
  end

  @doc """
  Updates a page's position in the tree by removing the old range and adding the new range.
  Only updates if the range actually changed for efficiency.
  """
  @spec update_page_in_tree(t(), page(), page()) :: t()
  def update_page_in_tree(tree, old_page, new_page) do
    old_range = {Page.left_key(old_page), Page.right_key(old_page)}
    new_range = {Page.left_key(new_page), Page.right_key(new_page)}

    if old_range == new_range do
      tree
    else
      tree
      |> remove_page_from_tree(old_page)
      |> add_page_to_tree(new_page)
    end
  end

  @doc """
  Returns page IDs for all pages that overlap with the given query range.
  Uses boundary-based tree traversal with minimal comparisons for optimal efficiency.

  Algorithm:
  1. find_and_collect_overlapping: Navigate tree seeking pages that overlap [query_start, query_end)
  2. collect_all_until_boundary: Once found, collect all remaining pages until boundary (no overlap checks!)

  This is O(log n + k) where k is the number of overlapping pages.
  Minimizes comparisons by switching to collection mode after finding boundaries.
  """
  @spec page_ids_in_range(t(), Bedrock.key(), Bedrock.key()) :: [page_id()]
  def page_ids_in_range({size, tree_node}, query_start, query_end) when size > 0,
    do: traverse_tree_collecting_overlapping_pages(tree_node, query_start, query_end)

  def page_ids_in_range(_, _query_start, _query_end), do: []

  defp traverse_tree_collecting_overlapping_pages(tree_node, query_start, query_end) do
    tree_node
    |> find_and_collect_overlapping_pages(query_start, query_end, [])
    |> Enum.reverse()
  end

  defp find_and_collect_overlapping_pages(nil, _query_start, _query_end, collected_page_ids), do: collected_page_ids

  defp find_and_collect_overlapping_pages({last_key, _, _left, right}, query_start, query_end, collected_page_ids)
       when last_key < query_start,
       do: find_and_collect_overlapping_pages(right, query_start, query_end, collected_page_ids)

  defp find_and_collect_overlapping_pages(
         {last_key, {page_id, first_key}, left, right},
         query_start,
         query_end,
         collected_page_ids
       ) do
    collection_after_left = find_and_collect_overlapping_pages(left, query_start, query_end, collected_page_ids)

    if first_key <= query_end and last_key >= query_start do
      collection_with_current = [page_id | collection_after_left]
      collect_remaining_pages_until_boundary(right, query_end, collection_with_current)
    else
      find_and_collect_overlapping_pages(right, query_start, query_end, collection_after_left)
    end
  end

  defp collect_remaining_pages_until_boundary(nil, _query_end, collected_page_ids), do: collected_page_ids

  defp collect_remaining_pages_until_boundary(
         {_last_key, {page_id, first_key}, left, right},
         query_end,
         collected_page_ids
       ) do
    if first_key > query_end do
      collected_page_ids
    else
      collection_after_left = collect_remaining_pages_until_boundary(left, query_end, collected_page_ids)
      collection_with_current = [page_id | collection_after_left]
      collect_remaining_pages_until_boundary(right, query_end, collection_with_current)
    end
  end
end
