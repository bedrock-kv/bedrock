defmodule Bedrock.DataPlane.Storage.Olivine.Index do
  @moduledoc """
  B-tree-like index structure for the Olivine storage driver.

  ## Structure

  The index consists of:
  - **Tree**: gb_trees keyed by page `last_key`, storing `{page_id, first_key}`
  - **Page Map**: Map of page_id → Page structs containing key-value pairs
  - **Page Chain**: Linked list of pages via `next_id` pointers, starting from page 0

  ## Critical Invariants

  1. **Page 0 Existence**: Page 0 must always exist as the leftmost page
  2. **Tree Ordering**: Pages in tree order have strictly ascending `last_key` values
  3. **Chain Integrity**: Page chain starts at 0, terminates at 0, covers all pages
  4. **Key Ordering**: Following page chain yields all keys in strictly ascending order
  5. **Non-overlapping**: Pages have non-overlapping key ranges
  6. **Page Keys**: Within each page, keys are sorted; `first_key <= last_key`

  ## Page Chain Reconstruction

  When pages are added or modified, the page chain is automatically rebuilt from tree
  ordering to maintain consistency. This ensures proper key ordering across page boundaries
  and prevents chain corruption during concurrent operations and page splits.

  ## Multi-Split Support

  Pages can be split into multiple pages when they exceed size limits. The original page
  ID is preserved for the first split to maintain consistency, especially for page 0.
  Chain pointers are updated to maintain traversal integrity.

  ## Algorithms

  ### Key Lookup
  1. Use `Tree.page_for_key(key)` to find page containing key
  2. Search within page for exact key match

  ### Mutation Application
  1. Use `Tree.page_for_insertion(key)` to find target page
  2. For gaps between pages, place key in page whose `first_key` is smallest > key
  3. If no such page exists, use rightmost page
  4. Apply operations to page, maintaining sorted order within page

  ### Page Splitting
  1. When page exceeds 256 keys, split at midpoint (or into multiple pages)
  2. Left half keeps original page_id (preserves page 0 as leftmost)
  3. Right halves get new page_ids
  4. Update tree entries and rebuild page chain from tree ordering
  5. Chain integrity is automatically maintained through reconstruction

  ### Range Clearing
  1. Find all pages intersecting the range using tree
  2. For single page: clear keys within range
  3. For multiple pages: delete middle pages, clear edges
  4. Chain integrity maintained through automatic reconstruction
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager

  @type operation :: IndexManager.operation()

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(),
          page_map: map()
        }

  defstruct [
    :tree,
    :page_map
  ]

  @doc """
  Creates a new empty Index with an initial page.
  """
  @spec new() :: t()
  def new do
    initial_page = Page.new(0, [])
    initial_tree = :gb_trees.empty()
    initial_page_map = %{0 => initial_page}

    %__MODULE__{
      tree: initial_tree,
      page_map: initial_page_map
    }
  end

  @doc """
  Loads an Index from the database by traversing the page chain and building the tree structure.
  Returns {:ok, index, max_page_id, free_page_ids} or an error.
  """
  @spec load_from(Database.t()) ::
          {:ok, t(), Page.id(), [Page.id()]} | {:error, :corrupted_page | :broken_chain | :cycle_detected | :no_chain}
  def load_from(database) do
    case load_page_chain(database, 0, %{}) do
      {:ok, page_map} ->
        tree = Tree.from_page_map(page_map)
        page_ids = page_map |> Map.keys() |> MapSet.new()
        max_page_id = max(0, Enum.max(page_ids))
        free_page_ids = calculate_free_page_ids(max_page_id, page_ids)

        initial_page_map =
          if :gb_trees.is_empty(tree) and max_page_id == 0 do
            %{0 => Page.new(0, [])}
          else
            page_map
          end

        index = %__MODULE__{
          tree: tree,
          page_map: initial_page_map
        }

        {:ok, index, max_page_id, free_page_ids}

      {:error, :no_chain} ->
        {:ok, new(), 0, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_page_chain(_database, page_id, page_map) when is_map_key(page_map, page_id), do: {:error, :cycle_detected}

  defp load_page_chain(database, page_id, page_map) do
    with {:ok, page} <- Database.load_page(database, page_id),
         :ok <- Page.validate(page) do
      page_map
      |> Map.put(page_id, page)
      |> load_next_page_in_chain(database, page)
    else
      {:error, :not_found} when page_id == 0 ->
        {:error, :no_chain}

      _ ->
        {:error, :broken_chain}
    end
  end

  defp load_next_page_in_chain(page_map, database, page) do
    case Page.next_id(page) do
      0 -> {:ok, page_map}
      next_id -> load_page_chain(database, next_id, page_map)
    end
  end

  defp calculate_free_page_ids(0, _all_existing_page_ids), do: []

  defp calculate_free_page_ids(max_page_id, all_existing_page_ids) do
    0..max_page_id
    |> MapSet.new()
    |> MapSet.difference(all_existing_page_ids)
    |> Enum.sort()
  end

  @doc """
  Gets a page by its ID from the index.
  Raises if the page is not found.
  """
  @spec get_page!(t(), Page.id()) :: Page.t()
  def get_page!(%__MODULE__{page_map: page_map}, page_id), do: Map.fetch!(page_map, page_id)

  @doc """
  Finds the page containing the given key in this index.
  Returns {:ok, Page.t()} if found, {:error, :not_found} if not found.
  """
  @spec page_for_key(t(), Bedrock.key()) :: {:ok, Page.t()} | {:error, :not_found}
  def page_for_key(%__MODULE__{tree: tree, page_map: page_map}, key) do
    tree
    |> Tree.page_for_key(key)
    |> case do
      nil -> {:error, :not_found}
      page_id -> {:ok, Map.fetch!(page_map, page_id)}
    end
  end

  @doc """
  Finds all pages that contain keys within the given range in this index.
  Returns {:ok, [Page.t()]} with the list of pages (may be empty).
  """
  @spec pages_for_range(t(), Bedrock.key(), Bedrock.key()) :: {:ok, [Page.t()]}
  def pages_for_range(%__MODULE__{tree: tree, page_map: page_map}, start_key, end_key) do
    {:ok,
     tree
     |> Tree.page_ids_in_range(start_key, end_key)
     |> Enum.map(&Map.fetch!(page_map, &1))}
  end

  @doc """
  Removes a single page from the index.
  Updates both the tree structure and page_map.
  Returns the updated index.

  **Note**: Page 0 cannot be deleted as it must always exist as the leftmost page.
  Attempting to delete page 0 raises `ArgumentError`.
  """
  @spec delete_page(t(), Page.id()) :: t()
  def delete_page(_index, 0),
    do: raise(ArgumentError, "Cannot delete page 0 - it must always exist as the leftmost page")

  def delete_page(%__MODULE__{tree: tree, page_map: page_map} = index, page_id) do
    case Map.fetch(page_map, page_id) do
      {:ok, page} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  @doc """
  Updates a page in the index with a new version.
  Updates both the tree structure and page_map.
  Returns the updated index.
  """
  @spec update_page(t(), Page.t(), Page.t()) :: t()
  def update_page(%__MODULE__{tree: tree, page_map: page_map} = index, old_page, new_page) do
    updated_tree = Tree.update_page_in_tree(tree, old_page, new_page)
    updated_page_map = Map.put(page_map, Page.id(new_page), new_page)
    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  @doc """
  Splits a page in the index when it becomes too large.
  First deletes the original page, then adds the split pages.
  Returns the updated index.
  """
  @spec split_page(t(), Page.t(), Page.t(), Page.id()) :: t()
  def split_page(index, original_page, updated_page, right_page_id) do
    original_page_id = Page.id(updated_page)

    key_count = Page.key_count(updated_page)
    {left_page_raw, right_page_raw} = Page.split_page(updated_page, div(key_count, 2), right_page_id)

    left_page = Page.new(original_page_id, Page.key_versions(left_page_raw), 0)
    right_page = Page.new(right_page_id, Page.key_versions(right_page_raw), 0)

    temp_index =
      index
      |> update_page_lazy(original_page, left_page)
      |> add_page_lazy(right_page)

    split_chain_update(temp_index, original_page, left_page, right_page)
  end

  @doc """
  Splits a page into multiple pages when it contains too many keys.
  Ensures no page exceeds 256 keys while maintaining chain continuity.

  The original page ID is preserved for the first page in the chain.
  The last page points to the original page's next_id.
  """
  @spec multi_split_page(t(), Page.t(), Page.t(), [Page.id()]) :: t()
  def multi_split_page(index, original_page, updated_page, new_page_ids) do
    original_page_id = Page.id(updated_page)
    original_next_id = Page.next_id(updated_page)
    all_key_versions = Page.key_versions(updated_page)

    key_chunks = Enum.chunk_every(all_key_versions, 256)

    all_page_ids = [original_page_id | new_page_ids]

    next_ids = new_page_ids ++ [original_next_id]

    new_pages =
      key_chunks
      |> Enum.zip(all_page_ids)
      |> Enum.zip(next_ids)
      |> Enum.map(fn {{chunk_keys, page_id}, next_id} ->
        Page.new(page_id, chunk_keys, next_id)
      end)

    [first_page | remaining_pages] = new_pages

    index_with_first_page_updated =
      index
      |> update_page_lazy(original_page, first_page)
      |> add_pages_batch(remaining_pages)

    index_with_first_page_updated
  end

  @doc """
  Adds a page to the index.
  Updates both the tree structure and page_map, and ensures chain consistency.
  Returns the updated index.
  """
  @spec add_page(t(), Page.t()) :: t()
  def add_page(%__MODULE__{tree: tree, page_map: page_map} = index, page) do
    updated_tree = Tree.add_page_to_tree(tree, page)
    updated_page_map = Map.put(page_map, Page.id(page), page)
    temp_index = %{index | tree: updated_tree, page_map: updated_page_map}
    ensure_chain_current(temp_index)
  end

  @spec ensure_chain_current(t()) :: t()
  defp ensure_chain_current(%__MODULE__{tree: tree, page_map: page_map} = index) do
    updated_page_map = rebuild_page_chain_from_tree(tree, page_map)
    %{index | page_map: updated_page_map}
  end

  @spec update_page_lazy(t(), Page.t(), Page.t()) :: t()
  defp update_page_lazy(%__MODULE__{tree: tree, page_map: page_map} = index, old_page, new_page) do
    updated_tree = Tree.update_page_in_tree(tree, old_page, new_page)
    updated_page_map = Map.put(page_map, Page.id(new_page), new_page)
    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  @spec add_page_lazy(t(), Page.t()) :: t()
  defp add_page_lazy(%__MODULE__{tree: tree, page_map: page_map} = index, page) do
    updated_tree = Tree.add_page_to_tree(tree, page)
    updated_page_map = Map.put(page_map, Page.id(page), page)
    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  @spec rebuild_page_chain_from_tree(:gb_trees.tree(), map()) :: map()
  defp rebuild_page_chain_from_tree(tree, page_map) do
    tree_page_ids = Tree.page_ids_in_range(tree, <<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>)

    case tree_page_ids do
      [] -> create_empty_page_0(page_map)
      [first_id | _] -> rebuild_chain_with_pages(tree_page_ids, page_map, first_id)
    end
  end

  defp create_empty_page_0(page_map), do: Map.put(page_map, 0, Page.new(0, [], 0))

  defp rebuild_chain_with_pages(tree_page_ids, page_map, first_id) do
    updated_page_map = update_page_0_for_chain(page_map, first_id)

    updated_page_map
    |> chain_pages_together(tree_page_ids)
    |> set_last_page_terminator(tree_page_ids)
  end

  defp update_page_0_for_chain(page_map, first_id) do
    case Map.get(page_map, 0) do
      nil -> Map.put(page_map, 0, Page.new(0, [], first_id))
      existing_page_0 -> update_existing_page_0(page_map, existing_page_0, first_id)
    end
  end

  defp update_existing_page_0(page_map, existing_page_0, first_id) do
    if Page.empty?(existing_page_0) do
      Map.put(page_map, 0, Page.new(0, [], first_id))
    else
      Map.put(page_map, 0, Page.new(0, Page.key_versions(existing_page_0), first_id))
    end
  end

  defp chain_pages_together(page_map, tree_page_ids) do
    tree_page_ids
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.reduce(page_map, &update_page_next_id/2)
  end

  defp update_page_next_id([current_id, next_id], acc_map) do
    case Map.get(acc_map, current_id) do
      nil -> acc_map
      current_page -> Map.put(acc_map, current_id, Page.new(current_id, Page.key_versions(current_page), next_id))
    end
  end

  defp set_last_page_terminator(page_map, tree_page_ids) do
    last_id = List.last(tree_page_ids)

    case Map.get(page_map, last_id) do
      nil -> page_map
      last_page -> Map.put(page_map, last_id, Page.new(last_id, Page.key_versions(last_page), 0))
    end
  end

  @doc """
  Removes multiple pages from the index by their IDs.
  Updates both the tree structure and page_map.
  Returns the updated index.
  """
  @spec delete_pages(t(), [Page.id()]) :: t()
  def delete_pages(index, []), do: index

  def delete_pages(%__MODULE__{} = index, page_ids) do
    sorted_page_ids = Enum.sort(page_ids)

    case detect_sequential_deletion(index, sorted_page_ids) do
      {:sequential, first_id, last_id} ->
        index_with_updated_chain = range_clear_chain_update(index, first_id, last_id, sorted_page_ids)
        delete_pages_from_structures(index_with_updated_chain, page_ids)

      :non_sequential ->
        delete_pages_individually(index, page_ids)
    end
  end

  defp detect_sequential_deletion(index, sorted_page_ids) do
    tree_order = Tree.page_ids_in_range(index.tree, <<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>)

    case find_contiguous_sequence(tree_order, sorted_page_ids) do
      {:ok, first, last} -> {:sequential, first, last}
      :error -> :non_sequential
    end
  end

  defp find_contiguous_sequence(tree_order, page_ids) do
    positions = Enum.map(page_ids, &Enum.find_index(tree_order, fn id -> id == &1 end))

    if Enum.all?(positions, &(&1 != nil)) do
      sorted_positions = Enum.sort(positions)

      if contiguous_sequence?(sorted_positions) do
        first_pos = List.first(sorted_positions)
        last_pos = List.last(sorted_positions)
        {:ok, Enum.at(tree_order, first_pos), Enum.at(tree_order, last_pos)}
      else
        :error
      end
    else
      :error
    end
  end

  defp contiguous_sequence?([_]), do: true
  defp contiguous_sequence?([a, b | rest]) when b == a + 1, do: contiguous_sequence?([b | rest])
  defp contiguous_sequence?(_), do: false

  defp delete_pages_individually(index, page_ids) do
    Enum.reduce(page_ids, index, fn page_id, index_acc ->
      index_acc
      |> delete_page_chain_update(page_id)
      |> delete_page_from_structures(page_id)
    end)
  end

  defp delete_page_from_structures(%__MODULE__{tree: tree, page_map: page_map} = index, page_id) do
    case Map.fetch(page_map, page_id) do
      {:ok, page} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  defp delete_pages_from_structures(%__MODULE__{tree: tree, page_map: page_map} = index, page_ids) do
    tree_without_pages =
      Enum.reduce(page_ids, tree, fn page_id, tree_acc ->
        case Map.fetch(page_map, page_id) do
          {:ok, page} -> Tree.remove_page_from_tree(tree_acc, page)
          :error -> tree_acc
        end
      end)

    page_map_without_pages = Map.drop(page_map, page_ids)

    %{index | tree: tree_without_pages, page_map: page_map_without_pages}
  end

  @spec split_chain_update(t(), Page.t(), Page.t(), Page.t()) :: t()
  defp split_chain_update(index, original_page, left_page, right_page) do
    original_next_id = Page.next_id(original_page)

    updated_left = Page.new(Page.id(left_page), Page.key_versions(left_page), Page.id(right_page))
    updated_right = Page.new(Page.id(right_page), Page.key_versions(right_page), original_next_id)

    updated_page_map =
      index.page_map
      |> Map.put(Page.id(updated_left), updated_left)
      |> Map.put(Page.id(updated_right), updated_right)

    %{index | page_map: updated_page_map}
  end

  @spec range_clear_chain_update(t(), Page.id(), Page.id(), [Page.id()]) :: t()
  defp range_clear_chain_update(index, first_affected_id, last_affected_id, _middle_page_ids) do
    last_affected_page = Map.get(index.page_map, last_affected_id)
    successor_id = Page.next_id(last_affected_page)

    case find_predecessor_via_tree(index.tree, first_affected_id) do
      nil ->
        index

      pred_id ->
        predecessor = Map.get(index.page_map, pred_id)
        updated_predecessor = Page.new(Page.id(predecessor), Page.key_versions(predecessor), successor_id)

        %{index | page_map: Map.put(index.page_map, pred_id, updated_predecessor)}
    end
  end

  @spec delete_page_chain_update(t(), Page.id()) :: t()
  defp delete_page_chain_update(index, page_id) do
    page = Map.get(index.page_map, page_id)
    successor_id = Page.next_id(page)

    case find_predecessor_via_tree(index.tree, page_id) do
      nil ->
        index

      pred_id ->
        predecessor = Map.get(index.page_map, pred_id)
        updated_predecessor = Page.new(Page.id(predecessor), Page.key_versions(predecessor), successor_id)

        %{index | page_map: Map.put(index.page_map, pred_id, updated_predecessor)}
    end
  end

  @spec add_pages_batch(t(), [Page.t()]) :: t()
  defp add_pages_batch(index, pages) do
    Enum.reduce(pages, index, fn page, acc_index ->
      updated_tree = Tree.add_page_to_tree(acc_index.tree, page)
      updated_page_map = Map.put(acc_index.page_map, Page.id(page), page)

      %{acc_index | tree: updated_tree, page_map: updated_page_map}
    end)
  end

  @spec find_predecessor_via_tree(:gb_trees.tree(), Page.id()) :: Page.id() | nil
  defp find_predecessor_via_tree(tree, target_page_id) do
    case find_page_last_key_in_tree(tree, target_page_id) do
      nil ->
        nil

      target_last_key ->
        find_predecessor_by_last_key(tree, target_last_key)
    end
  end

  @spec find_page_last_key_in_tree(:gb_trees.tree(), Page.id()) :: binary() | nil
  defp find_page_last_key_in_tree(tree, target_page_id) do
    iterator = :gb_trees.iterator(tree)
    find_page_last_key_in_iterator(iterator, target_page_id)
  end

  defp find_page_last_key_in_iterator(iterator, target_page_id) do
    case :gb_trees.next(iterator) do
      {last_key, {page_id, _first_key}, next_iter} ->
        if page_id == target_page_id do
          last_key
        else
          find_page_last_key_in_iterator(next_iter, target_page_id)
        end

      :none ->
        nil
    end
  end

  @spec find_predecessor_by_last_key(:gb_trees.tree(), binary()) :: Page.id() | nil
  defp find_predecessor_by_last_key(tree, target_last_key) do
    iterator = :gb_trees.iterator(tree)
    find_predecessor_via_iterator(iterator, target_last_key, nil)
  end

  defp find_predecessor_via_iterator(iterator, target_key, best_candidate) do
    case :gb_trees.next(iterator) do
      {node_key, {page_id, _first_key}, next_iter} ->
        if node_key < target_key do
          find_predecessor_via_iterator(next_iter, target_key, page_id)
        else
          best_candidate
        end

      :none ->
        best_candidate
    end
  end
end
