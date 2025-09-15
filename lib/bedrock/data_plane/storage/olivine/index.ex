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
    initial_page_map = %{0 => {initial_page, 0}}

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
            empty_page = Page.new(0, [])
            %{0 => {empty_page, 0}}
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
    with {:ok, {page, next_id}} <- Database.load_page(database, page_id),
         :ok <- Page.validate(page) do
      page_map
      |> Map.put(page_id, {page, next_id})
      |> load_next_page_in_chain(database, next_id)
    else
      {:error, :not_found} when page_id == 0 ->
        {:error, :no_chain}

      _ ->
        {:error, :broken_chain}
    end
  end

  defp load_next_page_in_chain(page_map, database, next_id) do
    case next_id do
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
  def get_page!(%__MODULE__{page_map: page_map}, page_id) do
    {page, _next_id} = Map.fetch!(page_map, page_id)
    page
  end

  @doc """
  Gets a page and its cached next_id from the index.
  Returns {page_binary, next_id}.
  """
  @spec get_page_with_next_id!(t(), Page.id()) :: {Page.t(), Page.id()}
  def get_page_with_next_id!(%__MODULE__{page_map: page_map}, page_id) do
    Map.fetch!(page_map, page_id)
  end

  @doc """
  Finds the page containing the given key in this index.
  Returns {:ok, Page.t()} if found, {:error, :not_found} if not found.
  """
  @spec page_for_key(t(), Bedrock.key()) :: {:ok, Page.t()} | {:error, :not_found}
  def page_for_key(%__MODULE__{tree: tree, page_map: page_map}, key) do
    tree
    |> Tree.page_for_key(key)
    |> case do
      nil ->
        {:error, :not_found}

      page_id ->
        {page, _next_id} = Map.fetch!(page_map, page_id)
        {:ok, page}
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
     |> Enum.map(fn page_id ->
       {page, _next_id} = Map.fetch!(page_map, page_id)
       page
     end)}
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
      {:ok, {page, _next_id}} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  @doc """
  Splits a page into multiple pages when it contains too many keys.
  Ensures no page exceeds 256 keys while maintaining chain continuity.

  The original page ID is preserved for the first page in the chain.
  The last page points to the original page's next_id.
  """
  @spec multi_split_page(t(), Page.id(), Page.id(), Page.t(), [Page.id()]) :: t()
  def multi_split_page(index, original_page_id, original_next_id, updated_page, new_page_ids) do
    all_key_versions = Page.key_versions(updated_page)

    key_chunks = Enum.chunk_every(all_key_versions, 256)

    all_page_ids = [original_page_id | new_page_ids]

    # Build proper chain: first -> second -> ... -> last -> original_next_id
    next_ids = new_page_ids ++ [original_next_id]

    new_page_tuples =
      key_chunks
      |> Enum.zip(all_page_ids)
      |> Enum.zip(next_ids)
      |> Enum.map(fn {{chunk_keys, page_id}, next_id} ->
        # next_id in binary doesn't matter, we use tuple
        page = Page.new(page_id, chunk_keys)
        {page, next_id}
      end)

    [first_page_tuple | remaining_page_tuples] = new_page_tuples

    {first_page, first_next_id} = first_page_tuple

    # Get the original page from page_map for the update operation
    {original_page, _} = Map.get(index.page_map, original_page_id)

    index_with_first_page_updated =
      index
      |> update_index_with_page(original_page, first_page, first_next_id)
      |> add_pages_batch(remaining_page_tuples)

    index_with_first_page_updated
  end

  # Unified helpers to eliminate tree/page_map update duplication

  @spec update_index_with_page(t(), Page.t(), Page.t(), Page.id()) :: t()
  defp update_index_with_page(%__MODULE__{tree: tree, page_map: page_map} = index, old_page, new_page, next_id) do
    updated_tree = Tree.update_page_in_tree(tree, old_page, new_page)
    updated_page_map = Map.put(page_map, Page.id(new_page), {new_page, next_id})
    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  @doc """
  Removes multiple pages from the index by their IDs.
  Updates both the tree structure and page_map.
  Returns the updated index.
  """
  @spec delete_pages(t(), [Page.id()]) :: t()
  def delete_pages(index, []), do: index

  def delete_pages(index, page_ids) do
    Enum.reduce(page_ids, index, fn page_id, index_acc ->
      index_acc
      |> delete_page_chain_update(page_id)
      |> delete_page_from_structures(page_id)
    end)
  end

  defp delete_page_from_structures(%__MODULE__{tree: tree, page_map: page_map} = index, page_id) do
    case Map.fetch(page_map, page_id) do
      {:ok, {page, _next_id}} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  # Unified chain update helper - eliminates redundant logic and unnecessary Page.new calls
  @spec update_page_chain_pointer(t(), Page.id(), Page.id()) :: t()
  defp update_page_chain_pointer(%__MODULE__{page_map: page_map} = index, page_id, new_next_id) do
    case Map.get(page_map, page_id) do
      nil ->
        index

      {page, _old_next_id} ->
        updated_page_map = Map.put(page_map, page_id, {page, new_next_id})
        %{index | page_map: updated_page_map}
    end
  end

  @spec update_predecessor_chain_pointer(t(), Page.id(), Page.id()) :: t()
  defp update_predecessor_chain_pointer(%__MODULE__{tree: tree} = index, target_page_id, new_successor_id) do
    case find_predecessor_via_tree(tree, target_page_id) do
      nil ->
        # No predecessor found in tree - check if target is leftmost page (pointed to by page 0)
        handle_leftmost_page_deletion(index, target_page_id, new_successor_id)

      pred_id ->
        update_page_chain_pointer(index, pred_id, new_successor_id)
    end
  end

  # Handle deletion of leftmost page by updating page 0's pointer
  @spec handle_leftmost_page_deletion(t(), Page.id(), Page.id()) :: t()
  defp handle_leftmost_page_deletion(index, target_page_id, new_successor_id) do
    case Map.get(index.page_map, 0) do
      {_page_0, leftmost_id} when leftmost_id == target_page_id ->
        # Page 0 points to the target page - update it to point to successor
        update_page_chain_pointer(index, 0, new_successor_id)

      _ ->
        # Target page is not the leftmost - no predecessor to update
        index
    end
  end

  @spec delete_page_chain_update(t(), Page.id()) :: t()
  defp delete_page_chain_update(index, page_id) do
    {_page, successor_id} = Map.get(index.page_map, page_id)
    update_predecessor_chain_pointer(index, page_id, successor_id)
  end

  # Optimized batch addition - single tree operation for all pages
  @spec add_pages_batch(t(), [{Page.t(), Page.id()}]) :: t()
  defp add_pages_batch(%__MODULE__{tree: tree, page_map: page_map} = index, page_tuples) do
    # Extract pages for batch tree operation
    pages = Enum.map(page_tuples, fn {page, _next_id} -> page end)

    # Single batch tree update
    updated_tree = Enum.reduce(pages, tree, &Tree.add_page_to_tree(&2, &1))

    # Batch page_map update
    updated_page_map =
      Enum.reduce(page_tuples, page_map, fn {page, next_id}, acc_map ->
        Map.put(acc_map, Page.id(page), {page, next_id})
      end)

    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  # Optimized tree traversal to find predecessor - single pass with state tracking
  @spec find_predecessor_via_tree(:gb_trees.tree(), Page.id()) :: Page.id() | nil
  defp find_predecessor_via_tree(tree, target_page_id) do
    iterator = :gb_trees.iterator(tree)
    find_predecessor_optimized(iterator, target_page_id, nil)
  end

  # Single pass through tree to find the page that comes just before target_page_id
  defp find_predecessor_optimized(iterator, target_page_id, last_page_id) do
    case :gb_trees.next(iterator) do
      {_last_key, {page_id, _first_key}, next_iter} ->
        if page_id == target_page_id do
          # Found target page, return the previous page we saw
          last_page_id
        else
          # Keep track of this page as potential predecessor and continue
          find_predecessor_optimized(next_iter, target_page_id, page_id)
        end

      :none ->
        # Reached end without finding target page
        nil
    end
  end
end
