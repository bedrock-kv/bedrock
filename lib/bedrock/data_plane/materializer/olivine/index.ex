defmodule Bedrock.DataPlane.Materializer.Olivine.Index do
  @moduledoc """
  B-tree-like index structure for the Olivine storage driver.

  ## Structure

  The index consists of:
  - **Tree**: gb_trees keyed by page `last_key`, storing `page_id`
  - **Page Map**: Map of page_id → Page structs containing key-value pairs
  - **Page Chain**: Linked list of pages via `next_id` pointers, starting from page 0
  - **Gap-Free Design**: Pages cover entire keyspace with no gaps between them

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
  1. Use `Tree.page_for_key(key)` to find target page (no gaps exist)
  2. Apply operations to page, maintaining sorted order within page

  ### Page Splitting
  1. When page exceeds 512 keys, split at 3/4 of max capacity (384 keys per page)
  2. First page keeps original page_id (preserves page 0 as leftmost)
  3. Subsequent pages get new page_ids
  4. Update tree entries and rebuild page chain from tree ordering
  5. Chain integrity is automatically maintained through reconstruction

  ### Range Clearing
  1. Find all pages intersecting the range using tree
  2. For single page: clear keys within range
  3. For multiple pages: delete middle pages, clear edges
  4. Chain integrity maintained through automatic reconstruction
  """

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IdAllocator
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree
  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Version

  # Default page sizing constant
  @default_max_keys_per_page 256

  @type operation :: IndexManager.operation()

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(),
          page_map: map(),
          min_key: Bedrock.key(),
          max_key: Bedrock.key(),
          max_keys_per_page: pos_integer(),
          target_keys_per_page: pos_integer()
        }

  defstruct [
    :tree,
    :page_map,
    :min_key,
    :max_key,
    max_keys_per_page: @default_max_keys_per_page,
    target_keys_per_page: div(@default_max_keys_per_page * 9, 10)
  ]

  @doc """
  Creates a new empty Index with an initial page covering the entire keyspace.

  ## Options
  - `max_keys_per_page` - Maximum keys per page before splitting (default: #{@default_max_keys_per_page})
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    max_keys = Keyword.get(opts, :max_keys_per_page, @default_max_keys_per_page)
    target_keys = div(max_keys * 9, 10)

    initial_page = Page.new(0, [])
    initial_tree = Tree.add_page_to_tree(:gb_trees.empty(), initial_page)
    initial_page_map = %{0 => {initial_page, 0}}

    %__MODULE__{
      tree: initial_tree,
      page_map: initial_page_map,
      min_key: <<0xFF, 0xFF>>,
      max_key: <<>>,
      max_keys_per_page: max_keys,
      target_keys_per_page: target_keys
    }
  end

  @spec locator_for_key(t(), Bedrock.key()) ::
          {:ok, Page.t(), Database.locator()} | {:error, :not_found}
  def locator_for_key(index, key) do
    page = page_for_key(index, key)

    case Page.locator_for_key(page, key) do
      {:ok, locator} -> {:ok, page, locator}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Loads an Index from the database by traversing the page chain and building the tree structure.
  Returns {:ok, index, max_id, free_ids, total_key_count} or an error.

  ## Options
  - `max_keys_per_page` - Maximum keys per page (default: #{@default_max_keys_per_page})
  """
  @spec load_from(Database.t(), keyword()) ::
          {:ok, t(), Page.id(), [Page.id()], non_neg_integer()}
          | {:error, :missing_pages}
  def load_from({_data_db, index_db}, opts \\ []) do
    {:ok, durable_version} = IndexDatabase.load_durable_version(index_db)
    max_keys = Keyword.get(opts, :max_keys_per_page, @default_max_keys_per_page)
    target_keys = div(max_keys * 3, 4)

    if durable_version == Version.zero() do
      {:ok, new(opts), 0, [], 0}
    else
      needed_page_ids = MapSet.new([0])

      case load_needed_pages(index_db, %{}, %{}, needed_page_ids, durable_version) do
        {:ok, final_page_map} ->
          build_index_from_page_map(final_page_map, max_keys, target_keys)

        {:error, :missing_pages} ->
          {:error, :missing_pages}
      end
    end
  end

  # Load needed pages iteratively from older version blocks
  # page_map: final result map (only needed pages)
  # all_pages_seen: cumulative view of all pages (newest version wins)
  @spec load_needed_pages(
          IndexDatabase.t(),
          page_map :: %{Page.id() => {Page.t(), Page.id()}},
          all_pages_seen :: %{Page.id() => {Page.t(), Page.id()}},
          needed_page_ids :: MapSet.t(Page.id()),
          Bedrock.version()
        ) :: {:ok, %{Page.id() => {Page.t(), Page.id()}}} | {:error, :missing_pages}
  defp load_needed_pages(index_db, page_map, all_pages_seen, needed_page_ids, current_version) do
    cond do
      MapSet.size(needed_page_ids) == 0 ->
        {:ok, page_map}

      current_version == nil || current_version == Version.zero() ->
        missing = Enum.reject(needed_page_ids, &Map.has_key?(all_pages_seen, &1))

        case missing do
          [] ->
            final_page_map = Enum.reduce(needed_page_ids, page_map, &Map.put_new(&2, &1, all_pages_seen[&1]))
            {:ok, final_page_map}

          [0] ->
            {:ok, Map.put(page_map, 0, {Page.new(0, []), 0})}

          _ ->
            {:error, :missing_pages}
        end

      true ->
        load_needed_pages_from_version(index_db, page_map, all_pages_seen, needed_page_ids, current_version)
    end
  end

  defp load_needed_pages_from_version(index_db, page_map, all_pages_seen, needed_page_ids, current_version) do
    case IndexDatabase.load_page_block(index_db, current_version) do
      {:ok, version_pages, next_version} ->
        updated_all_pages = Map.merge(version_pages, all_pages_seen)

        {updated_page_map, updated_needed} =
          process_version_pages(version_pages, page_map, needed_page_ids, updated_all_pages)

        load_needed_pages(index_db, updated_page_map, updated_all_pages, updated_needed, next_version)

      {:error, :not_found} ->
        load_needed_pages(index_db, page_map, all_pages_seen, needed_page_ids, Version.zero())
    end
  end

  defp process_version_pages(version_pages, page_map, needed_page_ids, updated_all_pages) do
    process_version_pages_loop(version_pages, page_map, needed_page_ids, updated_all_pages)
  end

  defp process_version_pages_loop(version_pages, page_map, needed_page_ids, updated_all_pages) do
    version_page_ids = MapSet.new(Map.keys(version_pages))

    to_process =
      needed_page_ids
      |> MapSet.intersection(version_page_ids)
      |> find_additional_needed_pages(page_map, updated_all_pages, version_page_ids)

    case MapSet.size(to_process) do
      0 ->
        {page_map, needed_page_ids}

      _ ->
        {new_page_map, new_needed} =
          Enum.reduce(to_process, {page_map, needed_page_ids}, fn page_id, {map_acc, needed_acc} ->
            process_needed_page(page_id, map_acc, MapSet.put(needed_acc, page_id), updated_all_pages)
          end)

        if new_needed == needed_page_ids do
          {new_page_map, new_needed}
        else
          process_version_pages_loop(version_pages, new_page_map, new_needed, updated_all_pages)
        end
    end
  end

  defp find_additional_needed_pages(initial_set, page_map, all_pages_seen, version_page_ids) do
    Enum.reduce(all_pages_seen, initial_set, fn
      {_id, {_page, next_id}}, acc when next_id != 0 ->
        if MapSet.member?(version_page_ids, next_id) and not Map.has_key?(page_map, next_id) do
          MapSet.put(acc, next_id)
        else
          acc
        end

      _, acc ->
        acc
    end)
  end

  defp process_needed_page(page_id, acc_map, acc_needed, updated_all_pages) do
    {resolved_page, resolved_next_id} = Map.get(updated_all_pages, page_id)
    new_map = Map.put(acc_map, page_id, {resolved_page, resolved_next_id})
    new_needed = MapSet.delete(acc_needed, page_id)

    final_needed =
      if resolved_next_id != 0 and not Map.has_key?(new_map, resolved_next_id) do
        MapSet.put(new_needed, resolved_next_id)
      else
        new_needed
      end

    {new_map, final_needed}
  end

  @spec build_index_from_page_map(%{Page.id() => {Page.t(), Page.id()}}, pos_integer(), pos_integer()) ::
          {:ok, t(), Page.id(), [Page.id()], non_neg_integer()}
  defp build_index_from_page_map(page_map, max_keys_per_page, target_keys_per_page) do
    case verify_page_chain(page_map) do
      :ok ->
        :ok

      {:error, {:broken_chain, missing_page_id}} ->
        require Logger

        Logger.error("Page chain is broken: page #{missing_page_id} is referenced but not in page_map")

      {:error, {:cycle, page_id}} ->
        require Logger

        Logger.error("Page chain has a cycle at page #{page_id}")
    end

    tree = Tree.from_page_map(page_map)
    page_ids = page_map |> Map.keys() |> MapSet.new()
    max_id = if MapSet.size(page_ids) > 0, do: Enum.max(page_ids), else: 0
    free_ids = calculate_free_ids(max_id, page_ids)

    initial_page_map =
      if :gb_trees.is_empty(tree) and max_id == 0 do
        empty_page = Page.new(0, [])
        %{0 => {empty_page, 0}}
      else
        page_map
      end

    {min_key, max_key} = calculate_key_bounds(tree, initial_page_map)

    total_key_count =
      initial_page_map
      |> Enum.map(fn {_, {page, _next_id}} -> Page.key_count(page) end)
      |> Enum.sum()

    index = %__MODULE__{
      tree: tree,
      page_map: initial_page_map,
      min_key: min_key,
      max_key: max_key,
      max_keys_per_page: max_keys_per_page,
      target_keys_per_page: target_keys_per_page
    }

    {:ok, index, max_id, free_ids, total_key_count}
  end

  defp verify_page_chain(page_map) do
    case Map.get(page_map, 0) do
      nil ->
        :ok

      {_page, next_id} ->
        verify_chain_walk(page_map, next_id, MapSet.new([0]))
    end
  end

  defp verify_chain_walk(_page_map, 0, _visited), do: :ok

  defp verify_chain_walk(page_map, page_id, visited) do
    cond do
      MapSet.member?(visited, page_id) ->
        {:error, {:cycle, page_id}}

      not Map.has_key?(page_map, page_id) ->
        {:error, {:broken_chain, page_id}}

      true ->
        {_page, next_id} = page_map[page_id]
        verify_chain_walk(page_map, next_id, MapSet.put(visited, page_id))
    end
  end

  defp calculate_free_ids(0, _all_existing_page_ids), do: []

  defp calculate_free_ids(max_id, all_existing_page_ids) do
    0..max_id
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
  """
  @spec page_for_key(t(), Bedrock.key()) :: Page.t()
  def page_for_key(%__MODULE__{tree: tree, page_map: page_map}, key) do
    page_id = Tree.page_for_key(tree, key)
    {page, _next_id} = Map.fetch!(page_map, page_id)
    page
  end

  @doc """
  Finds all pages that contain keys within the given range in this index.
  Returns {:ok, [Page.t()]} with the list of pages (may be empty).
  """
  @spec pages_for_range(t(), Bedrock.key(), Bedrock.key()) :: {:ok, [Page.t()]}
  def pages_for_range(%__MODULE__{tree: tree, page_map: page_map}, start_key, end_key) do
    first_page_id = Tree.page_for_key(tree, start_key)
    page_ids = collect_pages_in_range(page_map, first_page_id, end_key, [])

    pages =
      Enum.map(page_ids, fn page_id ->
        {page, _next_id} = Map.fetch!(page_map, page_id)
        page
      end)

    {:ok, pages}
  end

  defp collect_pages_in_range(page_map, page_id, end_key, collected_page_ids) do
    {page, next_id} = Map.fetch!(page_map, page_id)
    updated_collected = [page_id | collected_page_ids]
    last_key = Page.right_key(page)

    cond do
      last_key != nil and last_key >= end_key ->
        Enum.reverse(updated_collected)

      next_id == 0 ->
        Enum.reverse(updated_collected)

      true ->
        collect_pages_in_range(page_map, next_id, end_key, updated_collected)
    end
  end

  @doc """
  Splits a page into multiple pages using per-key segments for efficiency.

  This version accepts pre-computed segments from `Page.apply_operations_as_segments/2`,
  avoiding the need to decode an oversized binary page.

  Uses floor division to minimize chunks and distribute remainder evenly.
  Example: 462 keys with target 230 → [231, 231] rather than [230, 230, 2]

  The original page ID is preserved for the first page in the chain.
  The last page points to the original page's next_id.

  Returns {updated_index, new_page_ids, updated_allocator}.
  """
  @spec multi_split_page_from_segments(
          t(),
          Page.id(),
          Page.id(),
          Page.t(),
          [Page.segment()],
          non_neg_integer(),
          IdAllocator.t()
        ) :: {t(), [Page.id()], IdAllocator.t()}
  def multi_split_page_from_segments(
        index,
        original_page_id,
        original_next_id,
        original_page,
        segments,
        key_count,
        id_allocator
      ) do
    chunk_sizes = determine_chunk_sizes(key_count, index.target_keys_per_page)

    additional_pages_needed = length(chunk_sizes) - 1
    {new_page_ids, updated_allocator} = IdAllocator.allocate_ids(id_allocator, additional_pages_needed)

    {new_page_tuples, _remaining, _, _} =
      Enum.reduce(
        chunk_sizes,
        {[], segments, original_next_id, Enum.reverse([original_page_id | new_page_ids])},
        fn chunk_size, {pages_acc, segs_remaining, next_id, [page_id | remaining_ids]} ->
          rightmost_segment = hd(segs_remaining)

          rightmost_key =
            case rightmost_segment do
              {_offset, _length} -> extract_rightmost_from_segment(rightmost_segment, original_page)
              binary when is_binary(binary) -> extract_rightmost_from_segment(binary)
            end

          {iodata, remaining_segs} = take_and_materialize_reverse(segs_remaining, chunk_size, original_page)

          page_binary = Page.build_from_segments_iodata(page_id, iodata, chunk_size, rightmost_key)

          {[{page_binary, next_id} | pages_acc], remaining_segs, page_id, remaining_ids}
        end
      )

    [{first_page, first_next_id} | remaining_page_tuples] = new_page_tuples
    {original_page_binary, _} = Map.get(index.page_map, original_page_id)

    index_with_first_page_updated =
      index
      |> update_index_with_page(original_page_binary, first_page, first_next_id)
      |> add_pages_batch(remaining_page_tuples)

    {index_with_first_page_updated, new_page_ids, updated_allocator}
  end

  defp extract_rightmost_from_segment({offset, length}, original_page) do
    <<_locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>> =
      binary_part(original_page, offset, length)

    key
  end

  defp extract_rightmost_from_segment(
         <<_locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>>
       ) do
    key
  end

  defp take_and_materialize_reverse(segments, count, old_page) when count > 0,
    do: do_take_materialize_coalesce(segments, count, old_page, [], nil)

  defp do_take_materialize_coalesce(rest, 0, _old_page, acc, nil), do: {acc, rest}

  defp do_take_materialize_coalesce(rest, 0, old_page, acc, {offset, len}),
    do: {[binary_part(old_page, offset, len) | acc], rest}

  defp do_take_materialize_coalesce([], _count, _old_page, acc, nil), do: {acc, []}

  defp do_take_materialize_coalesce([], _count, old_page, acc, {offset, len}),
    do: {[binary_part(old_page, offset, len) | acc], []}

  defp do_take_materialize_coalesce([{o2, l2} | rest], count, old_page, acc, {o1, l1}) when o1 + l1 == o2 do
    do_take_materialize_coalesce(rest, count - 1, old_page, acc, {o1, l1 + l2})
  end

  defp do_take_materialize_coalesce([{o2, l2} | rest], count, old_page, acc, {o1, l1}) do
    segment_ref = binary_part(old_page, o1, l1)
    do_take_materialize_coalesce(rest, count - 1, old_page, [segment_ref | acc], {o2, l2})
  end

  defp do_take_materialize_coalesce([{offset, len} | rest], count, old_page, acc, nil),
    do: do_take_materialize_coalesce(rest, count - 1, old_page, acc, {offset, len})

  defp do_take_materialize_coalesce([binary | rest], count, old_page, acc, {offset, len}) when is_binary(binary),
    do: do_take_materialize_coalesce(rest, count - 1, old_page, [binary, binary_part(old_page, offset, len) | acc], nil)

  defp do_take_materialize_coalesce([binary | rest], count, old_page, acc, nil) when is_binary(binary),
    do: do_take_materialize_coalesce(rest, count - 1, old_page, [binary | acc], nil)

  defp determine_chunk_sizes(total_keys, target_size) do
    # Optimized chunk size calculation with early return
    cond do
      total_keys <= target_size ->
        [total_keys]

      total_keys <= target_size * 2 ->
        # Simple two-way split for small overages
        half = div(total_keys, 2)

        if rem(total_keys, 2) == 0 do
          [half, half]
        else
          [half + 1, half]
        end

      true ->
        # General case for larger splits
        num_chunks = calculate_optimal_chunks(total_keys, target_size)
        base_size = div(total_keys, num_chunks)
        extra_keys = rem(total_keys, num_chunks)

        # Build list of chunk sizes - extra keys go first
        if extra_keys == 0 do
          List.duplicate(base_size, num_chunks)
        else
          List.duplicate(base_size + 1, extra_keys) ++ List.duplicate(base_size, num_chunks - extra_keys)
        end
    end
  end

  defp calculate_optimal_chunks(total_keys, target_size) do
    num_chunks = div(total_keys, target_size)
    remainder = total_keys - num_chunks * target_size

    final_num_chunks = if remainder >= div(target_size, 2), do: num_chunks + 1, else: num_chunks

    base_size = div(total_keys, final_num_chunks)

    if base_size + 1 > @default_max_keys_per_page do
      div(total_keys, @default_max_keys_per_page) + 1
    else
      final_num_chunks
    end
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
    case Map.get(index.page_map, page_id) do
      {_page, successor_id} ->
        update_predecessor_chain_pointer(index, page_id, successor_id)

      nil ->
        # Page doesn't exist, no chain update needed
        index
    end
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
      {_last_key, page_id, next_iter} ->
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

  # Helper function to calculate min/max keys efficiently
  defp calculate_key_bounds(tree, page_map) do
    if :gb_trees.is_empty(tree) do
      {<<0xFF, 0xFF>>, <<>>}
    else
      # Get max from tree structure (O(log n) operation)
      {max_tree_key, _max_id} = :gb_trees.largest(tree)

      # For min_key, we need to find the actual smallest first_key across all pages
      # This is a one-time calculation during load
      min_key = find_minimum_first_key(page_map)

      {min_key, max_tree_key}
    end
  end

  # Find the minimum first_key across all pages (only used during load)
  defp find_minimum_first_key(page_map) when map_size(page_map) == 0, do: <<0xFF, 0xFF>>

  defp find_minimum_first_key(page_map) do
    page_map
    |> Enum.map(fn {_page_id, {page, _next_id}} -> Page.left_key(page) end)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> <<0xFF, 0xFF>>
      keys -> Enum.min(keys)
    end
  end
end
