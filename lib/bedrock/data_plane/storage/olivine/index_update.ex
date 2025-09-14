defmodule Bedrock.DataPlane.Storage.Olivine.IndexUpdate do
  @moduledoc """
  Tracks mutation state during index updates.

  ## Mutation Processing

  Mutations are distributed to pages based on key ranges:
  1. **Key Distribution**: Use `Tree.page_for_insertion/2` to find target page
  2. **Batch Processing**: Group operations by page_id for efficiency
  3. **Page Operations**: Apply all operations to a page at once
  4. **Automatic Splitting**: Split pages exceeding 256 keys
  5. **Chain Maintenance**: Update page chains when pages are added/removed

  ## Process Flow

  1. `apply_set_mutation/5`: Determines target page, stores value, queues operation
  2. `apply_clear_mutation/3`: Queues clear operation for existing key
  3. `apply_range_clear_mutation/4`: Handles range clears across multiple pages
  4. `process_pending_operations/1`: Applies all queued operations
  5. `finish/1`: Returns final index and page allocator state

  ## Page 0 Protection

  Page 0 is never deleted, only updated. When page 0 becomes empty,
  it remains in the index to preserve the leftmost chain entry point.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.Internal.Atomics

  @type t :: %__MODULE__{
          index: Index.t(),
          version: Bedrock.version(),
          page_allocator: PageAllocator.t(),
          modified_page_ids: MapSet.t(Page.id()),
          pending_operations: %{Page.id() => %{Bedrock.key() => {:set, Bedrock.version()} | :clear}}
        }

  defstruct [
    :index,
    :version,
    :page_allocator,
    :modified_page_ids,
    :pending_operations
  ]

  @doc """
  Creates an IndexUpdate for mutation tracking from an Index, version, and page allocator.
  """
  @spec new(Index.t(), Bedrock.version(), PageAllocator.t()) :: t()
  def new(%Index{} = index, version, page_allocator) do
    %__MODULE__{
      index: index,
      version: version,
      page_allocator: page_allocator,
      modified_page_ids: MapSet.new(),
      pending_operations: %{}
    }
  end

  @doc """
  Finishes the IndexUpdate, returning the final Index and PageAllocator.
  """
  def finish(%__MODULE__{index: index, page_allocator: page_allocator}), do: {index, page_allocator}

  @doc """
  Gets the modified pages from the IndexUpdate as a list of pages.
  """
  @spec modified_pages(t()) :: [Page.t()]
  def modified_pages(%__MODULE__{index: index, modified_page_ids: modified_page_ids}),
    do: Enum.map(modified_page_ids, &Index.get_page!(index, &1))

  @doc """
  Stores all modified pages from the IndexUpdate in the database.
  Returns the IndexUpdate for chaining.
  """
  @spec store_modified_pages(t(), Database.t()) :: t()
  def store_modified_pages(%__MODULE__{version: version} = index_update, database) do
    pages = modified_pages(index_update)
    :ok = Database.store_modified_pages(database, version, pages)
    index_update
  end

  @doc """
  Applies mutations to this IndexUpdate, returning the updated IndexUpdate.
  """
  @spec apply_mutations(t(), Enumerable.t(Tx.mutation()), Database.t()) :: t()
  def apply_mutations(%__MODULE__{version: version} = mutation_tracker, mutations, database) do
    Enum.reduce(mutations, mutation_tracker, fn mutation, tracker_acc ->
      apply_single_mutation(mutation, version, tracker_acc, database)
    end)
  end

  @doc """
  Process all pending operations for each modified page using sorted merge.
  """
  @spec process_pending_operations(t()) :: t()
  def process_pending_operations(%{pending_operations: pending_operations} = mutation_tracker) do
    Enum.reduce(pending_operations, mutation_tracker, fn {page_id, page_mutations}, tracker_acc ->
      apply_mutations_to_page(tracker_acc, page_id, page_mutations)
    end)
  end

  @spec apply_single_mutation(Tx.mutation(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_single_mutation(mutation, target_version, mutation_tracker, database) do
    case mutation do
      {:set, key, value} ->
        apply_set_mutation(key, value, target_version, mutation_tracker, database)

      {:clear, key} ->
        apply_clear_mutation(key, target_version, mutation_tracker)

      {:clear_range, start_key, end_key} ->
        apply_range_clear_mutation(start_key, end_key, target_version, mutation_tracker)

      {:atomic, :add, key, value} ->
        apply_add_mutation(key, value, target_version, mutation_tracker, database)

      {:atomic, :min, key, value} ->
        apply_min_mutation(key, value, target_version, mutation_tracker, database)

      {:atomic, :max, key, value} ->
        apply_max_mutation(key, value, target_version, mutation_tracker, database)
    end
  end

  @spec apply_set_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_set_mutation(key, value, target_version, %__MODULE__{} = mutation_tracker, database) do
    insertion_page_id = Tree.page_for_insertion(mutation_tracker.index.tree, key)

    :ok = Database.store_value(database, key, target_version, value)

    set_operation = {:set, target_version}

    updated_pending_operations =
      Map.update(mutation_tracker.pending_operations, insertion_page_id, %{key => set_operation}, fn page_mutations ->
        Map.put(page_mutations, key, set_operation)
      end)

    %{mutation_tracker | pending_operations: updated_pending_operations}
  end

  @spec apply_clear_mutation(binary(), Bedrock.version(), t()) :: t()
  defp apply_clear_mutation(key, _target_version, %__MODULE__{} = mutation_tracker) do
    case Tree.page_for_key(mutation_tracker.index.tree, key) do
      nil ->
        mutation_tracker

      containing_page_id ->
        updated_pending_operations =
          Map.update(mutation_tracker.pending_operations, containing_page_id, %{key => :clear}, fn page_mutations ->
            Map.put(page_mutations, key, :clear)
          end)

        %{mutation_tracker | pending_operations: updated_pending_operations}
    end
  end

  @spec apply_range_clear_mutation(binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_range_clear_mutation(start_key, end_key, _target_version, %__MODULE__{} = mutation_tracker) do
    case collect_range_pages_via_chain_following(mutation_tracker.index, start_key, end_key) do
      [] ->
        mutation_tracker

      [single_page_id] ->
        page = Index.get_page!(mutation_tracker.index, single_page_id)
        keys_to_clear = extract_keys_in_range(page, start_key, end_key)

        %{
          mutation_tracker
          | pending_operations:
              add_clear_operations_for_keys(mutation_tracker.pending_operations, single_page_id, keys_to_clear)
        }

      [first_page_id | remaining_page_ids] ->
        {middle_page_ids, [last_page_id]} = Enum.split(remaining_page_ids, -1)

        first_page = Index.get_page!(mutation_tracker.index, first_page_id)
        first_keys_to_clear = extract_keys_in_range(first_page, start_key, Page.right_key(first_page))

        last_page = Index.get_page!(mutation_tracker.index, last_page_id)
        last_keys_to_clear = extract_keys_in_range(last_page, Page.left_key(last_page), end_key)

        %{
          mutation_tracker
          | index: Index.delete_pages(mutation_tracker.index, middle_page_ids),
            page_allocator: PageAllocator.recycle_page_ids(mutation_tracker.page_allocator, middle_page_ids),
            pending_operations:
              mutation_tracker.pending_operations
              |> Map.drop(middle_page_ids)
              |> add_clear_operations_for_keys(first_page_id, first_keys_to_clear)
              |> add_clear_operations_for_keys(last_page_id, last_keys_to_clear)
        }
    end
  end

  @spec collect_range_pages_via_chain_following(Index.t(), binary(), binary()) :: [Page.id()]
  defp collect_range_pages_via_chain_following(index, start_key, end_key) do
    first_page_id = Tree.page_for_insertion(index.tree, start_key)
    follow_chain_collecting_range_pages(index.page_map, first_page_id, start_key, end_key, [])
  end

  defp follow_chain_collecting_range_pages(page_map, current_page_id, start_key, end_key, collected_page_ids) do
    case Map.get(page_map, current_page_id) do
      nil -> Enum.reverse(collected_page_ids)
      current_page -> process_page_in_range(page_map, current_page, start_key, end_key, collected_page_ids)
    end
  end

  defp process_page_in_range(page_map, current_page, start_key, end_key, collected_page_ids) do
    page_first_key = Page.left_key(current_page)
    page_last_key = Page.right_key(current_page)

    cond do
      page_entirely_before_range?(page_last_key, start_key) ->
        continue_to_next_page(page_map, current_page, start_key, end_key, collected_page_ids)

      page_entirely_after_range?(page_first_key, end_key) ->
        Enum.reverse(collected_page_ids)

      true ->
        include_page_and_continue(page_map, current_page, start_key, end_key, collected_page_ids)
    end
  end

  defp page_entirely_before_range?(page_last_key, start_key) do
    page_last_key != nil and page_last_key < start_key
  end

  defp page_entirely_after_range?(page_first_key, end_key) do
    page_first_key != nil and page_first_key > end_key
  end

  defp continue_to_next_page(page_map, current_page, start_key, end_key, collected_page_ids) do
    next_id = Page.next_id(current_page)

    if next_id == 0 do
      Enum.reverse(collected_page_ids)
    else
      follow_chain_collecting_range_pages(page_map, next_id, start_key, end_key, collected_page_ids)
    end
  end

  defp include_page_and_continue(page_map, current_page, start_key, end_key, collected_page_ids) do
    current_page_id = Page.id(current_page)
    next_id = Page.next_id(current_page)
    updated_collection = [current_page_id | collected_page_ids]

    if next_id == 0 do
      Enum.reverse(updated_collection)
    else
      follow_chain_collecting_range_pages(page_map, next_id, start_key, end_key, updated_collection)
    end
  end

  @spec apply_add_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_add_mutation(key, value, target_version, %__MODULE__{} = mutation_tracker, database) do
    current_value = get_current_value_for_atomic_op(mutation_tracker, database, key, target_version)
    sum_value = Atomics.add(current_value, value)
    apply_set_mutation(key, sum_value, target_version, mutation_tracker, database)
  end

  @spec apply_min_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_min_mutation(key, value, target_version, %__MODULE__{} = mutation_tracker, database) do
    current_value = get_current_value_for_atomic_op(mutation_tracker, database, key, target_version)
    minimum_value = Atomics.min(current_value, value)
    apply_set_mutation(key, minimum_value, target_version, mutation_tracker, database)
  end

  @spec apply_max_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_max_mutation(key, value, target_version, %__MODULE__{} = mutation_tracker, database) do
    current_value = get_current_value_for_atomic_op(mutation_tracker, database, key, target_version)
    maximum_value = Atomics.max(current_value, value)
    apply_set_mutation(key, maximum_value, target_version, mutation_tracker, database)
  end

  @spec extract_keys_in_range(Page.t(), Bedrock.key(), Bedrock.key()) :: [Bedrock.key()]
  defp extract_keys_in_range(page, start_key, end_key) do
    page
    |> Page.key_versions()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  defp add_clear_operations_for_keys(pending_operations, _page_id, []), do: pending_operations

  defp add_clear_operations_for_keys(pending_operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, pending_operations, fn key, operations_acc ->
      Map.update(operations_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
    end)
  end

  @spec fix_chain_before_delete(Index.t(), Page.id()) :: Index.t()
  defp fix_chain_before_delete(%Index{page_map: page_map} = index, page_id_to_delete) do
    case Map.fetch(page_map, page_id_to_delete) do
      {:ok, page_to_delete} ->
        deleted_next_id = Page.next_id(page_to_delete)
        updated_page_map = update_predecessor_chain(page_map, page_id_to_delete, deleted_next_id)
        %{index | page_map: updated_page_map}

      :error ->
        index
    end
  end

  @spec update_predecessor_chain(map(), Page.id(), Page.id()) :: map()
  defp update_predecessor_chain(page_map, page_to_delete_id, successor_page_id) do
    Enum.reduce(page_map, page_map, fn {id, page}, updated_page_map ->
      if Page.next_id(page) == page_to_delete_id do
        page_with_updated_chain = Page.new(Page.id(page), Page.key_versions(page), successor_page_id)
        Map.put(updated_page_map, id, page_with_updated_chain)
      else
        updated_page_map
      end
    end)
  end

  @spec apply_mutations_to_page(t(), Page.id(), %{Bedrock.key() => {:set, Bedrock.version()} | :clear}) :: t()
  defp apply_mutations_to_page(%__MODULE__{} = mutation_tracker, page_id, page_mutations) do
    page = Index.get_page!(mutation_tracker.index, page_id)
    updated_page = Page.apply_operations(page, page_mutations)

    cond do
      Page.empty?(updated_page) ->
        if page_id == 0 do
          %{mutation_tracker | index: Index.update_page(mutation_tracker.index, page, updated_page)}
        else
          index_with_fixed_chain = fix_chain_before_delete(mutation_tracker.index, page_id)

          %{
            mutation_tracker
            | index: Index.delete_page(index_with_fixed_chain, page_id),
              page_allocator: PageAllocator.recycle_page_id(mutation_tracker.page_allocator, page_id)
          }
        end

      Page.key_count(updated_page) > 256 ->
        key_count = Page.key_count(updated_page)
        pages_needed = div(key_count - 1, 256) + 1
        additional_pages_needed = pages_needed - 1

        {new_page_ids, allocator_after_allocation} =
          PageAllocator.allocate_ids(mutation_tracker.page_allocator, additional_pages_needed)

        index_after_split = Index.multi_split_page(mutation_tracker.index, page, updated_page, new_page_ids)

        all_modified_page_ids = [page_id | new_page_ids]

        %{
          mutation_tracker
          | index: index_after_split,
            page_allocator: allocator_after_allocation,
            modified_page_ids:
              Enum.reduce(all_modified_page_ids, mutation_tracker.modified_page_ids, &MapSet.put(&2, &1))
        }

      true ->
        %{
          mutation_tracker
          | index: Index.update_page(mutation_tracker.index, page, updated_page),
            modified_page_ids: MapSet.put(mutation_tracker.modified_page_ids, page_id)
        }
    end
  end

  @spec get_current_value_for_atomic_op(t(), Database.t(), Bedrock.key(), Bedrock.version()) :: binary()
  defp get_current_value_for_atomic_op(mutation_tracker, database, key, _target_version) do
    case find_key_in_index(mutation_tracker.index, key) do
      {:ok, _page, key_version} ->
        case Database.load_value(database, key, key_version) do
          {:ok, value} when is_binary(value) ->
            value

          {:error, :not_found} ->
            <<>>
        end

      {:error, :not_found} ->
        <<>>
    end
  rescue
    _ -> <<>>
  end

  @spec find_key_in_index(Index.t(), Bedrock.key()) ::
          {:ok, Page.t(), Bedrock.version()} | {:error, :not_found}
  defp find_key_in_index(index, key) do
    case Index.page_for_key(index, key) do
      {:ok, page} ->
        case Page.version_for_key(page, key) do
          {:ok, version} -> {:ok, page, version}
          {:error, :not_found} -> {:error, :not_found}
        end

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
