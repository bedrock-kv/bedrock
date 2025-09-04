defmodule Bedrock.DataPlane.Storage.Olivine.IndexUpdate do
  @moduledoc """
  Tracks mutation state during index updates.
  Contains the base index plus mutation tracking fields.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator

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
  def apply_mutations(%__MODULE__{version: version} = update, mutations, database) do
    Enum.reduce(mutations, update, fn mutation, update_acc ->
      apply_single_mutation(mutation, version, update_acc, database)
    end)
  end

  @doc """
  Process all pending operations for each modified page using sorted merge.
  """
  @spec process_pending_operations(t()) :: t()
  def process_pending_operations(%{pending_operations: pending_operations} = index_update) do
    # Process each page that has pending operations using sorted merge
    Enum.reduce(pending_operations, index_update, fn {page_id, operations}, index_update ->
      process_page_operations(index_update, page_id, operations)
    end)
  end

  @spec apply_single_mutation(Tx.mutation(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_single_mutation(mutation, new_version, update_data, database) do
    case mutation do
      {:set, key, value} ->
        apply_set_mutation(key, value, new_version, update_data, database)

      {:clear, key} ->
        apply_clear_mutation(key, new_version, update_data)

      {:clear_range, start_key, end_key} ->
        apply_range_clear_mutation(start_key, end_key, new_version, update_data)
    end
  end

  @spec apply_set_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_set_mutation(key, value, new_version, %__MODULE__{} = update_data, database) do
    target_page_id = Tree.page_for_insertion(update_data.index.tree, key)

    # Store the value in database (handles lookaside buffer internally)
    :ok = Database.store_value(database, key, new_version, value)

    # Add set operation to pending operations for this page (last-writer-wins)
    set_operation = {:set, new_version}

    updated_operations =
      Map.update(update_data.pending_operations, target_page_id, %{key => set_operation}, fn page_ops ->
        Map.put(page_ops, key, set_operation)
      end)

    %{update_data | pending_operations: updated_operations}
  end

  @spec apply_clear_mutation(binary(), Bedrock.version(), t()) :: t()
  defp apply_clear_mutation(key, _new_version, %__MODULE__{} = update_data) do
    case Tree.page_for_key(update_data.index.tree, key) do
      nil ->
        update_data

      page_id ->
        updated_operations =
          Map.update(update_data.pending_operations, page_id, %{key => :clear}, fn page_ops ->
            Map.put(page_ops, key, :clear)
          end)

        %{update_data | pending_operations: updated_operations}
    end
  end

  @spec apply_range_clear_mutation(binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_range_clear_mutation(start_key, end_key, _new_version, %__MODULE__{} = update_data) do
    update_data.index.tree
    |> Tree.page_ids_in_range(start_key, end_key)
    |> case do
      [] ->
        update_data

      [single_page_id] ->
        page = Index.get_page!(update_data.index, single_page_id)
        keys_to_clear = get_keys_in_range(page, start_key, end_key)

        %{
          update_data
          | pending_operations: add_clear_operations(update_data.pending_operations, single_page_id, keys_to_clear)
        }

      [first_page_id | remaining_page_ids] ->
        {middle_page_ids, [last_page_id]} = Enum.split(remaining_page_ids, -1)

        first_page = Index.get_page!(update_data.index, first_page_id)
        first_keys_to_clear = get_keys_in_range(first_page, start_key, Page.right_key(first_page))

        last_page = Index.get_page!(update_data.index, last_page_id)
        last_keys_to_clear = get_keys_in_range(last_page, Page.left_key(last_page), end_key)

        %{
          update_data
          | index: Index.delete_pages(update_data.index, middle_page_ids),
            page_allocator: PageAllocator.recycle_page_ids(update_data.page_allocator, middle_page_ids),
            pending_operations:
              update_data.pending_operations
              |> Map.drop(middle_page_ids)
              |> add_clear_operations(first_page_id, first_keys_to_clear)
              |> add_clear_operations(last_page_id, last_keys_to_clear)
        }
    end
  end

  # Helper function to get keys within a range from a page
  @spec get_keys_in_range(Page.t(), Bedrock.key(), Bedrock.key()) :: [Bedrock.key()]
  defp get_keys_in_range(page, start_key, end_key) do
    page
    |> Page.key_versions()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  # Helper function to add clear operations for keys on a specific page
  defp add_clear_operations(operations, _page_id, []), do: operations

  defp add_clear_operations(operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, operations, fn key, ops_acc ->
      Map.update(ops_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
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
  defp update_predecessor_chain(page_map, page_id_to_delete, deleted_next_id) do
    Enum.reduce(page_map, page_map, fn {id, page}, acc_map ->
      if Page.next_id(page) == page_id_to_delete do
        updated_page = Page.new(Page.id(page), Page.key_versions(page), deleted_next_id)
        Map.put(acc_map, id, updated_page)
      else
        acc_map
      end
    end)
  end

  # Helper functions for processing pending operations

  @spec process_page_operations(t(), Page.id(), %{Bedrock.key() => {:set, Bedrock.version()} | :clear}) :: t()
  defp process_page_operations(%__MODULE__{} = update_data, page_id, operations) do
    page = Index.get_page!(update_data.index, page_id)
    updated_page = Page.apply_operations(page, operations)

    cond do
      Page.empty?(updated_page) ->
        updated_index = fix_chain_before_delete(update_data.index, page_id)

        %{
          update_data
          | index: Index.delete_page(updated_index, page_id),
            page_allocator: PageAllocator.recycle_page_id(update_data.page_allocator, page_id)
        }

      Page.key_count(updated_page) > 256 ->
        # Allocate new page ID and split the page
        {right_page_id, updated_allocator} = PageAllocator.allocate_id(update_data.page_allocator)

        updated_index =
          update_data.index
          |> Index.delete_page(page_id)
          |> Index.split_page(updated_page, right_page_id)

        %{
          update_data
          | index: updated_index,
            page_allocator: updated_allocator,
            modified_page_ids:
              update_data.modified_page_ids
              # left page keeps original ID
              |> MapSet.put(page_id)
              |> MapSet.put(right_page_id)
        }

      true ->
        %{
          update_data
          | index: Index.update_page(update_data.index, page, updated_page),
            modified_page_ids: MapSet.put(update_data.modified_page_ids, page_id)
        }
    end
  end
end
