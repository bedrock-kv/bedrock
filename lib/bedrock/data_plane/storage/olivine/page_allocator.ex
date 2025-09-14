defmodule Bedrock.DataPlane.Storage.Olivine.PageAllocator do
  @moduledoc """
  Manages page ID allocation during index updates.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @type t :: %__MODULE__{
          max_page_id: Page.id(),
          free_page_ids: [Page.id()]
        }

  defstruct [:max_page_id, :free_page_ids]

  @spec new(Page.id(), [Page.id()]) :: t()
  def new(max_page_id, free_page_ids) do
    %__MODULE__{max_page_id: max_page_id, free_page_ids: free_page_ids}
  end

  @spec allocate_id(t()) :: {Page.id(), t()}
  def allocate_id(%__MODULE__{free_page_ids: [id | rest]} = allocator) do
    {id, %{allocator | free_page_ids: rest}}
  end

  def allocate_id(%__MODULE__{free_page_ids: [], max_page_id: max_id} = allocator) do
    new_id = max_id + 1
    {new_id, %{allocator | max_page_id: new_id}}
  end

  @spec allocate_ids(t(), non_neg_integer()) :: {[Page.id()], t()}
  def allocate_ids(allocator, count) when count <= 0, do: {[], allocator}

  def allocate_ids(allocator, count) do
    1..count
    |> Enum.reduce({[], allocator}, fn _, {ids_acc, allocator_acc} ->
      {new_id, updated_allocator} = allocate_id(allocator_acc)
      {[new_id | ids_acc], updated_allocator}
    end)
    |> then(fn {ids, final_allocator} -> {Enum.reverse(ids), final_allocator} end)
  end

  @spec recycle_page_id(t(), Page.id()) :: t()
  def recycle_page_id(%__MODULE__{free_page_ids: free_ids} = allocator, page_id),
    do: %{allocator | free_page_ids: [page_id | free_ids]}

  @spec recycle_page_ids(t(), [Page.id()]) :: t()
  def recycle_page_ids(%__MODULE__{free_page_ids: free_ids} = allocator, page_ids),
    do: %{allocator | free_page_ids: page_ids ++ free_ids}
end
