defmodule Bedrock.DataPlane.Storage.Olivine.PageChainMaintenanceTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  describe "page chain maintenance" do
    test "add_page maintains page chain ordering" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>

      # Create pages with non-overlapping key ranges
      page1_kvs = for i <- 1..10, do: {<<i::32>>, base_version}
      page2_kvs = for i <- 11..20, do: {<<i::32>>, base_version}
      page3_kvs = for i <- 21..30, do: {<<i::32>>, base_version}

      # Will be updated by add_page
      page1 = Page.new(1, page1_kvs, 0)
      # Will be updated by add_page
      page2 = Page.new(2, page2_kvs, 0)
      # Will be updated by add_page
      page3 = Page.new(3, page3_kvs, 0)

      # Start with empty index
      index = Index.new()

      # Add pages one by one
      index_with_page1 = Index.add_page(index, page1)
      index_with_page2 = Index.add_page(index_with_page1, page2)
      final_index = Index.add_page(index_with_page2, page3)

      # Page 0 should point to the leftmost page (page 1)
      page_0 = Map.get(final_index.page_map, 0)
      leftmost_id = Page.next_id(page_0)
      assert leftmost_id == 1, "Page 0 should point to leftmost page 1, but points to #{leftmost_id}"

      # Follow the chain to verify ordering
      chain_order = follow_chain(final_index, leftmost_id)
      expected_order = [1, 2, 3]

      assert chain_order == expected_order,
             "Chain order should be #{inspect(expected_order)}, got #{inspect(chain_order)}"

      # Chain should terminate properly
      page_3 = Map.get(final_index.page_map, 3)
      assert Page.next_id(page_3) == 0, "Chain should terminate with next_id=0"

      # Chain order should match tree order
      tree_order = Index.Tree.page_ids_in_range(final_index.tree, <<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>)

      assert chain_order == tree_order,
             "Chain order #{inspect(chain_order)} should match tree order #{inspect(tree_order)}"
    end

    test "add_page handles single page correctly" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>
      page_kvs = for i <- 1..5, do: {<<i::32>>, base_version}
      page = Page.new(1, page_kvs, 0)

      index = Index.add_page(Index.new(), page)

      # Page 0 should point to page 1
      page_0 = Map.get(index.page_map, 0)
      assert Page.next_id(page_0) == 1

      # Page 1 should terminate the chain
      page_1 = Map.get(index.page_map, 1)
      assert Page.next_id(page_1) == 0
    end

    test "add_page handles inserting page in middle of existing chain" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>

      # Create pages with gaps to test insertion
      page1_kvs = for i <- 1..10, do: {<<i::32>>, base_version}
      page3_kvs = for i <- 31..40, do: {<<i::32>>, base_version}
      # Insert between 1 and 3
      page2_kvs = for i <- 11..20, do: {<<i::32>>, base_version}

      page1 = Page.new(1, page1_kvs, 0)
      page3 = Page.new(3, page3_kvs, 0)
      # Should be inserted between 1 and 3
      page2 = Page.new(2, page2_kvs, 0)

      # Add pages in non-sorted order
      index =
        Index.new()
        |> Index.add_page(page1)
        |> Index.add_page(page3)
        # This should be inserted in the middle
        |> Index.add_page(page2)

      # Chain should be in key order: 1 -> 2 -> 3 -> 0
      page_0 = Map.get(index.page_map, 0)
      assert Page.next_id(page_0) == 1

      page_1 = Map.get(index.page_map, 1)
      assert Page.next_id(page_1) == 2

      page_2 = Map.get(index.page_map, 2)
      assert Page.next_id(page_2) == 3

      page_3 = Map.get(index.page_map, 3)
      assert Page.next_id(page_3) == 0

      # Verify chain order matches tree order
      chain_order = follow_chain(index, 1)
      tree_order = Index.Tree.page_ids_in_range(index.tree, <<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>)

      assert chain_order == tree_order,
             "Chain order #{inspect(chain_order)} should match tree order #{inspect(tree_order)}"
    end
  end

  # Helper function to follow the chain and collect page IDs
  defp follow_chain(index, start_id) do
    follow_chain_recursive(index, start_id, [])
  end

  defp follow_chain_recursive(_index, 0, acc), do: Enum.reverse(acc)

  defp follow_chain_recursive(index, current_id, acc) do
    page = Map.get(index.page_map, current_id)
    next_id = Page.next_id(page)
    follow_chain_recursive(index, next_id, [current_id | acc])
  end
end
