defmodule Bedrock.DataPlane.Materializer.Olivine.IndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree

  # Helper to create a locator (8-byte binary)
  defp locator(n), do: <<n::unsigned-big-64>>

  # Helper to build an index with pages
  defp build_index_with_pages(page_tuples) do
    # page_tuples is a list of {page_id, kvs, next_id}
    page_map =
      Enum.reduce(page_tuples, %{}, fn {page_id, kvs, next_id}, acc ->
        page = Page.new(page_id, kvs)
        Map.put(acc, page_id, {page, next_id})
      end)

    tree =
      Enum.reduce(page_tuples, :gb_trees.empty(), fn {page_id, kvs, _next_id}, tree_acc ->
        page = Page.new(page_id, kvs)
        Tree.add_page_to_tree(tree_acc, page)
      end)

    %Index{
      tree: tree,
      page_map: page_map,
      min_key: <<>>,
      max_key: <<0xFF, 0xFF>>
    }
  end

  describe "delete_pages/2" do
    test "deletes single page from index" do
      # Create an index with multiple pages
      # page_id, kvs, next_id
      index =
        build_index_with_pages([
          {0, [], 1},
          {1, [{"key1", locator(1)}], 2},
          {2, [{"key5", locator(2)}], 3},
          {3, [{"key9", locator(3)}], 0}
        ])

      # Verify page exists
      assert Map.has_key?(index.page_map, 2)

      # Delete page 2
      updated_index = Index.delete_pages(index, [2])

      # Verify page is gone
      refute Map.has_key?(updated_index.page_map, 2)
      assert Map.has_key?(updated_index.page_map, 1)
      assert Map.has_key?(updated_index.page_map, 3)
    end

    test "deletes multiple pages from index" do
      index =
        build_index_with_pages([
          {0, [], 1},
          {1, [{"key1", locator(1)}], 2},
          {2, [{"key5", locator(2)}], 3},
          {3, [{"key7", locator(3)}], 4},
          {4, [{"key9", locator(4)}], 0}
        ])

      # Delete multiple pages
      updated_index = Index.delete_pages(index, [2, 3])

      # Verify pages are gone
      refute Map.has_key?(updated_index.page_map, 2)
      refute Map.has_key?(updated_index.page_map, 3)
      assert Map.has_key?(updated_index.page_map, 1)
      assert Map.has_key?(updated_index.page_map, 4)
    end

    test "handles deleting non-existent page gracefully" do
      index = build_index_with_pages([{0, [], 1}, {1, [{"key1", locator(1)}], 0}])

      # Try to delete non-existent page
      updated_index = Index.delete_pages(index, [999])

      # Index should be unchanged
      assert updated_index.page_map == index.page_map
    end

    test "handles empty page list" do
      index = build_index_with_pages([{0, [], 1}, {1, [{"key1", locator(1)}], 0}])

      # Delete empty list
      updated_index = Index.delete_pages(index, [])

      # Index should be unchanged
      assert updated_index.page_map == index.page_map
    end

    test "updates page chain when deleting middle page" do
      # Create a chain: 0 -> 1 -> 2 -> 3
      index =
        build_index_with_pages([
          {0, [], 1},
          {1, [{"key1", locator(1)}], 2},
          {2, [{"key5", locator(2)}], 3},
          {3, [{"key9", locator(3)}], 0}
        ])

      # Delete middle page 2
      updated_index = Index.delete_pages(index, [2])

      # Verify page 2 is gone
      refute Map.has_key?(updated_index.page_map, 2)

      # Verify chain is updated: page1 should now point to page3
      {_page1, next_id} = Map.get(updated_index.page_map, 1)
      assert next_id == 3
    end

    test "deletes leftmost page and updates page 0 pointer" do
      # Create a chain where page 0 points to page 1
      index =
        build_index_with_pages([
          {0, [], 1},
          {1, [{"key1", locator(1)}], 2},
          {2, [{"key5", locator(2)}], 0}
        ])

      # Verify page 0 points to page 1
      {_page0, leftmost_id} = Map.get(index.page_map, 0)
      assert leftmost_id == 1

      # Delete leftmost page (page 1)
      updated_index = Index.delete_pages(index, [1])

      # Verify page 0 now points to page 2
      {_page0, new_leftmost_id} = Map.get(updated_index.page_map, 0)
      assert new_leftmost_id == 2
    end
  end
end
