defmodule Bedrock.DataPlane.Materializer.Olivine.Index.TreeTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree

  describe "page_for_key/2" do
    test "returns correct page for key insertion" do
      page1_kvs = [{"a", <<1::64>>}, {"f", <<2::64>>}]
      page2_kvs = [{"g", <<3::64>>}, {"m", <<4::64>>}]

      page1 = Page.new(1, page1_kvs)
      page2 = Page.new(2, page2_kvs)

      tree =
        :gb_trees.empty()
        |> Tree.add_page_to_tree(page1)
        |> Tree.add_page_to_tree(page2)

      # Key "c" should go to page 1 (contains "a" to "f")
      assert Tree.page_for_key(tree, "c") == 1

      # Key "j" should go to page 2 (contains "g" to "m")
      assert Tree.page_for_key(tree, "j") == 2

      # Key "z" (beyond all pages) should go to rightmost page (always 0)
      assert Tree.page_for_key(tree, "z") == 0
    end
  end

  describe "add_page_to_tree/2" do
    test "adds page correctly to tree" do
      page_kvs = [{"key1", <<1::64>>}, {"key3", <<2::64>>}]
      page = Page.new(1, page_kvs)

      tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

      # Should be able to find the page for keys in its range
      assert Tree.page_for_key(tree, "key1") == 1
      assert Tree.page_for_key(tree, "key2") == 1
      assert Tree.page_for_key(tree, "key3") == 1

      # In gap-free design, all keys map to a page
      # "key0" would go in page 1
      assert Tree.page_for_key(tree, "key0") == 1
      # "key4" beyond all pages goes to rightmost (0)
      assert Tree.page_for_key(tree, "key4") == 0
    end

    test "adds page 0 with no keys to tree" do
      tree = :gb_trees.empty()
      # Empty page (id 0, no keys) - has nil right_key
      page = Page.new(0, [])

      result_tree = Tree.add_page_to_tree(tree, page)

      # Page 0 with no right key should be added at empty binary key
      assert :gb_trees.lookup(<<>>, result_tree) == {:value, 0}
    end

    test "does not add non-zero page with no keys" do
      tree = :gb_trees.empty()
      # Non-zero page ID with no keys (nil right_key) - should not be added
      page = Page.new(5, [])

      result_tree = Tree.add_page_to_tree(tree, page)

      # Tree should remain unchanged (no entry for this page)
      assert result_tree == tree
    end
  end

  describe "remove_page_from_tree/2" do
    test "removes page 0 with no keys from tree" do
      tree = :gb_trees.empty()
      page = Page.new(0, [])

      # First add it
      tree = Tree.add_page_to_tree(tree, page)
      assert :gb_trees.lookup(<<>>, tree) == {:value, 0}

      # Then remove it
      result_tree = Tree.remove_page_from_tree(tree, page)

      # Should be removed
      assert :gb_trees.lookup(<<>>, result_tree) == :none
    end

    test "does not remove non-zero page with no keys" do
      tree = :gb_trees.empty()
      # Add a regular entry first
      tree = :gb_trees.enter("some_key", 10, tree)

      # Try to remove a non-zero page with no keys (nil right_key)
      page = Page.new(5, [])

      result_tree = Tree.remove_page_from_tree(tree, page)

      # Tree should remain unchanged
      assert result_tree == tree
      assert :gb_trees.lookup("some_key", result_tree) == {:value, 10}
    end
  end
end
