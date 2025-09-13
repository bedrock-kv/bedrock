defmodule Bedrock.DataPlane.Storage.Olivine.Index.TreeTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree

  describe "page_ids_in_range/3" do
    test "returns empty list for empty tree" do
      tree = :gb_trees.empty()
      assert Tree.page_ids_in_range(tree, "a", "z") == []
    end

    test "returns pages in lexicographic order by first_key" do
      # Create NON-OVERLAPPING pages in proper B-tree order:
      # Page 1: first_key="a", last_key="f"
      # Page 2: first_key="g", last_key="m"
      # Page 3: first_key="n", last_key="z"

      page1_kvs = [{"a", <<1::64>>}, {"f", <<2::64>>}]
      page2_kvs = [{"g", <<3::64>>}, {"m", <<4::64>>}]
      page3_kvs = [{"n", <<5::64>>}, {"z", <<6::64>>}]

      page1 = Page.new(1, page1_kvs, 0)
      page2 = Page.new(2, page2_kvs, 0)
      page3 = Page.new(3, page3_kvs, 0)

      # Add pages to tree (keyed by last_key: "f", "m", "z")
      tree =
        :gb_trees.empty()
        # keyed by "f"
        |> Tree.add_page_to_tree(page1)
        # keyed by "m"
        |> Tree.add_page_to_tree(page2)
        # keyed by "z"
        |> Tree.add_page_to_tree(page3)

      # Range query should return pages ordered by first_key: "a" < "g" < "n"
      # With non-overlapping pages, this should be the same as last_key order: "f" < "m" < "z"
      result = Tree.page_ids_in_range(tree, "a", "z")
      # Both orderings are the same for non-overlapping pages
      expected_order = [1, 2, 3]

      assert result == expected_order, """
      Pages should be returned in lexicographic order.
      Expected: #{inspect(expected_order)} (first_keys: "a", "g", "n")
      Got: #{inspect(result)}
      """
    end

    test "handles partial overlaps correctly" do
      # Create overlapping pages:
      # Page 1: "a" to "f"
      # Page 2: "d" to "h"
      # Page 3: "g" to "k"

      page1_kvs = [{"a", <<1::64>>}, {"f", <<2::64>>}]
      page2_kvs = [{"d", <<3::64>>}, {"h", <<4::64>>}]
      page3_kvs = [{"g", <<5::64>>}, {"k", <<6::64>>}]

      page1 = Page.new(1, page1_kvs, 0)
      page2 = Page.new(2, page2_kvs, 0)
      page3 = Page.new(3, page3_kvs, 0)

      tree =
        :gb_trees.empty()
        |> Tree.add_page_to_tree(page1)
        |> Tree.add_page_to_tree(page2)
        |> Tree.add_page_to_tree(page3)

      # Query "b" to "i" should include all pages, ordered by first_key
      result = Tree.page_ids_in_range(tree, "b", "i")
      # first_keys: "a", "d", "g"
      expected = [1, 2, 3]

      assert result == expected
    end

    test "excludes pages outside query range" do
      page1_kvs = [{"a", <<1::64>>}, {"b", <<2::64>>}]
      page2_kvs = [{"m", <<3::64>>}, {"n", <<4::64>>}]
      page3_kvs = [{"y", <<5::64>>}, {"z", <<6::64>>}]

      page1 = Page.new(1, page1_kvs, 0)
      page2 = Page.new(2, page2_kvs, 0)
      page3 = Page.new(3, page3_kvs, 0)

      tree =
        :gb_trees.empty()
        |> Tree.add_page_to_tree(page1)
        |> Tree.add_page_to_tree(page2)
        |> Tree.add_page_to_tree(page3)

      # Query "d" to "x" should only include page 2
      result = Tree.page_ids_in_range(tree, "d", "x")
      assert result == [2]

      # Query "c" to "e" should NOT include any pages (no overlap)
      result = Tree.page_ids_in_range(tree, "c", "e")
      assert result == []
    end

    test "handles complex overlapping scenario" do
      # Create a scenario similar to the original bug report:
      # Mix of "class" and "attends" subspaces that should be properly ordered

      # Simulate subspace-prefixed keys
      # 'c' for "class"
      class_prefix = <<0x63>>
      # 'a' for "attends"
      attends_prefix = <<0x61>>

      # Page 1: class keys (should come after attends in ordering)
      page1_kvs = [
        {class_prefix <> "001", <<1::64>>},
        {class_prefix <> "099", <<2::64>>}
      ]

      # Page 2: attends keys (should come before class in ordering)
      page2_kvs = [
        {attends_prefix <> "001", <<3::64>>},
        {attends_prefix <> "099", <<4::64>>}
      ]

      # Page 3: more class keys
      page3_kvs = [
        {class_prefix <> "100", <<5::64>>},
        {class_prefix <> "199", <<6::64>>}
      ]

      page1 = Page.new(1, page1_kvs, 0)
      page2 = Page.new(2, page2_kvs, 0)
      page3 = Page.new(3, page3_kvs, 0)

      tree =
        :gb_trees.empty()
        |> Tree.add_page_to_tree(page1)
        |> Tree.add_page_to_tree(page2)
        |> Tree.add_page_to_tree(page3)

      # Query entire range - should get attends first, then class pages
      result = Tree.page_ids_in_range(tree, <<>>, <<0xFF>>)
      # attends (0x61), then class (0x63) pages
      expected = [2, 1, 3]

      assert result == expected, """
      Pages should be ordered by first_key lexicographically.
      Expected: #{inspect(expected)} (attends page first, then class pages)
      Got: #{inspect(result)}
      """
    end

    test "handles single page correctly" do
      page_kvs = [{"key1", <<1::64>>}, {"key2", <<2::64>>}]
      page = Page.new(42, page_kvs, 0)

      tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

      result = Tree.page_ids_in_range(tree, "key1", "key3")
      assert result == [42]

      # Outside range
      result = Tree.page_ids_in_range(tree, "a", "b")
      assert result == []

      result = Tree.page_ids_in_range(tree, "z", "zz")
      assert result == []
    end

    test "handles empty pages (pages with no keys)" do
      # Empty page should not appear in tree or results
      empty_page = Page.new(1, [], 0)
      regular_page = Page.new(2, [{"key", <<1::64>>}], 0)

      tree =
        :gb_trees.empty()
        # Should not be added
        |> Tree.add_page_to_tree(empty_page)
        |> Tree.add_page_to_tree(regular_page)

      result = Tree.page_ids_in_range(tree, "a", "z")
      assert result == [2]
    end
  end

  describe "page_for_insertion/2" do
    test "returns correct page for key insertion" do
      page1_kvs = [{"a", <<1::64>>}, {"f", <<2::64>>}]
      page2_kvs = [{"g", <<3::64>>}, {"m", <<4::64>>}]

      page1 = Page.new(1, page1_kvs, 0)
      page2 = Page.new(2, page2_kvs, 0)

      tree =
        :gb_trees.empty()
        |> Tree.add_page_to_tree(page1)
        |> Tree.add_page_to_tree(page2)

      # Key "c" should go to page 1 (contains "a" to "f")
      assert Tree.page_for_insertion(tree, "c") == 1

      # Key "j" should go to page 2 (contains "g" to "m")
      assert Tree.page_for_insertion(tree, "j") == 2

      # Key "z" (beyond all pages) should go to rightmost page
      assert Tree.page_for_insertion(tree, "z") == 2
    end
  end

  describe "add_page_to_tree/2" do
    test "adds page correctly to tree" do
      page_kvs = [{"key1", <<1::64>>}, {"key3", <<2::64>>}]
      page = Page.new(1, page_kvs, 0)

      tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

      # Should be able to find the page for keys in its range
      assert Tree.page_for_key(tree, "key1") == 1
      assert Tree.page_for_key(tree, "key2") == 1
      assert Tree.page_for_key(tree, "key3") == 1

      # Should not find it for keys outside its range
      assert Tree.page_for_key(tree, "key0") == nil
      assert Tree.page_for_key(tree, "key4") == nil
    end
  end

  describe "Index.split_page/3" do
    test "properly removes original page when splitting to avoid key conflicts" do
      # Create a page that will be split
      original_page_kvs = [
        {"key01", <<1::64>>},
        {"key02", <<2::64>>},
        {"key03", <<3::64>>},
        {"key04", <<4::64>>}
      ]

      original_page = Page.new(1, original_page_kvs, 0)

      # Add page to index
      index = Index.add_page(Index.new(), original_page)

      # Verify initial state
      # One page in tree
      assert elem(index.tree, 0) == 1
      # Page 0 (empty) + page 1
      assert map_size(index.page_map) == 2

      # This should not raise an error - currently it raises {:key_exists, "key04"}
      split_index = Index.split_page(index, original_page, 2)

      # After split, should have 2 pages in tree (left + right, original removed)
      assert elem(split_index.tree, 0) == 2

      # Should have 3 pages in page_map (page 0 + left page 1 + right page 2)
      assert map_size(split_index.page_map) == 3

      # Verify pages are properly ordered
      page_ids = Tree.page_ids_in_range(split_index.tree, "", "zzz")
      # Left page (1), then right page (2)
      assert page_ids == [1, 2]
    end
  end
end
