defmodule Bedrock.DataPlane.Storage.Olivine.PageSplitTreeOrderingTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Storage.Olivine.InvariantChecks

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.Test.Storage.Olivine.IndexTestHelpers

  describe "page splitting maintains tree ordering" do
    test "page splitting maintains key ordering after distributed mutations" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>

      # Create initial pages with odd numbers (big-endian 16-bit keys)
      # This creates gaps between pages for even numbers to fill
      # 1,3,5,...,197
      odd_keys_page1 = for i <- 1..99, i = i * 2 - 1, do: {<<i::16>>, base_version}
      # 199,201,203,...,397
      odd_keys_page2 = for i <- 100..199, i = i * 2 - 1, do: {<<i::16>>, base_version}
      # 399,401,403,...,599
      odd_keys_page3 = for i <- 200..300, i = i * 2 - 1, do: {<<i::16>>, base_version}

      page1 = Page.new(1, odd_keys_page1)
      page2 = Page.new(2, odd_keys_page2)
      page3 = Page.new(3, odd_keys_page3)

      index = build_index_from_page_tuples([{page1, 2}, {page2, 3}, {page3, 0}])

      # Verify initial state is correct
      all_keys_before = extract_all_keys_in_order(index)
      assert all_keys_before == Enum.sort(all_keys_before), "Initial keys should be ordered"

      # Create even number keys and shuffle them to simulate real mutations
      # 2,4,6,...,600
      even_keys = for i <- 1..300, i = i * 2, do: i
      shuffled_even_keys = Enum.shuffle(even_keys)

      # Distribute even keys to their correct pages based on Tree.page_for_insertion
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>

      # Group operations by target page
      operations_by_page =
        Enum.reduce(shuffled_even_keys, %{}, fn key, acc ->
          key_binary = <<key::16>>
          target_page_id = Tree.page_for_insertion(index.tree, key_binary)
          operation = {key_binary, {:set, version}}

          Map.update(acc, target_page_id, [operation], &[operation | &1])
        end)

      # Convert to the format expected by pending_operations
      pending_operations =
        Map.new(operations_by_page, fn {page_id, operations} ->
          ops_map = Map.new(operations, fn {key, op} -> {key, op} end)
          {page_id, ops_map}
        end)

      # Apply distributed operations
      allocator = PageAllocator.new(3, [])
      index_update = IndexUpdate.new(index, version, allocator)

      index_update_with_ops = %{
        index_update
        | pending_operations: pending_operations
      }

      # Process all operations - this should trigger splits where needed
      final_index_update = IndexUpdate.process_pending_operations(index_update_with_ops)
      {final_index, _} = IndexUpdate.finish(final_index_update)

      # Verify all keys are still in perfect order
      all_keys_after = extract_all_keys_in_order(final_index)
      sorted_keys = Enum.sort(all_keys_after)

      if all_keys_after != sorted_keys do
        {bad_idx, prev_key, bad_key} = find_first_out_of_order_key(all_keys_after)

        flunk("""
        Page splitting after distributed mutations caused out-of-order keys!
        First out-of-order key at index #{bad_idx}:
        Previous key: #{inspect(prev_key, base: :hex)}
        Out-of-order key: #{inspect(bad_key, base: :hex)}

        Tree page order: #{inspect(Tree.page_ids_in_range(final_index.tree, <<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>))}
        """)
      end

      # Verify we have all expected keys (odds + evens)
      expected_keys = for i <- 1..600, do: <<i::16>>
      expected_sorted = Enum.sort(expected_keys)

      assert all_keys_after == expected_sorted, "Should have all keys 1-600 in order"
    end

    test "tree consistency is maintained after page operations" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>

      # Create pages with predictable key ranges
      # 'a' prefix
      attends_kvs = for i <- 1..100, do: {<<0x61, i::16>>, base_version}
      # 'c' prefix
      class_kvs = for i <- 1..100, do: {<<0x63, i::16>>, base_version}
      # 'b' prefix (will split)
      middle_kvs = for i <- 1..250, do: {<<0x62, i::16>>, base_version}

      attends_page = Page.new(1, attends_kvs)
      class_page = Page.new(2, class_kvs)
      middle_page = Page.new(3, middle_kvs)

      index = build_index_from_page_tuples([{attends_page, 2}, {class_page, 3}, {middle_page, 0}])

      # Apply a range clear that will trigger page splitting on the middle page
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      allocator = PageAllocator.new(3, [])
      index_update = IndexUpdate.new(index, version, allocator)

      # Add enough operations to trigger splitting
      extra_ops = for i <- 300..400, do: {<<0x62, i::16>>, {:set, version}}
      operations_for_page3 = Map.new(extra_ops)

      index_update_with_ops = %{
        index_update
        | pending_operations: %{3 => operations_for_page3}
      }

      final_index_update = IndexUpdate.process_pending_operations(index_update_with_ops)
      {final_index, _} = IndexUpdate.finish(final_index_update)

      # Verify we have all expected keys (original + added)
      initial_keys =
        List.flatten([
          # Extract keys from {key, version} tuples
          Enum.map(attends_kvs, &elem(&1, 0)),
          Enum.map(class_kvs, &elem(&1, 0)),
          Enum.map(middle_kvs, &elem(&1, 0))
        ])

      added_keys = Map.keys(operations_for_page3)
      expected_keys = Enum.sort(initial_keys ++ added_keys)

      actual_keys = extract_all_keys_in_order(final_index)

      assert actual_keys == expected_keys,
             "Key preservation failed: expected #{length(expected_keys)} keys, got #{length(actual_keys)} keys"

      # Use comprehensive invariant checks from centralized module
      assert_all_invariants(final_index)
    end

    test "large scale key preservation with 1710+ keys" do
      base_version = <<0, 0, 0, 0, 0, 0, 0, 0>>

      # Create 1710 keys similar to your demo
      levels = ["intro", "for dummies", "remedial", "101", "201", "301", "mastery", "lab", "seminar"]
      types = ["chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance"]
      times = for h <- 2..20, do: "#{h}:00"

      class_names = for i <- times, t <- types, l <- levels, do: "#{i} #{t} #{l}"

      # Convert to binary keys and create initial pages
      binary_keys = Enum.map(class_names, &:erlang.term_to_binary/1)
      _key_versions = Enum.map(binary_keys, &{&1, base_version})

      # Build index by adding keys in batches (simulating your insertion pattern)
      index = Index.new()
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      allocator = PageAllocator.new(0, [])

      # Process all keys through IndexUpdate to trigger realistic page splits
      index_update = IndexUpdate.new(index, version, allocator)

      operations = Map.new(binary_keys, &{&1, {:set, version}})
      index_update_with_ops = %{index_update | pending_operations: %{0 => operations}}

      final_index_update = IndexUpdate.process_pending_operations(index_update_with_ops)
      {final_index, _} = IndexUpdate.finish(final_index_update)

      # Verify we have ALL keys - this is the critical test
      actual_keys = extract_all_keys_in_order(final_index)
      expected_keys = Enum.sort(binary_keys)

      # Debug the index structure before asserting
      if length(actual_keys) != length(expected_keys) do
        debug_invariants(final_index)
      end

      assert actual_keys == expected_keys,
             "Key preservation failed with large dataset: expected #{length(expected_keys)} keys, got #{length(actual_keys)} keys"

      # Also run invariant checks
      assert_all_invariants(final_index)
    end
  end

  # Helper functions

  defp extract_all_keys_in_order(index) do
    # Get pages in tree order, then extract keys in that order
    index.tree
    |> Tree.page_ids_in_range(<<>>, <<0xFF, 0xFF, 0xFF, 0xFF>>)
    |> Enum.flat_map(fn page_id ->
      case Map.get(index.page_map, page_id) do
        nil -> []
        {page, _next_id} -> Page.keys(page)
      end
    end)
  end

  defp find_first_out_of_order_key(keys) do
    keys
    |> Enum.with_index()
    |> Enum.reduce_while(nil, fn {key, idx}, prev_key ->
      if prev_key && key < prev_key do
        {:halt, {idx, prev_key, key}}
      else
        {:cont, key}
      end
    end)
  end

  # Helper function to efficiently build index from page tuples
  defp build_index_from_page_tuples(page_tuples) do
    initial_index = Index.new()

    # Build page_map with all pages
    page_map =
      Enum.reduce(page_tuples, initial_index.page_map, fn {page, next_id}, acc_map ->
        Map.put(acc_map, Page.id(page), {page, next_id})
      end)

    # Build tree with all pages
    tree =
      Enum.reduce(page_tuples, initial_index.tree, fn {page, _next_id}, acc_tree ->
        Tree.add_page_to_tree(acc_tree, page)
      end)

    # Create temporary index and ensure chain consistency once at the end
    temp_index = %{initial_index | tree: tree, page_map: page_map}
    IndexTestHelpers.rebuild_page_chain_consistency(temp_index)
  end
end
