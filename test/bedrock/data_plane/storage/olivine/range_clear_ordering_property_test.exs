defmodule Bedrock.DataPlane.Storage.Olivine.RangeClearOrderingPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.Test.Storage.Olivine.InvariantChecks

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.DataPlane.Version

  @min_keys 100
  @max_keys 300

  describe "range clearing preserves key ordering invariants" do
    property "key ordering is maintained after random range clears and page splits" do
      check all(
              keys <- unique_keys(@min_keys, @max_keys),
              clear_ranges <- clear_ranges_from_keys(keys),
              max_runs: 20
            ) do
        # Build initial index with all keys (no database needed)
        {index, allocator} = build_index_from_keys(keys)

        # Verify initial state is ordered
        assert_keys_ordered(index, "Initial state")

        # Apply range clear operations one by one, checking invariants after each
        final_state = apply_clear_ranges_with_checks(index, allocator, clear_ranges)

        # Calculate expected remaining keys after all clears
        expected_remaining_keys = calculate_remaining_keys_after_clears(keys, clear_ranges)

        # Final verification
        final_index = elem(final_state, 0)
        assert_keys_ordered(final_index, "Final state")
        assert_key_completeness(final_index, expected_remaining_keys)
        assert_all_invariants_comprehensive(final_index)
      end
    end

    property "tree structure remains consistent after complex operations" do
      check all(
              # Smaller for faster testing
              keys <- unique_keys(50, 150),
              operations <- mixed_operations_from_keys(keys),
              max_runs: 10
            ) do
        # Build initial index
        {index, allocator} = build_index_from_keys(keys)

        # Apply mixed operations (only clear_range, no sets to avoid database dependency)
        version = Version.from_bytes(<<System.system_time(:microsecond)::64>>)
        index_update = IndexUpdate.new(index, version, allocator)

        # Filter to only clear_range operations
        clear_operations = Enum.filter(operations, &match?({:clear_range, _, _}, &1))

        final_index_update =
          index_update
          |> apply_clear_mutations_only(clear_operations)
          |> IndexUpdate.process_pending_operations()

        {final_index, _} = IndexUpdate.finish(final_index_update)

        # Verify all invariants
        assert_keys_ordered(final_index, "After mixed operations")
        assert_all_invariants_comprehensive(final_index)
      end
    end
  end

  # Generators

  defp unique_keys(min, max) do
    # Generate keys with more structured format to avoid duplicates
    gen all(
          count <- integer(min..max),
          key_indices <- uniq_list_of(integer(0..99_999), length: count)
        ) do
      key_indices
      # Convert to 4-byte binary
      |> Enum.map(&<<&1::32>>)
      |> Enum.sort()
    end
  end

  defp clear_ranges_from_keys(keys) do
    gen all(
          num_ranges <- integer(1..min(10, div(length(keys), 50))),
          ranges <- list_of(range_from_keys(keys), length: num_ranges)
        ) do
      ranges
    end
  end

  defp range_from_keys(keys) do
    gen all(
          start_idx <- integer(0..(length(keys) - 1)),
          end_idx <- integer(start_idx..(length(keys) - 1))
        ) do
      start_key = Enum.at(keys, start_idx)
      end_key = Enum.at(keys, end_idx)
      {start_key, end_key}
    end
  end

  defp mixed_operations_from_keys(keys) do
    gen all(
          num_ops <- integer(5..20),
          operations <- list_of(operation_from_keys(keys), length: num_ops)
        ) do
      operations
    end
  end

  defp operation_from_keys(keys) do
    one_of([
      # Set operations (add new keys with structured format)
      gen all(
            # Use high values to avoid collision
            idx <- integer(100_000..199_999),
            value <- binary(min_length: 1, max_length: 100)
          ) do
        {:set, <<idx::32>>, value}
      end,
      # Clear range operations
      gen all({start_key, end_key} <- range_from_keys(keys)) do
        {:clear_range, start_key, end_key}
      end
    ])
  end

  # Helper functions

  defp build_index_from_keys(keys) do
    base_version = Version.zero()

    # Create key-version pairs
    key_versions = Enum.map(keys, &{&1, base_version})

    # Build pages with controlled size to ensure multiple pages
    pages = build_pages_from_key_versions(key_versions, 1)

    # Build index
    index = Enum.reduce(pages, Index.new(), &Index.add_page(&2, &1))

    # Create allocator with next available ID
    max_page_id = pages |> Enum.map(&Page.id/1) |> Enum.max(fn -> 0 end)
    allocator = PageAllocator.new(max_page_id, [])

    {index, allocator}
  end

  defp build_pages_from_key_versions(key_versions, start_id) do
    # Split into chunks of max 200 keys per page to ensure multiple pages
    key_versions
    |> Enum.chunk_every(200)
    |> Enum.with_index(start_id)
    |> Enum.map(fn {chunk, page_id} ->
      next_id = if page_id == start_id + div(length(key_versions), 200), do: 0, else: page_id + 1
      Page.new(page_id, chunk, next_id)
    end)
  end

  defp apply_clear_ranges_with_checks(index, allocator, clear_ranges) do
    Enum.reduce(clear_ranges, {index, allocator}, fn {start_key, end_key}, {current_index, current_allocator} ->
      version = Version.from_bytes(<<System.system_time(:microsecond)::64>>)

      # Apply single clear range
      index_update = IndexUpdate.new(current_index, version, current_allocator)

      updated_index_update =
        index_update
        |> apply_clear_mutations_only([{:clear_range, start_key, end_key}])
        |> IndexUpdate.process_pending_operations()

      {new_index, new_allocator} = IndexUpdate.finish(updated_index_update)

      # Check invariants after this operation
      assert_keys_ordered(
        new_index,
        "After clearing range #{inspect(start_key, base: :hex)}..#{inspect(end_key, base: :hex)}"
      )

      assert_all_invariants_comprehensive(new_index)

      {new_index, new_allocator}
    end)
  end

  # Apply only clear mutations without database dependency
  defp apply_clear_mutations_only(index_update, clear_mutations) do
    Enum.reduce(clear_mutations, index_update, fn
      {:clear_range, start_key, end_key}, acc ->
        apply_range_clear_without_db(acc, start_key, end_key)

      # Skip non-clear operations
      _, acc ->
        acc
    end)
  end

  defp apply_range_clear_without_db(index_update, start_key, end_key) do
    # Simplified version of apply_range_clear_mutation that doesn't need database
    pages_in_range = Index.Tree.page_ids_in_range(index_update.index.tree, start_key, end_key)

    case pages_in_range do
      [] ->
        index_update

      [single_page_id] ->
        page = Index.get_page!(index_update.index, single_page_id)
        keys_to_clear = extract_keys_in_range(page, start_key, end_key)

        clear_ops = Map.new(keys_to_clear, &{&1, :clear})
        updated_operations = Map.put(index_update.pending_operations, single_page_id, clear_ops)

        %{index_update | pending_operations: updated_operations}

      [first_page_id | remaining_page_ids] ->
        {middle_page_ids, [last_page_id]} = Enum.split(remaining_page_ids, -1)

        first_page = Index.get_page!(index_update.index, first_page_id)
        first_keys_to_clear = extract_keys_in_range(first_page, start_key, Page.right_key(first_page) || end_key)

        last_page = Index.get_page!(index_update.index, last_page_id)
        last_keys_to_clear = extract_keys_in_range(last_page, Page.left_key(last_page) || start_key, end_key)

        %{
          index_update
          | index: Index.delete_pages(index_update.index, middle_page_ids),
            page_allocator: PageAllocator.recycle_page_ids(index_update.page_allocator, middle_page_ids),
            pending_operations:
              index_update.pending_operations
              |> Map.drop(middle_page_ids)
              |> add_clear_operations_for_keys(first_page_id, first_keys_to_clear)
              |> add_clear_operations_for_keys(last_page_id, last_keys_to_clear)
        }
    end
  end

  defp extract_keys_in_range(page, start_key, end_key) do
    page
    |> Page.key_versions()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  defp add_clear_operations_for_keys(operations, _page_id, []), do: operations

  defp add_clear_operations_for_keys(operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, operations, fn key, ops_acc ->
      Map.update(ops_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
    end)
  end

  # Assertion helpers

  defp assert_keys_ordered(index, context) do
    all_keys = extract_all_keys(index)
    sorted_keys = Enum.sort(all_keys)

    if all_keys != sorted_keys do
      {out_of_order_idx, prev_key, bad_key} = find_first_out_of_order(all_keys)

      flunk("""
      #{context}: Keys are out of order!
      First out-of-order key at index #{out_of_order_idx}:
      Previous key: #{inspect(prev_key, base: :hex)}
      Out-of-order key: #{inspect(bad_key, base: :hex)}

      All keys (first 10): #{inspect(Enum.take(all_keys, 10), base: :hex)}
      Expected (first 10): #{inspect(Enum.take(sorted_keys, 10), base: :hex)}
      """)
    end
  end

  # Helper functions that are still needed (not duplicated in InvariantChecks)

  defp calculate_remaining_keys_after_clears(initial_keys, clear_ranges) do
    Enum.reduce(clear_ranges, initial_keys, fn {start_key, end_key}, remaining_keys ->
      Enum.reject(remaining_keys, fn key ->
        key >= start_key and key <= end_key
      end)
    end)
  end

  defp extract_all_keys(index) do
    index.page_map
    |> Map.values()
    |> Enum.flat_map(&Page.keys/1)
  end

  defp find_first_out_of_order(keys) do
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
end

# NOTE: Removed all duplicated invariant checking functions - now using InvariantChecks module
