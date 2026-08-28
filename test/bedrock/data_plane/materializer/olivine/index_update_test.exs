defmodule Bedrock.DataPlane.Materializer.Olivine.IndexUpdateTest do
  @moduledoc """
  Behavioral tests for IndexUpdate mutation processing: pending-operation
  merging, multi-page range clears, page 0 protection, chain-following edge
  cases, empty-page handling, statistics tracking, and atomic mutations.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IdAllocator
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree
  alias Bedrock.DataPlane.Materializer.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Materializer.Olivine.IndexTestHelpers

  setup do
    temp_dir = System.tmp_dir!() <> "/test_index_update_#{System.unique_integer()}"
    File.mkdir_p!(temp_dir)

    db_file_path = Path.join(temp_dir, "index_update_test.dets")
    {:ok, database} = Database.open(:"test_db_#{System.unique_integer()}", db_file_path)

    on_exit(fn ->
      :ok = Database.close(database)
      File.rm_rf!(temp_dir)
    end)

    {:ok, database: database}
  end

  # Builds an index from a page spec like [{1, ["a", "b"]}, {2, ["c", "d"]}].
  # Page ids may include 0 to place keys directly on page 0. Pages are entered
  # into the tree in key order and the chain is rebuilt for consistency.
  defp build_index(page_specs) do
    base_version = Version.zero()
    initial_index = Index.new()

    pages =
      Enum.map(page_specs, fn {page_id, keys} ->
        Page.new(page_id, Enum.map(keys, &{&1, base_version}))
      end)

    page_map =
      Enum.reduce(pages, initial_index.page_map, fn page, acc ->
        Map.put(acc, Page.id(page), {page, 0})
      end)

    tree =
      Enum.reduce(pages, initial_index.tree, fn page, acc ->
        Tree.add_page_to_tree(acc, page)
      end)

    index = IndexTestHelpers.rebuild_page_chain_consistency(%{initial_index | tree: tree, page_map: page_map})

    max_page_id = page_specs |> Enum.map(&elem(&1, 0)) |> Enum.max(fn -> 0 end)
    allocator = IdAllocator.new(max_page_id, [])

    {index, allocator}
  end

  defp new_update(index, allocator, database, opts \\ []) do
    version = Keyword.get(opts, :version, Version.from_bytes(<<1::64>>))
    update = IndexUpdate.new(index, version, allocator, database)

    if Keyword.get(opts, :track_statistics, false) do
      %{update | track_statistics: true}
    else
      update
    end
  end

  defp run_mutations(index, allocator, database, mutations, opts \\ []) do
    index
    |> new_update(allocator, database, opts)
    |> IndexUpdate.apply_mutations(mutations)
    |> IndexUpdate.process_pending_operations()
  end

  defp all_keys(index) do
    index.page_map
    |> Map.values()
    |> Enum.flat_map(fn {page, _next_id} -> Page.keys(page) end)
    |> Enum.sort()
  end

  describe "pending-operation merging" do
    test "two clears targeting keys on the same page merge into one pending map", %{database: database} do
      {index, allocator} = build_index([{0, ["a", "b", "c"]}])

      update =
        index
        |> new_update(allocator, database)
        |> IndexUpdate.apply_mutations([{:clear, "a"}, {:clear, "c"}])

      assert map_size(update.pending_operations) == 1
      assert %{"a" => :clear, "c" => :clear} = update.pending_operations[0]

      final = IndexUpdate.process_pending_operations(update)
      {final_index, _db, _allocator, _modified} = IndexUpdate.finish(final)

      assert all_keys(final_index) == ["b"]
    end

    test "a set followed by a clear on the same page share one pending map", %{database: database} do
      {index, allocator} = build_index([{0, ["a", "c"]}])

      update =
        index
        |> new_update(allocator, database)
        |> IndexUpdate.apply_mutations([{:set, "b", "value-b"}, {:clear, "a"}])

      assert map_size(update.pending_operations) == 1
      assert %{"a" => :clear, "b" => {:set, _locator}} = update.pending_operations[0]

      final = IndexUpdate.process_pending_operations(update)
      {final_index, final_db, _allocator, _modified} = IndexUpdate.finish(final)

      assert all_keys(final_index) == ["b", "c"]
      assert {:ok, _page, locator} = Index.locator_for_key(final_index, "b")
      assert {:ok, "value-b"} = Database.load_value(final_db, locator)
    end
  end

  describe "clear_range across a multi-page chain" do
    test "middle pages are deleted and recycled while boundary pages keep out-of-range keys", %{database: database} do
      {index, allocator} =
        build_index([
          {1, ["a", "b", "c"]},
          {2, ["d", "e", "f"]},
          {3, ["g", "h", "i"]}
        ])

      update = run_mutations(index, allocator, database, [{:clear_range, "b", "h"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      # Middle page 2 is deleted entirely and its id recycled
      refute Map.has_key?(final_index.page_map, 2)
      assert 2 in final_allocator.free_ids

      # Boundary pages keep only their out-of-range keys
      assert final_index |> Index.get_page!(1) |> Page.keys() == ["a"]
      assert final_index |> Index.get_page!(3) |> Page.keys() == ["i"]
      assert all_keys(final_index) == ["a", "i"]

      # Chain is repaired: page 1 now points past the deleted page to page 3
      {_page, next_id} = Index.get_page_with_next_id!(final_index, 1)
      assert next_id == 3

      # keys_removed counts first-page (b, c), middle-page (d, e, f) and last-page (g, h) keys
      assert update.keys_removed == 7
    end

    test "clear_range followed by sets of in-range keys in the same batch keeps only the re-set keys", %{
      database: database
    } do
      # The recovery-rewrite pattern: one transaction clear_ranges a keyed
      # family, then re-writes the current entries. The re-set keys must
      # survive (set overwrites the pending :clear for the same key) and
      # subset keys that are not re-written must be gone.
      {index, allocator} = build_index([{1, ["a", "b", "c"]}])

      update =
        run_mutations(index, allocator, database, [
          {:clear_range, "a", "d"},
          {:set, "b", "rewritten-b"},
          {:set, "d", "new-d"}
        ])

      {final_index, final_db, _allocator, _modified} = IndexUpdate.finish(update)

      assert all_keys(final_index) == ["b", "d"]
      assert {:ok, _page, locator} = Index.locator_for_key(final_index, "b")
      assert {:ok, "rewritten-b"} = Database.load_value(final_db, locator)
    end
  end

  describe "page 0 protection during range clears" do
    # Note: page 0 can never appear as a MIDDLE page of a chain traversal.
    # next_id == 0 is the chain terminator, so traversal always stops before
    # visiting page 0 as a successor; page 0 is only ever the first collected
    # page. The `0 in middle_page_ids` branch in apply_mutation is therefore
    # defensive and unreachable through the public API.
    test "page 0 as the first page of a multi-page range clear survives even when fully cleared", %{
      database: database
    } do
      {index, allocator} =
        build_index([
          {0, ["a", "b"]},
          {1, ["c", "d"]},
          {2, ["e", "f"]}
        ])

      update = run_mutations(index, allocator, database, [{:clear_range, "a", "e"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      # Page 0 lost all of its keys but must not be deleted or recycled
      assert Map.has_key?(final_index.page_map, 0)
      refute 0 in final_allocator.free_ids
      assert final_index |> Index.get_page!(0) |> Page.key_count() == 0

      # The middle page is deleted and recycled; the last page keeps "f"
      refute Map.has_key?(final_index.page_map, 1)
      assert 1 in final_allocator.free_ids
      assert final_index |> Index.get_page!(2) |> Page.keys() == ["f"]
      assert all_keys(final_index) == ["f"]

      # a, b from page 0 + c, d from the middle page + e from the last page
      assert update.keys_removed == 5
    end
  end

  describe "chain-following edge cases" do
    test "range entirely beyond all keys is a no-op", %{database: database} do
      # page_for_key falls back to page 0 (the empty head), so the chain walk
      # skips every real page as entirely-before-range until the chain ends.
      {index, allocator} =
        build_index([
          {1, ["a", "b"]},
          {2, ["d", "e"]}
        ])

      update = run_mutations(index, allocator, database, [{:clear_range, "x", "z"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      assert all_keys(final_index) == ["a", "b", "d", "e"]
      assert update.keys_removed == 0
      assert final_allocator.free_ids == []
      assert final_index.page_map |> Map.keys() |> Enum.sort() == [0, 1, 2]
    end

    test "range extending past the end of the chain clears only existing keys", %{database: database} do
      # The chain ends (next_id == 0) before the range end is reached
      {index, allocator} =
        build_index([
          {1, ["a", "b"]},
          {2, ["d", "e"]}
        ])

      update = run_mutations(index, allocator, database, [{:clear_range, "d", "zzzz"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      assert all_keys(final_index) == ["a", "b"]
      assert update.keys_removed == 2

      # Page 2 became empty, was deleted, and its id recycled
      refute Map.has_key?(final_index.page_map, 2)
      assert 2 in final_allocator.free_ids
    end

    test "range spanning from before the first key through the whole chain clears everything", %{database: database} do
      {index, allocator} =
        build_index([
          {1, ["b", "c"]},
          {2, ["d", "e"]},
          {3, ["f", "g"]}
        ])

      update = run_mutations(index, allocator, database, [{:clear_range, "a", "zzzz"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      assert all_keys(final_index) == []
      assert update.keys_removed == 6

      # All non-zero pages are gone and recycled; page 0 survives
      assert Map.keys(final_index.page_map) == [0]
      assert Enum.sort(final_allocator.free_ids) == [1, 2, 3]
    end
  end

  describe "empty-page handling after processing" do
    test "clearing all keys of a non-zero page deletes it and recycles its id", %{database: database} do
      {index, allocator} = build_index([{1, ["a", "b"]}])

      update = run_mutations(index, allocator, database, [{:clear, "a"}, {:clear, "b"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      refute Map.has_key?(final_index.page_map, 1)
      assert 1 in final_allocator.free_ids
      assert all_keys(final_index) == []

      # Lookups for the cleared keys now fall back to the (empty) page 0
      assert {:error, :not_found} = Index.locator_for_key(final_index, "a")
    end

    test "clearing all keys of page 0 keeps page 0 in the index with zero keys", %{database: database} do
      {index, allocator} = build_index([{0, ["a", "b"]}])

      update = run_mutations(index, allocator, database, [{:clear, "a"}, {:clear, "b"}])
      {final_index, _db, final_allocator, _modified} = IndexUpdate.finish(update)

      assert Map.has_key?(final_index.page_map, 0)
      refute 0 in final_allocator.free_ids
      assert final_index |> Index.get_page!(0) |> Page.key_count() == 0
      assert {:error, :not_found} = Index.locator_for_key(final_index, "a")
    end
  end

  describe "statistics tracking" do
    test "small mutation batch (direct path) counts added, changed, and removed keys", %{database: database} do
      {index, allocator} = build_index([{0, ["k1", "k2", "k3"]}])

      update =
        run_mutations(
          index,
          allocator,
          database,
          [
            # set on existing key -> changed
            {:set, "k1", "new-value"},
            # set on new key -> added
            {:set, "k9", "fresh"},
            # clear on existing key -> removed
            {:clear, "k2"},
            # clear on missing key -> no count
            {:clear, "missing"}
          ],
          track_statistics: true
        )

      assert update.keys_added == 1
      assert update.keys_changed == 1
      assert update.keys_removed == 1

      {final_index, _db, _allocator, _modified} = IndexUpdate.finish(update)
      assert all_keys(final_index) == ["k1", "k3", "k9"]
    end

    test "large mutation batch (MapSet path) counts added, changed, and removed keys", %{database: database} do
      existing = for i <- 1..8, do: "k#{i}"
      {index, allocator} = build_index([{0, existing}])

      # 4 sets on existing keys -> changed
      # 4 sets on new keys -> added
      # 3 clears on existing keys -> removed
      # 1 clear on a missing key -> no count
      mutations =
        Enum.map(1..4, fn i -> {:set, "k#{i}", "updated-#{i}"} end) ++
          Enum.map(1..4, fn i -> {:set, "n#{i}", "new-#{i}"} end) ++
          Enum.map(5..7, fn i -> {:clear, "k#{i}"} end) ++
          [{:clear, "zzz-missing"}]

      assert length(mutations) == 12

      update = run_mutations(index, allocator, database, mutations, track_statistics: true)

      assert update.keys_added == 4
      assert update.keys_changed == 4
      assert update.keys_removed == 3

      {final_index, _db, _allocator, _modified} = IndexUpdate.finish(update)
      assert all_keys(final_index) == Enum.sort(["k1", "k2", "k3", "k4", "k8", "n1", "n2", "n3", "n4"])
    end

    test "statistics are not accumulated when tracking is disabled", %{database: database} do
      {index, allocator} = build_index([{0, ["k1", "k2"]}])

      update = run_mutations(index, allocator, database, [{:set, "k1", "v"}, {:clear, "k2"}])

      assert update.keys_added == 0
      assert update.keys_changed == 0
      assert update.keys_removed == 0
    end
  end

  describe "atomic mutations" do
    test "atomic add on a missing key treats the current value as empty", %{database: database} do
      {index, allocator} = build_index([])

      update = run_mutations(index, allocator, database, [{:atomic, :add, "counter", <<5>>}])

      # Atomics are always counted as changes, even for previously-missing keys
      assert update.keys_changed == 1

      {final_index, final_db, _allocator, _modified} = IndexUpdate.finish(update)

      assert {:ok, _page, locator} = Index.locator_for_key(final_index, "counter")
      assert {:ok, <<5>>} = Database.load_value(final_db, locator)
    end

    test "atomic add on an existing key loads and combines the stored value", %{database: database} do
      {index, allocator} = build_index([])

      # First round: establish an existing value through the public API
      first_update =
        run_mutations(index, allocator, database, [{:set, "balance", <<10>>}], version: Version.from_bytes(<<1::64>>))

      {index_after_set, db_after_set, allocator_after_set, _modified} = IndexUpdate.finish(first_update)

      # Second round: atomic add against the existing value
      second_update =
        run_mutations(
          index_after_set,
          allocator_after_set,
          db_after_set,
          [{:atomic, :add, "balance", <<5>>}],
          version: Version.from_bytes(<<2::64>>)
        )

      {final_index, final_db, _allocator, _modified} = IndexUpdate.finish(second_update)

      assert {:ok, _page, locator} = Index.locator_for_key(final_index, "balance")
      assert {:ok, <<15>>} = Database.load_value(final_db, locator)
    end
  end
end
