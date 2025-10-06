defmodule Bedrock.DataPlane.Storage.Olivine.PageChainRecoveryBugTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp setup_tmp_dir(context) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "page_chain_bug_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  defp create_transaction(mutations, version_int) do
    transaction_map = %{
      mutations: mutations,
      read_conflicts: {nil, []},
      write_conflicts: []
    }

    encoded = Transaction.encode(transaction_map)
    version = Version.from_integer(version_int)

    {:ok, with_version} = Transaction.add_commit_version(encoded, version)
    with_version
  end

  describe "page chain recovery bug" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context)
    end

    test "recovers all pages in chain when durable version is empty", %{tmp_dir: tmp_dir} do
      # This reproduces the production bug:
      # 1. Create data that spans multiple pages (page 0 -> page 1 -> page 2 -> ... -> 0)
      # 2. Write an empty version block (squashing scenario)
      # 3. Recovery should load ALL pages in the chain, not just the first few

      file_path = Path.join(tmp_dir, "chain_bug.dets")
      {:ok, database} = Database.open(:chain_bug, file_path)

      # V1: Create enough data to force many pages
      # Each page holds ~256 keys, so let's create 10000 keys to get ~40 pages
      # This matches the production scenario better
      mutations_v1 =
        for i <- 1..10_000 do
          key = "key_#{String.pad_leading("#{i}", 6, "0")}"
          {:set, key, "value_#{i}"}
        end

      transaction_v1 = create_transaction(mutations_v1, 1_000_000)

      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      # Check how many pages we created
      [{_v1, {index_v1, modified_pages_v1}} | _] = index_manager_v1.versions
      page_count_v1 = map_size(index_v1.page_map)

      # Persist version 1
      {:ok, database_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # V2: Empty version (simulates the squashing scenario)
      transaction_v2 = create_transaction([], 2_000_000)
      {index_manager_v2, database_v2} = IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      [{_v2, {_index_v2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_v2,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      Database.close(final_database)

      # Now test recovery
      {:ok, recovered_database} = Database.open(:chain_bug_recovery, file_path)
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions

      # The bug: we only load the first few pages in the chain
      # We should load ALL pages that existed in v1
      recovered_page_count = map_size(recovered_index.page_map)

      assert recovered_page_count == page_count_v1,
             "Expected to recover #{page_count_v1} pages but got #{recovered_page_count}"

      # Verify the page chain is complete by following it
      assert_complete_page_chain(recovered_index.page_map, page_count_v1)

      # Verify we can find all keys
      for_result =
        for i <- 1..10_000 do
          key = "key_#{String.pad_leading("#{i}", 6, "0")}"

          case Index.locator_for_key(recovered_index, key) do
            {:ok, _page, _locator} -> nil
            _error -> key
          end
        end

      missing_keys = Enum.reject(for_result, &is_nil/1)

      assert missing_keys == [],
             "Failed to find #{length(missing_keys)} keys after recovery: #{inspect(Enum.take(missing_keys, 10))}"

      Database.close(recovered_database)
    end

    test "recovers pages across multiple version blocks", %{tmp_dir: tmp_dir} do
      # This tests the case where:
      # - Version 1 has pages 0-10
      # - Version 2 updates some middle pages (5-7) with new versions
      # - Version 3 is empty (squashing)
      # - Recovery needs to get page 0 from v1, pages 5-7 from v2, etc
      # - The chain is: page 0 (v1) -> page 1 (v1) -> ... -> page 5 (v2) -> page 6 (v2) -> ... -> page 10 (v1)

      file_path = Path.join(tmp_dir, "multi_version.dets")
      {:ok, database} = Database.open(:multi_version, file_path)

      # V1: Create data across multiple pages
      mutations_v1 =
        for i <- 1..2000 do
          key = "key_v1_#{String.pad_leading("#{i}", 5, "0")}"
          {:set, key, "value_#{i}"}
        end

      transaction_v1 = create_transaction(mutations_v1, 1_000_000)
      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      [{_v1, {index_v1, modified_pages_v1}} | _] = index_manager_v1.versions
      page_count_v1 = map_size(index_v1.page_map)

      {:ok, database_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # V2: Update some middle-range keys, creating new versions of some pages
      mutations_v2 =
        for i <- 500..1500 do
          key = "key_v1_#{String.pad_leading("#{i}", 5, "0")}"
          {:set, key, "value_#{i}_updated"}
        end

      transaction_v2 = create_transaction(mutations_v2, 2_000_000)
      {index_manager_v2, database_v2} = IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      [{_v2, {_index_v2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, database_v2, _metadata} =
        Database.advance_durable_version(
          database_v2,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      # V3: Empty version
      transaction_v3 = create_transaction([], 3_000_000)
      {index_manager_v3, database_v3} = IndexManager.apply_transactions(index_manager_v2, [transaction_v3], database_v2)

      [{_v3, {_index_v3, modified_pages_v3}} | _] = index_manager_v3.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_v3,
          index_manager_v3.current_version,
          index_manager_v2.current_version,
          1000,
          [modified_pages_v3]
        )

      Database.close(final_database)

      # Test recovery
      {:ok, recovered_database} = Database.open(:multi_version_recovery, file_path)
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions

      # Should recover all pages
      recovered_page_count = map_size(recovered_index.page_map)

      assert recovered_page_count == page_count_v1,
             "Expected #{page_count_v1} pages but got #{recovered_page_count}"

      # Verify complete chain
      assert_complete_page_chain(recovered_index.page_map, page_count_v1)

      # Verify all keys are findable
      for_result =
        for i <- 1..2000 do
          key = "key_v1_#{String.pad_leading("#{i}", 5, "0")}"

          case Index.locator_for_key(recovered_index, key) do
            {:ok, _page, _locator} -> nil
            _error -> key
          end
        end

      missing_keys = Enum.reject(for_result, &is_nil/1)

      assert missing_keys == [],
             "Missing #{length(missing_keys)} keys: #{inspect(Enum.take(missing_keys, 10))}"

      Database.close(recovered_database)
    end
  end

  # Verify page chain is complete by following all next_id pointers
  defp assert_complete_page_chain(page_map, expected_page_count) do
    # Start from page 0
    case Map.get(page_map, 0) do
      nil ->
        flunk("Page 0 not found in recovered page_map")

      {_page, next_id} ->
        visited = follow_chain_collecting_ids(page_map, next_id, MapSet.new([0]))
        actual_count = MapSet.size(visited)

        assert actual_count == expected_page_count,
               "Page chain has #{actual_count} pages but expected #{expected_page_count}"
    end
  end

  defp follow_chain_collecting_ids(_page_map, 0, visited), do: visited

  defp follow_chain_collecting_ids(page_map, page_id, visited) do
    if MapSet.member?(visited, page_id) do
      flunk("Cycle detected in page chain at page #{page_id}")
    end

    case Map.get(page_map, page_id) do
      nil ->
        flunk("Page #{page_id} referenced in chain but not found in page_map. Chain is broken!")

      {_page, next_id} ->
        new_visited = MapSet.put(visited, page_id)
        follow_chain_collecting_ids(page_map, next_id, new_visited)
    end
  end
end
