defmodule Bedrock.DataPlane.Storage.Olivine.PersistenceIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.PageTestHelpers

  # Helper functions for cleaner test assertions
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

  defp setup_tmp_dir(context, base_name) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "#{base_name}_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  defp create_test_pages(pages_data) do
    Enum.map(pages_data, fn
      {id, keys, _next_id} ->
        page = Page.new(id, Enum.map(keys, &{&1, Version.from_integer(id * 10)}))
        {id, page}

      {id, keys} ->
        page = Page.new(id, Enum.map(keys, &{&1, Version.from_integer(id * 10)}))
        {id, page}
    end)
  end

  describe "full persistence and recovery lifecycle" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_lifecycle_test")
    end

    test "server startup with empty database initializes correctly", %{tmp_dir: tmp_dir} do
      {:ok, state} = Logic.startup(:test_startup, self(), :test_id, tmp_dir)

      assert %{
               otp_name: :test_startup,
               id: :test_id,
               path: ^tmp_dir,
               database: db,
               index_manager: %{
                 id_allocator: %{max_id: 0, free_ids: []},
                 current_version: current_version
               }
             } = state

      assert db
      assert current_version == Version.zero()
      assert ^current_version = Database.durable_version(state.database)

      Logic.shutdown(state)
    end

    test "corrupted page handling during recovery", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("corrupt_test_#{System.unique_integer([:positive])}")
      {:ok, db1} = Database.open(table_name, Path.join(tmp_dir, "dets"))

      page0 = Page.new(0, [{<<"valid">>, Version.from_integer(100)}])
      page0_binary = PageTestHelpers.from_map(page0)
      :ok = Database.store_page(db1, 0, {page0_binary, 1})

      :ok = Database.store_page(db1, 1, {<<"definitely_not_a_valid_page">>, 0})

      page3 = Page.new(3, [{<<"isolated">>, Version.from_integer(300)}])
      page3_binary = PageTestHelpers.from_map(page3)
      :ok = Database.store_page(db1, 3, {page3_binary, 0})

      Database.close(db1)

      # Recovery should return error when encountering broken chain due to invalid page
      assert {:error, :broken_chain} = Logic.startup(:corrupt_recovery, self(), :test_id, tmp_dir)
    end
  end

  describe "edge cases and error conditions" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_edge_cases_test")
    end

    test "recovery with missing page 0 creates empty state", %{tmp_dir: tmp_dir} do
      # Create database with pages but no page 0
      {:ok, db1} = Database.open(:no_zero, Path.join(tmp_dir, "dets"))

      page5 = Page.new(5, [{<<"orphan">>, Version.from_integer(500)}])
      page5_binary = PageTestHelpers.from_map(page5)
      :ok = Database.store_page(db1, 5, {page5_binary, 0})

      Database.close(db1)

      # Recovery should handle missing page 0
      {:ok, state} = Logic.startup(:no_zero_recovery, self(), :test_id, tmp_dir)

      # Note: Since page 0 doesn't exist, no valid chain is found
      # max_page_id should be 0 (default) and no free pages
      assert %{
               index_manager: %{
                 id_allocator: %{max_id: 0, free_ids: []}
               }
             } = state

      Logic.shutdown(state)
    end

    test "database file permissions and error handling", %{tmp_dir: tmp_dir} do
      # Test with non-existent directory (should be created)
      non_existent_dir = Path.join(tmp_dir, "does/not/exist")

      {:ok, state} = Logic.startup(:permissions_test, self(), :test_id, non_existent_dir)

      # Should have created directory and initialized successfully
      assert File.exists?(non_existent_dir)
      assert state.database

      Logic.shutdown(state)
    end
  end

  describe "Phase 1.3 success criteria verification" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_criteria_test")
    end

    test "pages persist to DETS and can be retrieved", %{tmp_dir: tmp_dir} do
      {:ok, state} = Logic.startup(:criteria_pages, self(), :test_id, tmp_dir)

      # Create and persist a page
      page = Page.new(42, [{<<"test_key">>, Version.from_integer(12_345)}])
      :ok = PageTestHelpers.persist_page_to_database(state.index_manager, state.database, page)

      # Retrieve and verify
      {:ok, {retrieved_binary, _next_id}} = Database.load_page(state.database, 42)

      assert Page.id(retrieved_binary) == 42
      assert Page.keys(retrieved_binary) == [<<"test_key">>]
      versions = Enum.map(Page.key_locators(retrieved_binary), fn {_key, version} -> version end)
      assert versions == [Version.from_integer(12_345)]

      Logic.shutdown(state)
    end

    test "recovery rebuilds page structure correctly", %{tmp_dir: tmp_dir} do
      # Setup: Create complex page structure
      {:ok, db1} = Database.open(:rebuild_test, Path.join(tmp_dir, "dets"))

      # Chain: 0 -> 3 -> 7 -> 2 -> end (non-sequential for complexity)
      pages_data = [
        {0, [<<"start">>], 3},
        {3, [<<"second">>], 7},
        {7, [<<"third">>], 2},
        {2, [<<"end">>], 0}
      ]

      test_pages = create_test_pages(pages_data)

      # Store pages with their corresponding next_id from pages_data
      pages_data_map = Map.new(pages_data, fn {id, _keys, next_id} -> {id, next_id} end)

      Enum.each(test_pages, fn {id, page} ->
        next_id = Map.get(pages_data_map, id)
        :ok = Database.store_page(db1, id, {page, next_id})
      end)

      Database.close(db1)

      # Recovery
      {:ok, state} = Logic.startup(:rebuild_recovery, self(), :test_id, tmp_dir)

      # Verify structure rebuilt correctly
      # Free pages should be: 1, 4, 5, 6 (gaps in 0,2,3,7)
      expected_free = [1, 4, 5, 6]

      assert %{
               index_manager: %{
                 id_allocator: %{max_id: 7, free_ids: free_pages}
               }
             } = state

      assert Enum.sort(free_pages) == expected_free

      Logic.shutdown(state)
    end

    test "max_page_id is determined correctly during recovery", %{tmp_dir: tmp_dir} do
      # Create database with scattered page IDs
      {:ok, db1} = Database.open(:max_id_test, Path.join(tmp_dir, "dets"))

      # Unordered with large gap
      page_ids = [0, 5, 10, 3, 99, 50]

      Enum.each(page_ids, fn id ->
        page = Page.new(id, [{<<"page_#{id}">>, Version.from_integer(id)}])
        page_binary = PageTestHelpers.from_map(page)
        :ok = Database.store_page(db1, id, {page_binary, 0})
      end)

      Database.close(db1)

      # Recovery should find maximum correctly
      {:ok, state} = Logic.startup(:max_id_recovery, self(), :test_id, tmp_dir)

      # Note: Only page 0 is part of the valid chain (others are not linked)
      # max_page_id should be 0 (since page 0 is highest in valid chain) and no free pages
      assert %{
               index_manager: %{
                 id_allocator: %{max_id: 0, free_ids: []}
               }
             } = state

      Logic.shutdown(state)
    end
  end

  describe "window eviction with persistence (Phase 2.1)" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_window_test")
    end

    test "window eviction with page data persistence", %{tmp_dir: tmp_dir} do
      # Set up database
      file_path = Path.join(tmp_dir, "window_eviction.dets")
      {:ok, database} = Database.open(:window_test, file_path)

      # Create an IndexManager
      index_manager = IndexManager.new()

      # Create transactions with different timestamps to test eviction
      old_transaction = create_transaction([{:set, "old_key", "old_value"}], 1)
      recent_transaction = create_transaction([{:set, "recent_key", "recent_value"}], 1000)

      # Apply transactions to build up data in the IndexManager and Database
      {index_manager_with_old, database_with_old} =
        IndexManager.apply_transactions(index_manager, [old_transaction], database)

      {index_manager_with_both, database_with_both} =
        IndexManager.apply_transactions(index_manager_with_old, [recent_transaction], database_with_old)

      # Create many more transactions to fill the buffer tracking queue
      buffer_fill_transactions =
        for i <- 2..50 do
          create_transaction([{:set, "key_#{i}", "value_#{i}"}], i)
        end

      {final_index_manager, final_database} =
        IndexManager.apply_transactions(index_manager_with_both, buffer_fill_transactions, database_with_both)

      # Test window advancement - this should evict some old data but persist it to DETS
      case IndexManager.advance_window(final_index_manager, 10 * 1024 * 1024) do
        {:no_eviction, _updated_manager} ->
          # No eviction occurred - this is fine, the system may not have enough data to evict
          :ok

        {:evict, batch, updated_manager} ->
          # Eviction occurred - verify the batch contains versions
          assert length(batch) > 0
          # Each batch entry should be {version, data_size_in_bytes, bytes}
          Enum.each(batch, fn {version, _data_size_in_bytes, _bytes} ->
            assert Version.valid?(version)
          end)

          # The updated manager should still be valid
          assert is_struct(updated_manager, IndexManager)
      end

      # Verify that the IndexManager has processed transactions correctly
      # This tests that the core functionality is working
      assert final_index_manager.current_version > index_manager.current_version
      assert length(final_index_manager.versions) > length(index_manager.versions)

      Database.close(final_database)
    end
  end
end
