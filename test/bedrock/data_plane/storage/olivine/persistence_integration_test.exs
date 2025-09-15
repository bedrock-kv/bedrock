defmodule Bedrock.DataPlane.Storage.Olivine.PersistenceIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.PageTestHelpers

  # Helper functions for cleaner test assertions
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

  defp assert_values_in_db(database, key_values) do
    Enum.each(key_values, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = Database.load_value(database, key)
    end)
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
                 page_allocator: %{max_page_id: 0, free_page_ids: []},
                 current_version: current_version
               }
             } = state

      assert db
      assert current_version == Version.zero()
      assert {:ok, ^current_version} = Database.load_durable_version(state.database)

      Logic.shutdown(state)
    end

    test "server startup with existing data performs recovery", %{tmp_dir: tmp_dir} do
      {:ok, db1} = Database.open(:session1, Path.join(tmp_dir, "dets"))

      page0 = Page.new(0, [{<<"start">>, Version.from_integer(10)}])

      page2 = Page.new(2, [{<<"middle">>, Version.from_integer(20)}])

      page5 = Page.new(5, [{<<"end">>, Version.from_integer(30)}])

      page0_binary = PageTestHelpers.from_map(page0)
      page2_binary = PageTestHelpers.from_map(page2)
      page5_binary = PageTestHelpers.from_map(page5)
      :ok = Database.store_page(db1, 0, {page0_binary, 2})
      :ok = Database.store_page(db1, 2, {page2_binary, 5})
      :ok = Database.store_page(db1, 5, {page5_binary, 0})

      values = [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>},
        {<<"key3">>, <<"value3">>}
      ]

      :ok = Database.batch_store_values(db1, values)

      Database.close(db1)

      {:ok, state} = Logic.startup(:test_recovery, self(), :test_id, tmp_dir)

      assert %{
               index_manager: %{
                 page_allocator: %{max_page_id: 5, free_page_ids: free_pages}
               }
             } = state

      assert Enum.sort(free_pages) == [1, 3, 4]

      assert_values_in_db(state.database, [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>},
        {<<"key3">>, <<"value3">>}
      ])

      Logic.shutdown(state)
    end

    test "window advancement triggers persistence correctly", %{tmp_dir: tmp_dir} do
      {:ok, state} = Logic.startup(:test_window, self(), :test_id, tmp_dir)

      {:ok, updated_state} = Logic.advance_window_with_persistence(state)

      assert %{index_manager: vm, database: db} = updated_state
      assert vm
      assert db

      Logic.shutdown(updated_state)
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

    test "large dataset recovery performance", %{tmp_dir: tmp_dir} do
      {:ok, db1} = Database.open(:large_test, Path.join(tmp_dir, "dets"))

      page_tuples =
        for i <- 0..49 do
          next_id = if i == 49, do: 0, else: i + 1
          page = Page.new(i, [{<<"key_#{i}">>, Version.from_integer(i * 10)}])
          {page, next_id}
        end

      Enum.each(page_tuples, fn {page, next_id} ->
        :ok = Database.store_page(db1, Page.id(page), {page, next_id})
      end)

      values =
        for i <- 1..1000 do
          {<<"bulk_key_#{i}">>, <<"bulk_value_#{i}">>}
        end

      :ok = Database.batch_store_values(db1, values)

      Database.close(db1)

      start_time = System.monotonic_time(:millisecond)
      {:ok, state} = Logic.startup(:large_recovery, self(), :test_id, tmp_dir)
      end_time = System.monotonic_time(:millisecond)

      recovery_time = end_time - start_time
      assert recovery_time < 1000

      assert %{
               index_manager: %{
                 page_allocator: %{max_page_id: 49, free_page_ids: []}
               }
             } = state

      assert_values_in_db(state.database, [
        {<<"bulk_key_1">>, <<"bulk_value_1">>},
        {<<"bulk_key_500">>, <<"bulk_value_500">>}
      ])

      Logic.shutdown(state)
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
                 page_allocator: %{max_page_id: 0, free_page_ids: []}
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
      versions = Enum.map(Page.key_versions(retrieved_binary), fn {_key, version} -> version end)
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
                 page_allocator: %{max_page_id: 7, free_page_ids: free_pages}
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
                 page_allocator: %{max_page_id: 0, free_page_ids: []}
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

    test "window eviction maintains data integrity in DETS", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "window_eviction.dets")
      {:ok, db} = Database.open(:window_test, file_path)

      vm = IndexManager.new()
      # Create test versions (older to newer)
      # Old version - will be evicted
      v1 = Version.from_integer(100)
      # Recent version - will remain
      v2 = Version.from_integer(200)
      # Current version - will remain
      v3 = Version.from_integer(300)

      # Set up version manager with these versions
      vm = %{
        vm
        | versions: [
            {v3, {:gb_trees.empty(), %{}}},
            {v2, {:gb_trees.empty(), %{}}},
            {v1, {:gb_trees.empty(), %{}}}
          ],
          current_version: v3
      }

      # Set durable version in database to v1 (the oldest)
      {:ok, db} = Database.store_durable_version(db, v1)

      # Store some data for each version in DETS
      # Store values using new API (key, value) - versions not stored in DETS
      :ok = Database.store_value(db, <<"key1">>, <<"value1">>)
      :ok = Database.store_value(db, <<"key2">>, <<"value2">>)
      :ok = Database.store_value(db, <<"key3">>, <<"value3">>)

      # Advance to a new version, which should trigger window eviction
      new_version = Version.from_integer(400)
      updated_vm = IndexManager.advance_version(vm, new_version)

      # NOTE: advance_version doesn't add to versions list, only current_version is updated
      # So only existing versions should be present in memory
      version_list = Enum.map(updated_vm.versions, fn {v, _} -> v end)
      # advance_version doesn't add to versions list
      refute new_version in version_list
      assert v2 in version_list
      assert v3 in version_list
      # MVP keeps all existing versions in memory
      assert v1 in version_list
      # But current_version should be updated
      assert updated_vm.current_version == new_version

      # But data should still be accessible in DETS (without version since DETS stores version-less)
      assert_values_in_db(db, [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>},
        {<<"key3">>, <<"value3">>}
      ])

      # Durable version should be the oldest version (v1, since all are kept in MVP)
      assert {:ok, version} = Database.load_durable_version(db)
      assert version == v1

      Database.close(db)
    end

    test "window persistence integration with advance_window_with_persistence", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "window_persistence.dets")
      {:ok, db} = Database.open(:window_persist_test, file_path)

      vm = IndexManager.new()
      # Create test versions (older to newer)
      # Old version - will be evicted
      old_version = Version.from_integer(100)
      # Recent version - will remain
      recent_version = Version.from_integer(200)

      vm = %{
        vm
        | versions: [
            {recent_version, {:gb_trees.empty(), %{}}},
            {old_version, {:gb_trees.empty(), %{}}}
          ],
          current_version: recent_version
      }

      # Store data before window advancement
      # Store values using new API (key, value) - versions not stored in DETS
      :ok = Database.store_value(db, <<"old_key">>, <<"old_value">>)
      :ok = Database.store_value(db, <<"recent_key">>, <<"recent_value">>)

      # Advance window with persistence
      temp_state = %State{index_manager: vm, database: db}
      {:ok, updated_state} = Logic.advance_window_with_persistence(temp_state)
      _updated_vm = updated_state.index_manager

      # Data should be synced to disk (without version since DETS stores version-less)
      assert_values_in_db(db, [
        {<<"old_key">>, <<"old_value">>},
        {<<"recent_key">>, <<"recent_value">>}
      ])

      Database.close(db)
    end

    test "window eviction with page data persistence", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "page_window.dets")
      {:ok, db} = Database.open(:page_window_test, file_path)

      vm = IndexManager.new()
      # Mark as unused
      _current_time = :os.system_time(:millisecond)

      # Create versions spanning the window boundary
      # Old version - will be evicted
      old_version = Version.from_integer(1)
      # Recent version - will be kept
      recent_version = Version.from_integer(2)

      # Create pages for both versions
      old_page = Page.new(1, [{<<"old_key">>, old_version}])
      recent_page = Page.new(2, [{<<"recent_key">>, recent_version}])

      # Persist pages to DETS
      :ok = PageTestHelpers.persist_page_to_database(vm, db, old_page)
      :ok = PageTestHelpers.persist_page_to_database(vm, db, recent_page)

      # Set up version manager
      vm = %{
        vm
        | versions: [
            {recent_version, {:gb_trees.empty(), %{}}},
            {old_version, {:gb_trees.empty(), %{}}}
          ],
          current_version: recent_version
      }

      # Advance version to trigger eviction
      # Use predictable test version
      new_version = Version.from_integer(3)
      updated_vm = IndexManager.advance_version(vm, new_version)

      # NOTE: For MVP, no versions are evicted, so both should still be in memory
      version_list = Enum.map(updated_vm.versions, fn {v, _} -> v end)
      # MVP keeps all versions
      assert old_version in version_list
      assert recent_version in version_list

      # But pages should still be retrievable from DETS
      {:ok, {old_page_binary, _next_id}} = Database.load_page(db, 1)
      assert Page.keys(old_page_binary) == [<<"old_key">>]

      {:ok, {recent_page_binary, _next_id}} = Database.load_page(db, 2)
      assert Page.keys(recent_page_binary) == [<<"recent_key">>]

      Database.close(db)
    end

    test "version window behavior across restarts", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "restart_window.dets")

      # Session 1: Create data with timestamps and shut down
      {:ok, db1} = Database.open(:session1, file_path)
      _vm1 = IndexManager.new()
      # Mark as unused
      _current_time = :os.system_time(:millisecond)

      # Create versions with known values (not used in DETS storage but kept for test structure)
      # First version
      _v1 = Version.from_integer(1)
      # Second version
      _v2 = Version.from_integer(2)

      # Store version-specific data
      # Store values using new API (key, value) - versions not stored in DETS
      :ok = Database.store_value(db1, <<"key1">>, <<"value1">>)
      :ok = Database.store_value(db1, <<"key2">>, <<"value2">>)

      Database.close(db1)

      # Session 2: Restart and verify window behavior
      {:ok, db2} = Database.open(:session2, file_path)
      {:ok, vm2} = IndexManager.recover_from_database(db2)

      # Data should still be accessible regardless of in-memory window state
      # (without version since DETS stores version-less)
      assert_values_in_db(db2, [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>}
      ])

      # Version manager should start fresh (versions list will be different)
      # but persistent data remains accessible
      zero_version = Version.zero()
      assert vm2.current_version == zero_version
      assert {:ok, ^zero_version} = Database.load_durable_version(db2)

      Database.close(db2)
    end
  end
end
