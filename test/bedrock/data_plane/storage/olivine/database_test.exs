defmodule Bedrock.DataPlane.Storage.Olivine.DatabaseTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Version

  defp with_db(context, file_name, table_name) do
    tmp_dir = context[:tmp_dir] || raise "tmp_dir not available in context"
    file_path = Path.join(tmp_dir, file_name)

    # Suppress expected connection retry logs during database open
    {result, _logs} =
      with_log(fn ->
        Database.open(table_name, file_path, pool_size: 1)
      end)

    {:ok, db} = result
    on_exit(fn -> Database.close(db) end)
    {:ok, db: db}
  end

  describe "database lifecycle" do
    @tag :tmp_dir

    setup context do
      tmp_dir =
        context[:tmp_dir] || Path.join(System.tmp_dir!(), "db_lifecycle_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

    test "open/2 creates and opens a SQLite database", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.sqlite")

      assert {:ok, %{pool: pool, window_size_in_microseconds: 5_000_000} = db} =
               Database.open(table_name, file_path, pool_size: 1)

      assert is_pid(pool)

      Database.close(db)
    end

    test "open/3 accepts custom window size", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.sqlite")

      assert {:ok, %{window_size_in_microseconds: 2_000_000} = db} = Database.open(table_name, file_path, 2_000)

      Database.close(db)
    end

    test "close/1 properly syncs and closes database", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.sqlite")

      {:ok, db} = Database.open(table_name, file_path, pool_size: 1)
      assert :ok = Database.close(db)
    end

    test "database persists data across open/close cycles", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "persist_test.sqlite")

      table_name = String.to_atom("persist_test_#{System.unique_integer([:positive])}")
      {:ok, db1} = Database.open(table_name, file_path, pool_size: 1)
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db1, 42, version, {<<"test_page_data">>, 0})
      :ok = Database.store_value(db1, <<"key1">>, version, <<"value1">>)

      # Persist data to SQLite before closing
      {:ok, _updated_db} = Database.advance_durable_version(db1, version, [version])

      Database.close(db1)

      # Reopen the same database instance (same OTP name, same file)
      {:ok, db2} = Database.open(table_name, file_path, pool_size: 1)

      {:ok, {page_data, _next_id}} = Database.load_page(db2, 42)
      assert page_data == <<"test_page_data">>

      {:ok, value} = Database.load_value(db2, <<"key1">>)
      assert value == <<"value1">>

      Database.close(db2)
    end
  end

  describe "page operations" do
    setup context, do: with_db(context, "pages.sqlite", :pages_test)

    @tag :tmp_dir
    test "store_page/3 and load_page/2 work correctly", %{db: db} do
      page_binary = <<"this is a test page">>

      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db, 1, version, {page_binary, 0})

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, {^page_binary, _next_id}} = Database.load_page(db, 1)
    end

    @tag :tmp_dir
    test "load_page/2 returns error for non-existent page", %{db: db} do
      assert {:error, :not_found} = Database.load_page(db, 999)
    end

    @tag :tmp_dir
    test "store_page/3 overwrites existing pages", %{db: db} do
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db, 1, version, {<<"original">>, 0})
      :ok = Database.store_page_version(db, 1, version, {<<"updated">>, 0})

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, {<<"updated">>, _next_id}} = Database.load_page(db, 1)
    end
  end

  describe "value operations" do
    setup context, do: with_db(context, "values.sqlite", :values_test)

    @tag :tmp_dir
    test "store_value/4 and load_value/2 work correctly", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>

      :ok = Database.store_value(db, key, version, value)

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, ^value} = Database.load_value(db, key)
    end

    @tag :tmp_dir
    test "load_value/2 returns error for non-existent key", %{db: db} do
      assert {:error, :not_found} = Database.load_value(db, <<"missing">>)
    end

    @tag :tmp_dir
    test "store_value/3 handles last-write-wins behavior", %{db: db} do
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_value(db, <<"key1">>, version, <<"initial_value">>)

      :ok = Database.store_value(db, <<"key1">>, version, <<"updated_value">>)

      :ok = Database.store_value(db, <<"key2">>, version, <<"different_key">>)

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, <<"updated_value">>} = Database.load_value(db, <<"key1">>)
      assert {:ok, <<"different_key">>} = Database.load_value(db, <<"key2">>)
    end
  end

  describe "database info and statistics" do
    setup context, do: with_db(context, "info.sqlite", :info_test)

    @tag :tmp_dir
    test "info/2 returns database statistics", %{db: db} do
      assert Database.info(db, :n_keys) == 0
      assert Database.info(db, :size_in_bytes) >= 0
      assert Database.info(db, :utilization) >= 0.0
      assert Database.info(db, :key_ranges) == []

      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db, 1, version, {<<"page_data">>, 0})
      :ok = Database.store_value(db, <<"key1">>, version, <<"value1">>)

      # Persist data to SQLite so info/2 can see it
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert Database.info(db, :n_keys) > 0
      assert Database.info(db, :size_in_bytes) > 0
    end

    @tag :tmp_dir
    test "info/2 handles unknown statistics", %{db: db} do
      assert Database.info(db, :unknown_stat) == :undefined
    end

    @tag :tmp_dir
    test "sync/1 forces data to disk", %{db: db} do
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db, 1, version, {<<"test">>, 0})
      # Persist data to SQLite first
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])
      assert :ok = Database.sync(db)
    end
  end

  describe "DETS schema verification" do
    setup context, do: with_db(context, "schema.sqlite", :schema_test)

    @tag :tmp_dir
    test "natural type separation works correctly", %{db: db} do
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_page_version(db, 42, version, {<<"page_data">>, 0})

      :ok = Database.store_value(db, <<"key">>, version, <<"value_data">>)

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, {<<"page_data">>, _next_id}} = Database.load_page(db, 42)
      assert {:ok, <<"value_data">>} = Database.load_value(db, <<"key">>)
    end

    @tag :tmp_dir
    test "handles edge cases in schema separation", %{db: db} do
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_value(db, <<42>>, version, <<"binary_42">>)

      :ok = Database.store_page_version(db, 42, version, {<<"page_42">>, 0})

      # Persist data to SQLite
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      assert {:ok, <<"binary_42">>} = Database.load_value(db, <<42>>)
      assert {:ok, {<<"page_42">>, _next_id}} = Database.load_page(db, 42)
    end
  end

  describe "unified value operations" do
    setup context, do: with_db(context, "unified_values.sqlite", :unified_test)

    @tag :tmp_dir
    test "load_value/3 returns values from lookaside buffer for recent versions", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>
      version = Version.from_integer(1000)

      # Store value in lookaside buffer
      :ok = Database.store_value(db, key, version, value)

      # Should be able to fetch it
      assert {:ok, ^value} = Database.load_value(db, key, version)
    end

    @tag :tmp_dir
    test "load_value/3 returns values from DETS for durable versions", %{db: db} do
      key = <<"durable_key">>
      value = <<"durable_value">>

      # Store in buffer then persist to SQLite (simulating durable storage)
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>
      :ok = Database.store_value(db, key, version, value)

      # Persist to SQLite to make it durable
      {:ok, _updated_db} = Database.advance_durable_version(db, version, [version])

      # Should be able to fetch from SQLite storage
      assert {:ok, ^value} = Database.load_value(db, key, Version.zero())
    end

    @tag :tmp_dir
    test "load_value/3 routes correctly based on durable version", %{db: db} do
      key = <<"routing_key">>
      hot_value = <<"hot_value">>
      cold_value = <<"cold_value">>
      cold_version = Version.from_integer(500)

      # Store cold value and persist it to SQLite (making it "cold"/durable)
      :ok = Database.store_value(db, key, cold_version, cold_value)
      {:ok, _updated_db} = Database.advance_durable_version(db, cold_version, [cold_version])

      # Store hot value in lookaside buffer (version higher than durable)
      hot_version = Version.from_integer(2000)
      :ok = Database.store_value(db, key, hot_version, hot_value)

      # Fetch at durable version should get cold value from SQLite
      assert {:ok, ^cold_value} = Database.load_value(db, key, Version.zero())

      # Fetch at hot version should get hot value
      assert {:ok, ^hot_value} = Database.load_value(db, key, hot_version)
    end

    @tag :tmp_dir
    test "store_value/4 handles version-specific storage", %{db: db} do
      key = <<"versioned_key">>
      version1 = Version.from_integer(1000)
      version2 = Version.from_integer(2000)

      :ok = Database.store_value(db, key, version1, <<"value1">>)
      :ok = Database.store_value(db, key, version2, <<"value2">>)

      assert {:ok, <<"value1">>} = Database.load_value(db, key, version1)
      assert {:ok, <<"value2">>} = Database.load_value(db, key, version2)
    end

    @tag :tmp_dir
    test "store_value/4 allows duplicate entries (last write wins)", %{db: db} do
      key = <<"dup_key">>
      version = Version.from_integer(1000)

      :ok = Database.store_value(db, key, version, <<"first">>)
      :ok = Database.store_value(db, key, version, <<"second">>)

      # Should have the last value (last write wins)
      assert {:ok, <<"second">>} = Database.load_value(db, key, version)
    end
  end

  describe "value_loader function" do
    setup context, do: with_db(context, "value_loader.sqlite", :value_loader_test)

    @tag :tmp_dir
    test "value_loader/1 creates function that can load values", %{db: db} do
      key = <<"loader_key">>
      hot_version = Version.from_integer(1000)
      cold_version = Version.zero()

      # Store cold value and persist it
      :ok = Database.store_value(db, key, cold_version, <<"cold_value">>)
      {:ok, _updated_db} = Database.advance_durable_version(db, cold_version, [cold_version])

      # Store hot value (stays in buffer)
      :ok = Database.store_value(db, key, hot_version, <<"hot_value">>)

      # Create value loader
      loader = Database.value_loader(db)

      # Should load hot value for hot version
      assert {:ok, <<"hot_value">>} = loader.(key, hot_version)

      # Should load cold value for cold version
      assert {:ok, <<"cold_value">>} = loader.(key, cold_version)

      # Should return not_found for missing key
      assert {:error, :not_found} = loader.(<<"missing">>, hot_version)
    end
  end

  describe "persistence optimization" do
    setup context, do: with_db(context, "persistence.sqlite", :persistence_test)

    @tag :tmp_dir
    test "advance_durable_version/3 persists and updates durable version", %{db: db} do
      # Database starts with zero durable version
      _initial_version = Version.zero()

      # Set up test versions and data
      v1 = Version.from_integer(100)
      v2 = Version.from_integer(200)

      # Store data in lookaside buffer
      :ok = Database.store_value(db, <<"key1">>, v1, <<"value1">>)
      :ok = Database.store_value(db, <<"key2">>, v2, <<"value2">>)
      :ok = Database.store_page_version(db, 42, v1, {<<"page42">>, 0})

      # Advance durable version to v2
      {:ok, updated_db} = Database.advance_durable_version(db, v2, [v1, v2])

      # Verify durable version was updated
      assert updated_db.durable_version == v2

      # Verify data was persisted to DETS
      assert {:ok, <<"value1">>} = Database.load_value(updated_db, <<"key1">>)
      assert {:ok, <<"value2">>} = Database.load_value(updated_db, <<"key2">>)
      assert {:ok, {<<"page42">>, _next_id}} = Database.load_page(updated_db, 42)

      # Verify durable version was persisted
      assert {:ok, ^v2} = Database.load_durable_version(updated_db)
    end

    @tag :tmp_dir
    test "advance_durable_version/3 cleans up lookaside buffer", %{db: db} do
      # Set up test data
      v1 = Version.from_integer(100)
      v2 = Version.from_integer(200)
      v3 = Version.from_integer(300)

      # Store data across multiple versions
      :ok = Database.store_value(db, <<"key1">>, v1, <<"value1">>)
      :ok = Database.store_value(db, <<"key2">>, v2, <<"value2">>)
      # Above cutoff
      :ok = Database.store_value(db, <<"key3">>, v3, <<"value3">>)

      # Advance durable version to v2 (should clean up v1 and v2, keep v3)
      {:ok, updated_db} = Database.advance_durable_version(db, v2, [v1, v2])

      # v1 and v2 should be cleaned from lookaside buffer but still accessible through DETS
      # Since v1 and v2 are now <= durable_version, load_value should route to DETS
      assert {:ok, <<"value1">>} = Database.load_value(updated_db, <<"key1">>, v1)
      assert {:ok, <<"value2">>} = Database.load_value(updated_db, <<"key2">>, v2)

      # v3 should still be in lookaside buffer
      assert {:ok, <<"value3">>} = Database.load_value(updated_db, <<"key3">>, v3)

      # But persisted data should be accessible via DETS
      assert {:ok, <<"value1">>} = Database.load_value(updated_db, <<"key1">>)
      assert {:ok, <<"value2">>} = Database.load_value(updated_db, <<"key2">>)
    end

    @tag :tmp_dir
    test "advance_durable_version/3 handles deduplication correctly", %{db: db} do
      # Set up overlapping data that should be deduplicated
      v1 = Version.from_integer(100)
      v2 = Version.from_integer(200)

      # Same key updated across versions
      :ok = Database.store_value(db, <<"key1">>, v1, <<"old_value">>)
      :ok = Database.store_value(db, <<"key1">>, v2, <<"new_value">>)

      # Same page updated across versions
      :ok = Database.store_page_version(db, 42, v1, {<<"old_page">>, 0})
      :ok = Database.store_page_version(db, 42, v2, {<<"new_page">>, 0})

      # Advance durable version - should persist only newest values
      {:ok, updated_db} = Database.advance_durable_version(db, v2, [v1, v2])

      # Should have persisted the newest values only (not old_value)
      assert {:ok, <<"new_value">>} = Database.load_value(updated_db, <<"key1">>)
      # Should have persisted the newest page only (not old_page)
      assert {:ok, {<<"new_page">>, _next_id}} = Database.load_page(updated_db, 42)
    end
  end
end
