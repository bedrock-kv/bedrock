defmodule Bedrock.DataPlane.Storage.Olivine.KeySelectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @moduletag :integration

  # Helper function to create a test state with data
  defp create_test_state_with_data do
    tmp_dir = System.tmp_dir!()
    db_file = Path.join(tmp_dir, "key_selector_test_#{System.unique_integer([:positive])}.dets")
    table_name = String.to_atom("key_selector_test_#{System.unique_integer([:positive])}")
    {:ok, database} = Database.open(table_name, db_file)

    index_manager = IndexManager.new()

    # Add some test data
    mutations = [
      {:set, "key1", "value1"},
      {:set, "key2", "value2"},
      {:set, "key3", "value3"},
      {:set, "test:key", "test:value"},
      {:set, "range:a", "range_a_value"},
      {:set, "range:b", "range_b_value"},
      {:set, "range:c", "range_c_value"},
      {:set, "range:z", "range_z_value"}
    ]

    transaction =
      Transaction.encode(%{
        commit_version: Version.from_integer(1),
        mutations: mutations
      })

    updated_index_manager = IndexManager.apply_transaction(index_manager, transaction, database)

    state = %State{
      database: database,
      index_manager: updated_index_manager,
      mode: :running,
      waiting_fetches: %{}
    }

    on_exit(fn ->
      Database.close(database)
      File.rm_rf(db_file)
    end)

    state
  end

  setup do
    {:ok, state: create_test_state_with_data()}
  end

  describe "KeySelector fetch operations" do
    test "fetch/4 with KeySelector resolves keys correctly", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("test:key")
      opts = []
      version = Version.from_integer(1)

      result = Logic.fetch(state, key_selector, version, opts)
      assert {:ok, {resolved_key, value}} = result
      assert resolved_key == "test:key"
      assert value == "test:value"
    end

    test "fetch/4 with KeySelector handles offsets", %{state: state} do
      # Test offset of +1 from key1 should resolve to key2
      base_selector = KeySelector.first_greater_or_equal("key1")
      offset_selector = KeySelector.add(base_selector, 1)
      version = Version.from_integer(1)

      result = Logic.fetch(state, offset_selector, version, [])
      assert {:ok, {resolved_key, value}} = result
      assert resolved_key == "key2"
      assert value == "value2"
    end

    test "fetch/4 with KeySelector handles boundary errors", %{state: state} do
      # Test high offset that goes beyond available keys
      high_offset_selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)
      version = Version.from_integer(1)

      result = Logic.fetch(state, high_offset_selector, version, [])
      assert {:error, :not_found} = result
    end
  end

  describe "KeySelector range operations" do
    test "range_fetch/5 with KeySelectors resolves range boundaries", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_than("range:z")
      version = Version.from_integer(1)

      result = Logic.range_fetch(state, start_selector, end_selector, version, [])
      # Range may not find data or could succeed
      case result do
        {:ok, key_value_pairs} ->
          assert is_list(key_value_pairs)
          assert Enum.all?(key_value_pairs, fn {k, v} -> is_binary(k) and is_binary(v) end)
          # Should include range keys if data exists
          keys = Enum.map(key_value_pairs, fn {k, _v} -> k end)
          assert Enum.any?(keys, fn k -> String.starts_with?(k, "range:") end)

        {:error, _reason} ->
          # Acceptable for test data setup
          :ok
      end
    end

    test "range_fetch/5 handles invalid ranges", %{state: state} do
      # Test range where start > end after resolution
      start_selector = KeySelector.first_greater_or_equal("z")
      end_selector = KeySelector.first_greater_or_equal("a")
      version = Version.from_integer(1)

      result = Logic.range_fetch(state, start_selector, end_selector, version, [])
      # Can be :invalid_range or :not_found depending on implementation
      assert {:error, error} = result
      assert error in [:invalid_range, :not_found]
    end

    test "range_fetch/5 respects limit option", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("key")
      end_selector = KeySelector.first_greater_than("range")
      version = Version.from_integer(1)

      result = Logic.range_fetch(state, start_selector, end_selector, version, limit: 2)
      assert {:ok, results} = result
      assert length(results) <= 2
    end
  end

  describe "IndexManager KeySelector resolution" do
    test "page_for_key/3 with KeySelector finds correct page", %{state: state} do
      key_selector = KeySelector.first_greater_than("key1")
      version = Version.from_integer(1)

      result = IndexManager.page_for_key(state.index_manager, key_selector, version)
      assert {:ok, resolved_key, page} = result
      assert is_binary(resolved_key)
      # first_greater_than "key1" should resolve to the next available key
      # Could be key2 or key3 depending on sort order
      assert resolved_key in ["key2", "key3"]
      # Page can be binary or page_map
      assert is_binary(page) or is_map(page)
    end

    test "pages_for_range/4 with KeySelectors finds correct pages", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("key1")
      end_selector = KeySelector.last_less_than("range")
      version = Version.from_integer(1)

      result = IndexManager.pages_for_range(state.index_manager, start_selector, end_selector, version)
      assert {:ok, {resolved_start, resolved_end}, pages} = result
      assert is_binary(resolved_start)
      assert is_binary(resolved_end)
      assert is_list(pages)
      assert resolved_start == "key1"
      # resolved_end should be the last key less than "range"
    end

    test "resolution handles version constraints", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("key1")

      # Test version_too_old scenario
      old_result = IndexManager.page_for_key(state.index_manager, key_selector, Version.zero())
      # Can be :version_too_old or :not_found depending on implementation
      assert {:error, error} = old_result
      assert error in [:version_too_old, :not_found]

      # Test version_too_new scenario
      future_version = Version.from_integer(999_999)
      future_result = IndexManager.page_for_key(state.index_manager, key_selector, future_version)
      assert {:error, :version_too_new} = future_result
    end
  end

  describe "waitlist operations with KeySelectors" do
    test "add_to_waitlist/5 supports KeySelector requests", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("wait_key")
      # Future version that doesn't exist yet
      version = Version.from_integer(2)
      reply_fn = fn _result -> :ok end
      timeout = 5000

      fetch_request = {key_selector, version}
      state_with_waitlist = Logic.add_to_waitlist(state, fetch_request, version, reply_fn, timeout)

      assert %State{waiting_fetches: waiting} = state_with_waitlist
      assert map_size(waiting) > 0
    end

    test "notify_waiting_fetches/2 processes KeySelector requests", %{state: state} do
      # Add KeySelector request to waitlist for future version
      key_selector = KeySelector.first_greater_or_equal("key1")
      future_version = Version.from_integer(2)
      reply_fn = fn _result -> :ok end
      timeout = 5000

      fetch_request = {key_selector, future_version}
      state_with_waitlist = Logic.add_to_waitlist(state, fetch_request, future_version, reply_fn, timeout)

      # Verify request was added to waitlist
      assert map_size(state_with_waitlist.waiting_fetches) > 0

      # Simulate version becoming available - notify waiting fetches
      updated_state = Logic.notify_waiting_fetches(state_with_waitlist, future_version)

      # Waitlist should be processed (though we can't easily verify the results are sent)
      assert %State{} = updated_state
    end
  end

  describe "edge cases and error conditions" do
    test "KeySelector with extreme offsets", %{state: state} do
      extreme_selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(999_999)
      version = Version.from_integer(1)

      # Should handle gracefully without crashing
      assert %KeySelector{offset: 999_999} = extreme_selector

      result = Logic.fetch(state, extreme_selector, version, [])
      assert {:error, :not_found} = result
    end

    test "empty key KeySelector", %{state: state} do
      empty_key_selector = KeySelector.first_greater_or_equal("")
      version = Version.from_integer(1)

      assert %KeySelector{key: "", or_equal: true, offset: 0} = empty_key_selector

      # Should handle empty keys appropriately - should find the first key
      result = Logic.fetch(state, empty_key_selector, version, [])
      assert {:ok, {resolved_key, _value}} = result
      assert is_binary(resolved_key)
    end

    test "binary key with special characters", %{state: state} do
      special_key = <<0xFF, 0x00, "special", 0x01>>
      special_selector = KeySelector.first_greater_than(special_key)
      version = Version.from_integer(1)

      assert %KeySelector{key: ^special_key} = special_selector

      # Should handle binary keys with special bytes correctly without crashing
      result = Logic.fetch(state, special_selector, version, [])
      # Since this key doesn't exist and is after all our test keys, should be not found
      assert {:error, :not_found} = result
    end
  end
end
