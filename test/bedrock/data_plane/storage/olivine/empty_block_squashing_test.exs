defmodule Bedrock.DataPlane.Storage.Olivine.EmptyBlockSquashingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.TestHelpers
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp setup_tmp_dir(context) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "empty_squash_test_#{System.unique_integer([:positive])}")

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

  describe "empty version block squashing" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context)
    end

    test "consecutive empty blocks get squashed", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "squash_test.dets")
      {:ok, database} = Database.open(:squash_test, file_path)

      # Version 1: Create initial data
      keys_v1 = [{:set, "initial_key", "initial_value"}]
      transaction_v1 = create_transaction(keys_v1, 1_000_000)

      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      # Persist version 1
      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # Version 2: Empty (no modifications)
      transaction_v2 = create_transaction([], 2_000_000)
      {index_manager_v2, database_v2} = IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, database_v2, _metadata} =
        Database.advance_durable_version(
          database_v2,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      # Version 3: Empty again (should squash with v2)
      transaction_v3 = create_transaction([], 3_000_000)
      {index_manager_v3, database_v3} = IndexManager.apply_transactions(index_manager_v2, [transaction_v3], database_v2)

      [{_v3, {_index3, modified_pages_v3}} | _] = index_manager_v3.versions

      {:ok, database_v3, _metadata} =
        Database.advance_durable_version(
          database_v3,
          index_manager_v3.current_version,
          index_manager_v2.current_version,
          1000,
          [modified_pages_v3]
        )

      # Version 4: Empty again (should squash with v3)
      transaction_v4 = create_transaction([], 4_000_000)
      {index_manager_v4, database_v4} = IndexManager.apply_transactions(index_manager_v3, [transaction_v4], database_v3)

      [{_v4, {_index4, modified_pages_v4}} | _] = index_manager_v4.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_v4,
          index_manager_v4.current_version,
          index_manager_v3.current_version,
          1000,
          [modified_pages_v4]
        )

      Database.close(final_database)

      # Verify: Should only have 2 version blocks (v1 with data, v4 empty)
      # Not 4 blocks (v1, v2, v3, v4)
      {:ok, recovered_database} = Database.open(:squash_test_recovery, file_path)
      {_data_db, index_db} = recovered_database

      version_metadata = TestHelpers.load_version_range_metadata(index_db)

      # We should have exactly 2 blocks:
      # 1. Version 1 with data (points to version 0)
      # 2. Version 4 empty (points to version 1, squashing v2 and v3)
      assert length(version_metadata) == 2

      [{v4, v4_prev}, {v1, v1_prev}] = version_metadata

      assert v4 == Version.from_integer(4_000_000)
      assert v4_prev == Version.from_integer(1_000_000), "v4 should point to v1, skipping v2 and v3"

      assert v1 == Version.from_integer(1_000_000)
      assert v1_prev == Version.zero(), "v1 should point to version 0 (initial)"

      Database.close(recovered_database)
    end

    test "empty block followed by non-empty block does not squash", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "no_squash_test.dets")
      {:ok, database} = Database.open(:no_squash_test, file_path)

      # Version 1: Create initial data
      keys_v1 = [{:set, "key1", "value1"}]
      transaction_v1 = create_transaction(keys_v1, 1_000_000)

      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # Version 2: Empty
      transaction_v2 = create_transaction([], 2_000_000)
      {index_manager_v2, database_v2} = IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, database_v2, _metadata} =
        Database.advance_durable_version(
          database_v2,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      # Version 3: Non-empty (should NOT squash)
      keys_v3 = [{:set, "key2", "value2"}]
      transaction_v3 = create_transaction(keys_v3, 3_000_000)
      {index_manager_v3, database_v3} = IndexManager.apply_transactions(index_manager_v2, [transaction_v3], database_v2)

      [{_v3, {_index3, modified_pages_v3}} | _] = index_manager_v3.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_v3,
          index_manager_v3.current_version,
          index_manager_v2.current_version,
          1000,
          [modified_pages_v3]
        )

      Database.close(final_database)

      # Verify: Should have 3 version blocks (v1, v2, v3) - no squashing
      {:ok, recovered_database} = Database.open(:no_squash_test_recovery, file_path)
      {_data_db, index_db} = recovered_database

      version_metadata = TestHelpers.load_version_range_metadata(index_db)

      # All 3 blocks should be present
      assert length(version_metadata) == 3

      [{v3, v3_prev}, {v2, v2_prev}, {v1, v1_prev}] = version_metadata

      assert v3 == Version.from_integer(3_000_000)
      assert v3_prev == Version.from_integer(2_000_000), "v3 should point to v2"

      assert v2 == Version.from_integer(2_000_000)
      assert v2_prev == Version.from_integer(1_000_000), "v2 should point to v1"

      assert v1 == Version.from_integer(1_000_000)
      assert v1_prev == Version.zero(), "v1 should point to version 0"

      Database.close(recovered_database)
    end
  end
end
