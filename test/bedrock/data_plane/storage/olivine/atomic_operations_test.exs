defmodule Bedrock.DataPlane.Storage.Olivine.AtomicOperationsTest do
  @moduledoc """
  Tests for atomic operations in the Olivine storage engine (IndexManager).

  These tests verify that atomic operations (add, min, max) work
  correctly at the Olivine IndexManager layer, including:
  - Integration with persistent database storage
  - Proper handling of missing keys (default to empty binary)
  - Correct binary arithmetic with variable-length operands
  - Database persistence of atomic operation results
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database, as: OlivineDatabase
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  describe "Olivine IndexManager atomic operations" do
    setup do
      # Create temporary directory for test database
      temp_dir = System.tmp_dir!() <> "/test_olivine_#{System.unique_integer()}"
      File.mkdir_p!(temp_dir)

      db_file_path = Path.join(temp_dir, "olivine_test.dets")
      {:ok, database} = OlivineDatabase.open(:"test_db_#{System.unique_integer()}", db_file_path)
      index_manager = IndexManager.new()

      on_exit(fn ->
        :ok = OlivineDatabase.close(database)
        File.rm_rf!(temp_dir)
      end)

      {:ok, database: database, index_manager: index_manager}
    end

    test "add works with missing key", %{database: database, index_manager: index_manager} do
      # Create transaction with add
      transaction = create_atomic_transaction([{:atomic, :add, "counter", <<8>>}])

      # Apply transaction
      updated_manager = IndexManager.apply_transaction(index_manager, transaction, database)

      # Ensure the transaction was applied successfully
      assert updated_manager != index_manager

      # Verify the value was stored correctly in database
      version = Transaction.commit_version!(transaction)
      assert {:ok, <<8>>} = OlivineDatabase.load_value(database, "counter", version)
    end

    test "atomic operations work with existing values through database", %{
      database: database,
      index_manager: index_manager
    } do
      # First, apply a transaction that sets an initial value
      initial_transaction = create_set_transaction("balance", <<100>>, 1)
      manager_with_initial = IndexManager.apply_transaction(index_manager, initial_transaction, database)

      # Then apply add (note: using 231 which is -25 as unsigned byte)
      add_transaction = create_atomic_transaction([{:atomic, :add, "balance", <<231>>}], 2)
      final_manager = IndexManager.apply_transaction(manager_with_initial, add_transaction, database)

      # Ensure the atomic operation was applied successfully
      assert final_manager != manager_with_initial

      # Verify the result (100 + 231 with carry = 75 as next byte)
      version = Transaction.commit_version!(add_transaction)
      assert {:ok, <<75, 1>>} = OlivineDatabase.load_value(database, "balance", version)
    end

    test "min and max work correctly", %{database: database, index_manager: index_manager} do
      # Apply transaction with min and max operations
      atomic_transaction =
        create_atomic_transaction([
          {:atomic, :min, "temperature", <<22>>},
          {:atomic, :max, "pressure", <<150>>}
        ])

      updated_manager = IndexManager.apply_transaction(index_manager, atomic_transaction, database)

      # Ensure the atomic operations were applied successfully
      assert updated_manager != index_manager

      # Verify results
      version = Transaction.commit_version!(atomic_transaction)
      assert {:ok, <<22>>} = OlivineDatabase.load_value(database, "temperature", version)
      assert {:ok, <<150>>} = OlivineDatabase.load_value(database, "pressure", version)
    end
  end

  # Helper functions

  defp create_atomic_transaction(mutations, version_int \\ 1) do
    create_transaction(mutations, version_int)
  end

  defp create_set_transaction(key, value, version_int) do
    create_transaction([{:set, key, value}], version_int)
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
end
