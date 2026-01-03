defmodule Bedrock.DataPlane.Storage.BasaltAtomicOperationsTest do
  @moduledoc """
  Tests for atomic operations in the Basalt storage engine (MVCC).

  These tests verify that atomic operations (add, min, max) work
  correctly at the Basalt MVCC layer, including:
  - Read-modify-write semantics
  - Proper handling of missing keys (default to empty binary)
  - Correct binary arithmetic with variable-length operands
  - Integration with MVCC versioning
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Basalt.MultiVersionConcurrencyControl, as: MVCC
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  describe "Basalt MVCC atomic operations" do
    setup do
      mvcc = MVCC.new(:"test_mvcc_#{System.unique_integer()}", Version.zero())
      {:ok, mvcc: mvcc}
    end

    test "add works with missing key (defaults to empty)", %{mvcc: mvcc} do
      # Create transaction with add on non-existent key
      transaction = create_atomic_transaction([{:atomic, :add, "counter", <<5>>}])

      # Apply transaction
      :ok = MVCC.apply_one_transaction!(mvcc, transaction)

      # Verify the result - should return operand since existing is empty
      version = Transaction.commit_version!(transaction)
      assert {:ok, <<5>>} = MVCC.fetch(mvcc, "counter", version)
    end

    test "add works with existing key", %{mvcc: mvcc} do
      # First, set initial value
      initial_transaction = create_set_transaction("counter", <<10>>, 1)
      :ok = MVCC.apply_one_transaction!(mvcc, initial_transaction)

      # Then add to it
      add_transaction = create_atomic_transaction([{:atomic, :add, "counter", <<5>>}], 2)
      :ok = MVCC.apply_one_transaction!(mvcc, add_transaction)

      # Verify the result (10 + 5 = 15)
      version = Transaction.commit_version!(add_transaction)
      assert {:ok, <<15>>} = MVCC.fetch(mvcc, "counter", version)
    end

    test "min works with missing key", %{mvcc: mvcc} do
      # min on non-existent key should use the operand value
      # 251 = -5 as unsigned byte
      transaction = create_atomic_transaction([{:atomic, :min, "min_temp", <<251>>}])
      :ok = MVCC.apply_one_transaction!(mvcc, transaction)

      version = Transaction.commit_version!(transaction)
      assert {:ok, <<251>>} = MVCC.fetch(mvcc, "min_temp", version)
    end

    test "min works with existing key", %{mvcc: mvcc} do
      # Set initial value of 10
      initial_transaction = create_set_transaction("temp", <<10>>, 1)
      :ok = MVCC.apply_one_transaction!(mvcc, initial_transaction)

      # Take min with 5 (should result in 5)
      min_transaction = create_atomic_transaction([{:atomic, :min, "temp", <<5>>}], 2)
      :ok = MVCC.apply_one_transaction!(mvcc, min_transaction)

      version = Transaction.commit_version!(min_transaction)
      assert {:ok, <<5>>} = MVCC.fetch(mvcc, "temp", version)
    end

    test "max works with missing key", %{mvcc: mvcc} do
      # max on non-existent key should use the operand value
      transaction = create_atomic_transaction([{:atomic, :max, "high_score", <<100>>}])
      :ok = MVCC.apply_one_transaction!(mvcc, transaction)

      version = Transaction.commit_version!(transaction)
      assert {:ok, <<100>>} = MVCC.fetch(mvcc, "high_score", version)
    end

    test "max works with existing key", %{mvcc: mvcc} do
      # Set initial value of 50
      initial_transaction = create_set_transaction("score", <<50>>, 1)
      :ok = MVCC.apply_one_transaction!(mvcc, initial_transaction)

      # Take max with 75 (should result in 75)
      max_transaction = create_atomic_transaction([{:atomic, :max, "score", <<75>>}], 2)
      :ok = MVCC.apply_one_transaction!(mvcc, max_transaction)

      version = Transaction.commit_version!(max_transaction)
      assert {:ok, <<75>>} = MVCC.fetch(mvcc, "score", version)
    end

    test "multiple atomic operations in single transaction", %{mvcc: mvcc} do
      # Set some initial values
      initial_transaction =
        create_transaction(
          [
            {:set, "counter", <<10>>},
            {:set, "min_val", <<20>>},
            {:set, "max_val", <<30>>}
          ],
          1
        )

      :ok = MVCC.apply_one_transaction!(mvcc, initial_transaction)

      # Perform multiple atomic operations
      atomic_transaction =
        create_atomic_transaction(
          [
            # 10 + 5 = 15
            {:atomic, :add, "counter", <<5>>},
            # min(20, 15) = 15
            {:atomic, :min, "min_val", <<15>>},
            # max(30, 25) = 30
            {:atomic, :max, "max_val", <<25>>}
          ],
          2
        )

      :ok = MVCC.apply_one_transaction!(mvcc, atomic_transaction)

      # Verify results
      version = Transaction.commit_version!(atomic_transaction)
      assert {:ok, <<15>>} = MVCC.fetch(mvcc, "counter", version)
      assert {:ok, <<15>>} = MVCC.fetch(mvcc, "min_val", version)
      assert {:ok, <<30>>} = MVCC.fetch(mvcc, "max_val", version)
    end

    test "atomic operations work on any binary data", %{mvcc: mvcc} do
      # Set initial value as string - atomic operations work on any binary
      initial_transaction = create_set_transaction("mixed", "test", 1)
      :ok = MVCC.apply_one_transaction!(mvcc, initial_transaction)

      # Perform atomic add - will add byte-by-byte to the existing binary
      add_transaction = create_atomic_transaction([{:atomic, :add, "mixed", <<1, 0, 0, 0>>}], 2)
      :ok = MVCC.apply_one_transaction!(mvcc, add_transaction)

      version = Transaction.commit_version!(add_transaction)
      # "test" + <<1,0,0,0>> = [116,101,115,116] + [1,0,0,0] = [117,101,115,116] = "uest"
      assert {:ok, "uest"} = MVCC.fetch(mvcc, "mixed", version)
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
