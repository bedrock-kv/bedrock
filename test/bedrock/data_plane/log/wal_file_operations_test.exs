defmodule Bedrock.DataPlane.Log.WALFileOperationsTest do
  @moduledoc """
  Critical test that would have caught the :not_found bug.

  Tests the core Log.pull/3 workflow: write transactions with specific versions,
  then pull transactions AFTER specific versions (start_after semantics).
  This tests the exact code path that storage servers use and is failing in production.
  """
  use ExUnit.Case, async: true

  import Bedrock.DataPlane.WALTestSupport, only: [setup_wal_test: 0]

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Version
  alias Bedrock.DataPlane.WALTestSupport

  setup_wal_test()

  describe "Log.pull/3 with start_after semantics" do
    setup [:create_test_log]

    test "retrieves transactions after specified version (exclusive)", %{log: log} do
      transactions = [
        WALTestSupport.create_test_transaction(100, [{"key1", "value1"}]),
        WALTestSupport.create_test_transaction(200, [{"key2", "value2"}]),
        WALTestSupport.create_test_transaction(300, [{"key3", "value3"}])
      ]

      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Pull after version 100 should return transactions 200 and 300, not 100
      assert {:ok, retrieved_txs} = Log.pull(log, Version.from_integer(100), limit: 10)
      versions = Enum.map(retrieved_txs, &WALTestSupport.extract_version/1)

      # Should NOT include version 100 (pull is exclusive)
      refute Version.from_integer(100) in versions
      assert Version.from_integer(200) in versions
      assert Version.from_integer(300) in versions
    end

    test "pull after latest version returns appropriate response", %{log: log} do
      tx = WALTestSupport.create_test_transaction(100, [{"key1", "value1"}])
      assert :ok = WALTestSupport.push_transactions_to_log(log, [tx])

      # Pull after the latest version should return empty list or version_too_new
      case Log.pull(log, Version.from_integer(100), limit: 10) do
        {:ok, []} -> :ok
        {:error, :version_too_new} -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "pull after intermediate version returns later transactions", %{log: log} do
      transactions = [
        WALTestSupport.create_test_transaction(100, [{"key1", "value1"}]),
        WALTestSupport.create_test_transaction(200, [{"key2", "value2"}]),
        WALTestSupport.create_test_transaction(300, [{"key3", "value3"}])
      ]

      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Pull after version 150 should only return versions 200 and 300
      assert {:ok, retrieved_txs} = Log.pull(log, Version.from_integer(150), limit: 10)
      versions = Enum.map(retrieved_txs, &WALTestSupport.extract_version/1)

      refute Version.from_integer(100) in versions
      assert Version.from_integer(200) in versions
      assert Version.from_integer(300) in versions
    end

    test "reproduces production scenario - pull after ancient version", %{log: log} do
      # Simulate the production scenario: storage has version <<0,0,0,0,2,45,220,105>>
      # and asks for transactions after it
      ancient_version = <<0, 0, 0, 0, 2, 45, 220, 105>>

      modern_tx1 = WALTestSupport.create_test_transaction(1000, [{"key1", "value1"}])
      modern_tx2 = WALTestSupport.create_test_transaction(2000, [{"key2", "value2"}])

      assert :ok = WALTestSupport.push_transactions_to_log(log, [modern_tx1, modern_tx2])

      # This should either:
      # 1. Return the modern transactions (if ancient_version < 1000)
      # 2. Return :error with appropriate reason (if ancient_version is too old)
      # 3. NOT return {:error, :not_found} unless genuinely nothing exists
      case Log.pull(log, ancient_version, limit: 10) do
        {:ok, txs} ->
          # If successful, should contain our modern transactions
          versions = Enum.map(txs, &WALTestSupport.extract_version/1)
          assert Version.from_integer(1000) in versions or Version.from_integer(2000) in versions

        {:error, :version_too_old} ->
          :ok

        {:error, :version_too_new} ->
          :ok

        {:error, :not_found} ->
          # This is the bug we're investigating - should not happen
          # if log claims to have version ranges that include newer transactions
          flunk("Got :not_found error - this indicates the version range bug")

        {:waiting_for, _version} ->
          :ok
      end
    end

    test "handles version boundaries with correct exclusion semantics", %{log: log} do
      tx1 = WALTestSupport.create_test_transaction(100, [{"key1", "value1"}])
      tx2 = WALTestSupport.create_test_transaction(200, [{"key2", "value2"}])

      assert :ok = WALTestSupport.push_transactions_to_log(log, [tx1, tx2])

      assert {:ok, txs_after_99} = Log.pull(log, Version.from_integer(99), limit: 10)
      versions_after_99 = Enum.map(txs_after_99, &WALTestSupport.extract_version/1)
      assert Version.from_integer(100) in versions_after_99
      assert Version.from_integer(200) in versions_after_99

      # Pull after version 100 should only include 200 (exclusive)
      assert {:ok, txs_after_100} = Log.pull(log, Version.from_integer(100), limit: 10)
      versions_after_100 = Enum.map(txs_after_100, &WALTestSupport.extract_version/1)
      refute Version.from_integer(100) in versions_after_100
      assert Version.from_integer(200) in versions_after_100
    end

    test "respects pull limits with correct semantics", %{log: log} do
      transactions =
        for i <- 1..10 do
          WALTestSupport.create_test_transaction(i * 100, [{"key#{i}", "value#{i}"}])
        end

      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      assert {:ok, limited_txs} = Log.pull(log, Version.from_integer(250), limit: 3)
      assert length(limited_txs) <= 3

      # Should only contain versions after 250
      versions = Enum.map(limited_txs, &WALTestSupport.extract_version/1)
      Enum.each(versions, fn v -> assert Version.newer?(v, Version.from_integer(250)) end)
    end
  end

  describe "version range consistency testing" do
    setup [:create_test_log]

    test "log version claims must match actual retrievability", %{log: log} do
      # This is the critical test that would catch the version range inconsistency bug

      transactions = [
        WALTestSupport.create_test_transaction(100, [{"key1", "value1"}]),
        WALTestSupport.create_test_transaction(200, [{"key2", "value2"}]),
        WALTestSupport.create_test_transaction(300, [{"key3", "value3"}])
      ]

      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Get the version range that the log claims to have
      # This would typically be done through info/1 or similar introspection
      # For now, we know we have versions 100-300

      test_versions = [
        # Before first - should get all
        Version.from_integer(99),
        # At first - should get 200, 300
        Version.from_integer(100),
        # Between - should get 200, 300
        Version.from_integer(150),
        # At second - should get 300
        Version.from_integer(200),
        # Between - should get 300
        Version.from_integer(250),
        # At last - should get empty
        Version.from_integer(300)
      ]

      Enum.each(test_versions, fn start_after ->
        case Log.pull(log, start_after, limit: 10) do
          {:ok, txs} ->
            # Verify all returned transactions have versions > start_after
            versions = Enum.map(txs, &WALTestSupport.extract_version/1)

            Enum.each(versions, fn v ->
              assert Version.newer?(v, start_after),
                     "Transaction version #{inspect(v)} should be newer than start_after #{inspect(start_after)}"
            end)

          {:error, :version_too_old} ->
            :ok

          {:error, :version_too_new} ->
            :ok

          {:waiting_for, _} ->
            :ok

          {:error, :not_found} ->
            # This should NOT happen if we claim to have data in this range
            flunk("Log.pull returned :not_found for start_after #{inspect(start_after)} - indicates version range bug")
        end
      end)
    end

    test "storage server workflow simulation", %{log: log} do
      # Simulate the exact workflow that storage servers use

      transactions = [
        WALTestSupport.create_test_transaction(1000, [{"account", "alice"}, {"balance", "100"}]),
        WALTestSupport.create_test_transaction(2000, [{"account", "bob"}, {"balance", "200"}])
      ]

      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Storage server starts with some last applied version and asks for newer transactions
      storage_last_applied = Version.from_integer(500)

      case Log.pull(log, storage_last_applied, limit: 100) do
        {:ok, pulled_txs} ->
          # Should get both transactions since they're after version 500
          assert length(pulled_txs) == 2
          versions = Enum.map(pulled_txs, &WALTestSupport.extract_version/1)
          assert Version.from_integer(1000) in versions
          assert Version.from_integer(2000) in versions

        error ->
          flunk("Storage server pull failed: #{inspect(error)}")
      end

      new_storage_last_applied = Version.from_integer(2000)

      case Log.pull(log, new_storage_last_applied, limit: 100) do
        {:ok, []} ->
          :ok

        {:waiting_for, _} ->
          :ok

        {:error, :version_too_new} ->
          :ok

        error ->
          flunk("Second storage server pull failed: #{inspect(error)}")
      end
    end
  end

  defp create_test_log(_context) do
    {:ok, log} = WALTestSupport.create_test_log()
    on_exit(fn -> WALTestSupport.cleanup_test_log(log) end)
    %{log: log}
  end
end
