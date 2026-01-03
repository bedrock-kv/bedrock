defmodule Bedrock.DataPlane.Log.WALFileOperationsTest do
  @moduledoc """
  Critical test that would have caught the :not_found bug.

  Tests the core Log.pull/3 workflow: write transactions with specific versions,
  then pull transactions AFTER specific versions (start_after semantics).
  This tests the exact code path that storage servers use and is failing in production.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.WALTestSupport

  describe "Log.pull/3 with start_after semantics" do
    setup [:create_test_log]

    test "retrieves transactions after specified version (exclusive)", %{log: log} do
      transactions = create_sequential_transactions([100, 200, 300])
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Pull after version 100 should return transactions 200 and 300, not 100
      assert {:ok, retrieved_txs} = Log.pull(log, Version.from_integer(100), limit: 10)

      # Use pattern matching to verify expected versions are present and 100 is excluded
      assert_versions_present_and_excluded(retrieved_txs, [200, 300], [100])
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
      transactions = create_sequential_transactions([100, 200, 300])
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Pull after version 150 should only return versions 200 and 300
      assert {:ok, retrieved_txs} = Log.pull(log, Version.from_integer(150), limit: 10)

      assert_versions_present_and_excluded(retrieved_txs, [200, 300], [100])
    end

    test "reproduces production scenario - pull after ancient version", %{log: log} do
      # Simulate the production scenario: storage has version <<0,0,0,0,2,45,220,105>>
      # and asks for transactions after it
      ancient_version = <<0, 0, 0, 0, 2, 45, 220, 105>>
      transactions = create_sequential_transactions([1000, 2000])
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # This should either return modern transactions or appropriate error, NOT :not_found
      case Log.pull(log, ancient_version, limit: 10) do
        {:ok, txs} ->
          # If successful, should contain our modern transactions using pattern matching
          assert_any_version_present(txs, [1000, 2000])

        {:error, error} when error in [:version_too_old, :version_too_new] ->
          :ok

        {:error, :not_found} ->
          flunk("Got :not_found error - this indicates the version range bug")

        {:waiting_for, _version} ->
          :ok
      end
    end

    test "handles version boundaries with correct exclusion semantics", %{log: log} do
      transactions = create_sequential_transactions([100, 200])
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      assert {:ok, txs_after_99} = Log.pull(log, Version.from_integer(99), limit: 10)
      assert_versions_present_and_excluded(txs_after_99, [100, 200], [])

      # Pull after version 100 should only include 200 (exclusive)
      assert {:ok, txs_after_100} = Log.pull(log, Version.from_integer(100), limit: 10)
      assert_versions_present_and_excluded(txs_after_100, [200], [100])
    end

    test "respects pull limits with correct semantics", %{log: log} do
      transaction_versions = for i <- 1..10, do: i * 100
      transactions = create_sequential_transactions(transaction_versions)
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      assert {:ok, limited_txs} = Log.pull(log, Version.from_integer(250), limit: 3)
      assert length(limited_txs) <= 3

      # Should only contain versions after 250 using pattern matching verification
      assert_all_versions_newer_than(limited_txs, Version.from_integer(250))
    end
  end

  describe "version range consistency testing" do
    setup [:create_test_log]

    test "log version claims must match actual retrievability", %{log: log} do
      # This is the critical test that would catch the version range inconsistency bug
      transactions = create_sequential_transactions([100, 200, 300])
      assert :ok = WALTestSupport.push_transactions_to_log(log, transactions)

      # Test various version boundaries to ensure consistency
      test_scenarios = [
        {99, "should get all"},
        {100, "should get 200, 300"},
        {150, "should get 200, 300"},
        {200, "should get 300"},
        {250, "should get 300"},
        {300, "should get empty"}
      ]

      Enum.each(test_scenarios, fn {version_int, _description} ->
        start_after = Version.from_integer(version_int)

        case Log.pull(log, start_after, limit: 10) do
          {:ok, txs} ->
            assert_all_versions_newer_than(txs, start_after)

          {:error, error} when error in [:version_too_old, :version_too_new] ->
            :ok

          {:waiting_for, _} ->
            :ok

          {:error, :not_found} ->
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

      assert {:ok, pulled_txs} = Log.pull(log, storage_last_applied, limit: 100)
      # Should get both transactions since they're after version 500
      assert length(pulled_txs) == 2
      assert_versions_present_and_excluded(pulled_txs, [1000, 2000], [])

      # Second pull after applying all transactions
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

  # Helper functions to reduce repetition and improve assertion clarity

  defp create_sequential_transactions(version_integers) do
    version_integers
    |> Enum.with_index(1)
    |> Enum.map(fn {version, index} ->
      WALTestSupport.create_test_transaction(version, [{"key#{index}", "value#{index}"}])
    end)
  end

  defp assert_versions_present_and_excluded(transactions, expected_present, expected_excluded) do
    versions = Enum.map(transactions, &WALTestSupport.extract_version/1)

    # Assert expected versions are present
    Enum.each(expected_present, fn version_int ->
      assert Version.from_integer(version_int) in versions,
             "Expected version #{version_int} to be present in #{inspect(versions)}"
    end)

    # Assert excluded versions are not present
    Enum.each(expected_excluded, fn version_int ->
      refute Version.from_integer(version_int) in versions,
             "Expected version #{version_int} to be excluded from #{inspect(versions)}"
    end)
  end

  defp assert_all_versions_newer_than(transactions, start_after_version) do
    versions = Enum.map(transactions, &WALTestSupport.extract_version/1)

    Enum.each(versions, fn version ->
      assert Version.newer?(version, start_after_version),
             "Transaction version #{inspect(version)} should be newer than #{inspect(start_after_version)}"
    end)
  end

  defp assert_any_version_present(transactions, expected_version_integers) do
    versions = Enum.map(transactions, &WALTestSupport.extract_version/1)
    expected_versions = Enum.map(expected_version_integers, &Version.from_integer/1)

    has_any_expected =
      Enum.any?(expected_versions, fn expected_version ->
        expected_version in versions
      end)

    assert has_any_expected,
           "Expected at least one of #{inspect(expected_version_integers)} to be present in #{inspect(versions)}"
  end
end
