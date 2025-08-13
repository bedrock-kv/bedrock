defmodule Bedrock.Cluster.Gateway.TransactionBuilder.CommittingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Committing
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  # Test helper to create a basic transaction state
  defp create_test_state(read_version, reads, writes) do
    %State{
      state: :valid,
      gateway: self(),
      transaction_system_layout: %{
        proxies: [:test_proxy1, :test_proxy2],
        storage_teams: [],
        services: %{}
      },
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec,
      read_version: read_version,
      reads: reads,
      writes: writes,
      stack: []
    }
  end

  # Mock commit function that captures the transaction for testing
  defp mock_commit_fn(expected_transaction) do
    fn _proxy, transaction ->
      assert transaction == expected_transaction
      # Return a mock version number
      {:ok, 42}
    end
  end

  describe "do_commit/2 integration tests" do
    test "write-only transaction with nil read_version" do
      reads = %{}
      writes = %{"key1" => "value1"}
      state = create_test_state(nil, reads, writes)

      expected_transaction = {nil, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "write-only transaction with read_version" do
      reads = %{}
      writes = %{"key1" => "value1", "key2" => "value2"}
      read_version = 123
      state = create_test_state(read_version, reads, writes)

      # Write-only transactions are blind writes and return {nil, writes}
      expected_transaction = {nil, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read-write transaction preserves read keys" do
      reads = %{"read_key1" => "read_val1", "read_key2" => "read_val2"}
      writes = %{"write_key" => "write_val"}
      read_version = 456
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "BedrockEx.setup_accounts scenario" do
      # This simulates the exact scenario that was failing
      # setup_accounts only writes
      reads = %{}

      writes = %{
        {"\x00\x00\x00\x08balances", "1"} => 100,
        {"\x00\x00\x00\x08balances", "2"} => 500
      }

      read_version = 1001
      state = create_test_state(read_version, reads, writes)

      # Write-only transactions are blind writes and return {nil, writes}
      expected_transaction = {nil, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "BedrockEx.move_money scenario" do
      # This simulates a read-then-write transaction
      reads = %{
        {"\x00\x00\x00\x08balances", "1"} => 100,
        {"\x00\x00\x00\x08balances", "2"} => 500
      }

      writes = %{
        {"\x00\x00\x00\x08balances", "1"} => 90,
        {"\x00\x00\x00\x08balances", "2"} => 510
      }

      read_version = 2001
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "nested transaction commit (stack not empty)" do
      # Test nested transaction handling
      reads = %{"current_read" => "val"}
      writes = %{"current_write" => "val"}
      stacked_reads = %{"stacked_read" => "val"}
      stacked_writes = %{"stacked_write" => "val"}

      state = %{create_test_state(100, reads, writes) | stack: [{stacked_reads, stacked_writes}]}

      # For nested commits, no external commit should happen
      {:ok, result_state} = Committing.do_commit(state)

      # Should merge the stacked state
      assert result_state.reads == Map.merge(reads, stacked_reads)
      assert result_state.writes == Map.merge(writes, stacked_writes)
      assert result_state.stack == []
      # Still valid, not committed
      assert result_state.state == :valid
    end

    test "commit proxy selection error handling" do
      # Test when no proxies are available
      state = %{
        create_test_state(100, %{}, %{"key" => "val"})
        | transaction_system_layout: %{
            proxies: [],
            storage_teams: [],
            services: %{}
          }
      }

      result = Committing.do_commit(state)

      assert {:error, :unavailable} = result
    end

    test "commit function failure handling" do
      reads = %{}
      writes = %{"key" => "val"}
      read_version = 789
      state = create_test_state(read_version, reads, writes)

      # Mock a failing commit
      failing_commit_fn = fn _proxy, _transaction ->
        {:error, :timeout}
      end

      result = Committing.do_commit(state, commit_fn: failing_commit_fn)

      assert {:error, :timeout} = result
    end
  end

  describe "read-dependent transactions (non-blind writes)" do
    test "single read then write transaction" do
      # Transaction reads account balance then writes new balance
      reads = %{"account_balance" => 250}
      writes = %{"account_balance" => 240}
      read_version = 555
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = ["account_balance"]
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "multiple reads with single write transaction" do
      # Transaction reads multiple accounts and writes to one
      reads = %{
        "account_1" => 100,
        "account_2" => 200,
        "account_3" => 300
      }

      writes = %{"transaction_log" => "transfer_completed"}
      read_version = 777
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "multiple reads with multiple writes transaction" do
      # Transaction reads multiple keys and writes to multiple keys
      reads = %{
        "user_profile" => %{name: "Alice", balance: 500},
        "settings" => %{theme: "dark", notifications: true}
      }

      writes = %{
        "user_profile" => %{name: "Alice", balance: 450},
        "audit_log" => %{action: "purchase", amount: 50},
        "inventory" => %{item_id: 123, quantity: 99}
      }

      read_version = 888
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read-only transaction (edge case)" do
      # Transaction has reads but no writes - should return empty writes map
      reads = %{
        "config_value" => "production",
        "feature_flags" => %{new_ui: true, beta_features: false}
      }

      writes = %{}
      read_version = 333
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, %{}}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read version handling with version 0" do
      # Verify read_version 0 is preserved correctly
      reads = %{"counter" => 0}
      writes = %{"counter" => 1}
      read_version = 0
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = ["counter"]
      expected_transaction = {{0, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read version handling with large version number" do
      # Verify large read_version numbers are preserved correctly
      reads = %{"large_dataset" => "chunk_1"}
      writes = %{"large_dataset" => "chunk_1_processed"}
      read_version = 999_999_999
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = ["large_dataset"]
      expected_transaction = {{999_999_999, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read keys order preservation" do
      # Verify read keys maintain consistent order when extracted
      reads = %{
        "z_last" => "value_z",
        "a_first" => "value_a",
        "m_middle" => "value_m"
      }

      writes = %{"result" => "processed"}
      read_version = 123
      state = create_test_state(read_version, reads, writes)

      # Map.keys/1 returns keys in the order they appear in the map
      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "complex key types in read-dependent transaction" do
      # Test with complex key types (tuples) that might be used in real scenarios
      reads = %{
        {"users", "alice"} => %{balance: 100, status: "active"},
        {"sessions", "session_123"} => %{user_id: "alice", expires_at: 1_234_567_890}
      }

      writes = %{
        {"users", "alice"} => %{balance: 90, status: "active"},
        {"transactions", "tx_456"} => %{from: "alice", to: "bob", amount: 10}
      }

      read_version = 456
      state = create_test_state(read_version, reads, writes)

      expected_read_keys = Map.keys(reads)
      expected_transaction = {{read_version, expected_read_keys}, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end
  end

  describe "regression prevention" do
    test "fix prevents transaction visibility bug" do
      # This test captures the exact bug that was causing transaction visibility issues

      # Setup: Write-only transaction with a read_version (acquired during normal flow)
      reads = %{}
      writes = %{"test_key" => "test_value"}
      read_version = 999
      state = create_test_state(read_version, reads, writes)

      # Write-only transactions are blind writes and return {nil, writes}
      expected_transaction = {nil, writes}
      commit_fn = mock_commit_fn(expected_transaction)

      # Execute
      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      # Verify success
      assert result_state.state == :committed
      assert result_state.commit_version == 42

      # The mock_commit_fn will fail if the wrong transaction format is passed,
      # so this test passing means the fix is working correctly
    end

    test "fix does not break existing functionality" do
      # Ensure all the original scenarios still work

      # Scenario 1: nil read_version (should remain unchanged)
      state1 = create_test_state(nil, %{}, %{"key" => "val"})
      expected1 = {nil, %{"key" => "val"}}
      commit_fn1 = mock_commit_fn(expected1)
      {:ok, _} = Committing.do_commit(state1, commit_fn: commit_fn1)

      # Scenario 2: read_version with reads (should remain unchanged)
      state2 = create_test_state(100, %{"rkey" => "rval"}, %{"wkey" => "wval"})
      expected2 = {{100, ["rkey"]}, %{"wkey" => "wval"}}
      commit_fn2 = mock_commit_fn(expected2)
      {:ok, _} = Committing.do_commit(state2, commit_fn: commit_fn2)

      # All assertions pass if we reach here
      assert true
    end
  end
end
