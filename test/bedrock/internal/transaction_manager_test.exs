defmodule Bedrock.Internal.TransactionManagerTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Internal.TransactionManager

  # Set up mock for cluster behavior
  defmock(MockCluster, for: Bedrock.Cluster)

  setup :verify_on_exit!

  # Helper to create a mock gateway that handles begin_transaction and returns
  # a transaction process that can handle various operations
  defp create_mock_gateway(txn_behavior) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:begin_transaction, _opts}} ->
          txn_pid = create_mock_transaction(txn_behavior)
          GenServer.reply(from, {:ok, txn_pid})
      end
    end)
  end

  # Common setup for successful transaction tests
  defp setup_successful_transaction(return_value) do
    gateway_pid = create_mock_gateway(:commit_success)
    expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)
    user_fun = fn _txn -> return_value end
    {gateway_pid, user_fun}
  end

  # Common setup for failed transaction tests
  defp setup_failed_transaction(behavior) do
    gateway_pid = create_mock_gateway(behavior)
    expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)
    {gateway_pid}
  end

  # Helper for retry test setup
  defp setup_retry_test(first_error, expected_result, retry_count) do
    call_count = :counters.new(1, [])

    gateway_pid =
      spawn(fn ->
        handle_retry_scenario(call_count, first_error, :commit_success)
      end)

    expect(MockCluster, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)
    user_fun = fn _txn -> expected_result end
    opts = [retry_count: retry_count]
    {user_fun, opts}
  end

  defp create_mock_transaction(behavior) do
    spawn(fn ->
      case behavior do
        :commit_success ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 123})
          end

        :commit_timeout ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:error, :timeout})
          end

        :commit_aborted ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:error, :aborted})
          end

        :commit_unavailable ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:error, :unavailable})
          end

        :rollback_only ->
          receive do
            {:"$gen_cast", :rollback} -> :ok
          end

        :nested_transaction ->
          receive do
            {:"$gen_call", from, :nested_transaction} ->
              GenServer.reply(from, :ok)
          end
      end
    end)
  end

  describe "transaction/3" do
    test "successful transaction with :ok return" do
      {_gateway_pid, user_fun} = setup_successful_transaction(:ok)

      result = TransactionManager.transaction(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "successful transaction with {:ok, value} return" do
      {_gateway_pid, user_fun} = setup_successful_transaction({:ok, "success"})

      result = TransactionManager.transaction(MockCluster, user_fun, [])
      assert result == {:ok, "success"}
    end

    test "non-ok return value triggers rollback" do
      {_gateway_pid} = setup_failed_transaction(:rollback_only)
      user_fun = fn _txn -> {:error, "failed"} end

      result = TransactionManager.transaction(MockCluster, user_fun, [])
      assert result == {:error, "failed"}
    end

    test "exception in user function triggers rollback and reraises" do
      {_gateway_pid} = setup_failed_transaction(:rollback_only)
      user_fun = fn _txn -> raise RuntimeError, "user error" end

      assert_raise RuntimeError, "user error", fn ->
        TransactionManager.transaction(MockCluster, user_fun, [])
      end
    end

    test "gateway fetch failure returns error" do
      expect(MockCluster, :fetch_gateway, fn -> {:error, :unavailable} end)

      user_fun = fn _txn -> :ok end

      result = TransactionManager.transaction(MockCluster, user_fun, [])
      assert result == {:error, :unavailable}
    end

    test "transaction begin failure returns error" do
      # Create a gateway that fails begin_transaction
      gateway_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:begin_transaction, []}} ->
              GenServer.reply(from, {:error, :timeout})
          end
        end)

      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      result = TransactionManager.transaction(MockCluster, user_fun, [])
      assert result == {:error, :timeout}
    end
  end

  describe "retry logic" do
    test "commit timeout triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:timeout, :ok, 3)

      result = TransactionManager.transaction(MockCluster, user_fun, opts)
      assert result == :ok
    end

    test "commit aborted triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:aborted, {:ok, "retried"}, 2)

      result = TransactionManager.transaction(MockCluster, user_fun, opts)
      assert result == {:ok, "retried"}
    end

    test "commit unavailable triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:unavailable, {:ok, "final_try"}, 1)

      result = TransactionManager.transaction(MockCluster, user_fun, opts)
      assert result == {:ok, "final_try"}
    end

    test "retry count exhaustion raises exception" do
      {_gateway_pid} = setup_failed_transaction(:commit_timeout)
      user_fun = fn _txn -> :ok end
      opts = [retry_count: 0]

      assert_raise RuntimeError, "Transaction failed: :timeout", fn ->
        TransactionManager.transaction(MockCluster, user_fun, opts)
      end
    end
  end

  describe "nested transactions" do
    test "nested transaction within main transaction" do
      # Create a transaction process that handles nested_transaction calls
      txn_pid = create_mock_transaction(:nested_transaction)

      # Put it in process dictionary to simulate existing transaction
      Process.put(TransactionManager.tx_key(MockCluster), txn_pid)

      try do
        user_fun = fn _txn -> :nested_ok end

        result = TransactionManager.transaction(MockCluster, user_fun, [])
        assert result == :nested_ok
      after
        Process.delete(TransactionManager.tx_key(MockCluster))
      end
    end
  end

  describe "tx_key/1" do
    test "returns consistent key for cluster" do
      key1 = TransactionManager.tx_key(MockCluster)
      key2 = TransactionManager.tx_key(MockCluster)

      assert key1 == key2
      assert key1 == {:transaction, MockCluster}
    end
  end

  # Helper function to handle retry scenarios
  defp handle_retry_scenario(call_count, first_error, second_behavior) do
    receive do
      {:"$gen_call", from, {:begin_transaction, _opts}} ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)

        if count == 0 do
          # First call - return transaction that fails with specified error
          error_atom =
            case first_error do
              :timeout -> :commit_timeout
              :aborted -> :commit_aborted
              :unavailable -> :commit_unavailable
            end

          txn_pid = create_mock_transaction(error_atom)
          GenServer.reply(from, {:ok, txn_pid})
        else
          # Second call - return transaction with success behavior
          txn_pid = create_mock_transaction(second_behavior)
          GenServer.reply(from, {:ok, txn_pid})
        end

        handle_retry_scenario(call_count, first_error, second_behavior)
    end
  end
end
