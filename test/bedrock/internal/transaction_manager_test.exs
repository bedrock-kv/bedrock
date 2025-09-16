defmodule Bedrock.Internal.TransactionManagerTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Internal.Repo

  # Set up mock for cluster behavior
  defmock(MockCluster, for: Bedrock.Cluster)

  setup :verify_on_exit!

  # Helper function for gateway that counts calls and always fails with retryable error
  defp gateway_loop_with_counter(counter_agent) do
    receive do
      {:"$gen_call", from, {:begin_transaction, _opts}} ->
        _call_count = Agent.get_and_update(counter_agent, &{&1, &1 + 1})
        # Always return retryable error to test retry limit
        GenServer.reply(from, {:error, :timeout})
        gateway_loop_with_counter(counter_agent)
    end
  end

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
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 456})

            {:"$gen_cast", :rollback} ->
              :ok
          end

        :nested_transaction ->
          handle_nested_transaction_loop()
      end
    end)
  end

  describe "transaction/3" do
    test "successful transaction with :ok return" do
      {_gateway_pid, user_fun} = setup_successful_transaction(:ok)

      result = Repo.transact(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "successful transaction with {:ok, value} return" do
      {_gateway_pid, user_fun} = setup_successful_transaction({:ok, "success"})

      result = Repo.transact(MockCluster, user_fun, [])
      assert result == {:ok, "success"}
    end

    test "successful transaction returns binary value from callback" do
      binary_result = <<1, 2, 3, 4>>
      {_gateway_pid, user_fun} = setup_successful_transaction({:ok, binary_result})

      result = Repo.transact(MockCluster, user_fun, [])
      assert result == {:ok, binary_result}
    end

    test "successful transaction returns complex data structures from callback" do
      complex_result = %{prefix: <<0, 1, 2>>, layer: "test", metadata: %{version: 1}}
      {_gateway_pid, user_fun} = setup_successful_transaction({:ok, complex_result})

      result = Repo.transact(MockCluster, user_fun, [])
      assert result == {:ok, complex_result}
    end

    test "non-ok return value triggers rollback" do
      {_gateway_pid} = setup_failed_transaction(:rollback_only)
      user_fun = fn _txn -> {:error, "failed"} end

      result = Repo.transact(MockCluster, user_fun, [])
      assert result == {:error, "failed"}
    end

    test "exception in user function triggers rollback and reraises" do
      {_gateway_pid} = setup_failed_transaction(:rollback_only)
      user_fun = fn _txn -> raise RuntimeError, "user error" end

      assert_raise RuntimeError, "user error", fn ->
        Repo.transact(MockCluster, user_fun, [])
      end
    end

    test "gateway fetch failure returns error" do
      expect(MockCluster, :fetch_gateway, fn -> {:error, :unavailable} end)

      user_fun = fn _txn -> :ok end

      assert_raise MatchError, fn ->
        Repo.transact(MockCluster, user_fun, [])
      end
    end

    test "transaction begin failure returns error" do
      # Create a simple counter to track calls
      {:ok, counter_agent} = Agent.start_link(fn -> 0 end)

      # Create a gateway that always fails with retryable error
      gateway_pid =
        spawn(fn ->
          gateway_loop_with_counter(counter_agent)
        end)

      # Use stub to allow unlimited calls
      stub(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      # Should retry once on :timeout, then hit retry limit and fail
      assert_raise Bedrock.TransactionError, ~r/Retry limit exceeded/, fn ->
        Repo.transact(MockCluster, user_fun, retry_limit: 1)
      end

      # Verify it was called exactly twice (first call + one retry before hitting limit)
      call_count = Agent.get(counter_agent, & &1)
      assert call_count == 2
    end

    test "transaction retry limit of 0 means no retries" do
      # Create a simple counter to track calls
      {:ok, counter_agent} = Agent.start_link(fn -> 0 end)

      # Create a gateway that always fails
      gateway_pid =
        spawn(fn ->
          gateway_loop_with_counter(counter_agent)
        end)

      stub(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      # Should fail immediately without any retries
      assert_raise Bedrock.TransactionError, ~r/Retry limit exceeded/, fn ->
        Repo.transact(MockCluster, user_fun, retry_limit: 0)
      end

      # Verify it was called exactly once (no retries allowed)
      call_count = Agent.get(counter_agent, & &1)
      assert call_count == 1
    end
  end

  describe "retry logic" do
    test "commit timeout triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:timeout, :ok, 3)

      result = Repo.transact(MockCluster, user_fun, opts)
      assert result == :ok
    end

    test "commit aborted triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:aborted, {:ok, "retried"}, 2)

      result = Repo.transact(MockCluster, user_fun, opts)
      assert result == {:ok, "retried"}
    end

    test "commit unavailable triggers retry with retry_count > 0" do
      {user_fun, opts} = setup_retry_test(:unavailable, {:ok, "final_try"}, 1)

      result = Repo.transact(MockCluster, user_fun, opts)
      assert result == {:ok, "final_try"}
    end
  end

  describe "nested transactions" do
    test "nested transaction within main transaction" do
      # Create a transaction process that handles nested_transaction calls
      txn_pid = create_mock_transaction(:nested_transaction)

      # Put it in process dictionary to simulate existing transaction
      tx_key = {:transaction, MockCluster}
      Process.put(tx_key, txn_pid)

      try do
        user_fun = fn _txn -> :ok end

        result = Repo.transact(MockCluster, user_fun, [])
        assert result == :ok
      after
        Process.delete(tx_key)
      end
    end
  end

  # Helper function to handle retry scenarios
  defp handle_nested_transaction_loop do
    receive do
      {:"$gen_call", from, :nested_transaction} ->
        GenServer.reply(from, :ok)
        handle_nested_transaction_loop()

      {:"$gen_call", from, :commit} ->
        GenServer.reply(from, {:ok, 123})

      {:"$gen_cast", :rollback} ->
        :ok
    end
  end

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
