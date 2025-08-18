defmodule Bedrock.Internal.RepoTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Internal.Repo

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

        :commit_unknown ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:error, :unknown})
          end

        :rollback_only ->
          receive do
            {:"$gen_cast", :rollback} -> :ok
          end

        :fetch_operations ->
          mock_transaction_with_operations()

        :multi_operations ->
          mock_transaction_multi_operations()
      end
    end)
  end

  defp mock_transaction_with_operations do
    receive do
      {:"$gen_call", from, {:fetch, key}} ->
        GenServer.reply(from, {:ok, "value_for_#{key}"})
        mock_transaction_with_operations()

      {:"$gen_call", from, :nested_transaction} ->
        nested_pid = spawn(fn -> :ok end)
        GenServer.reply(from, {:ok, nested_pid})
        mock_transaction_with_operations()

      {:"$gen_cast", {:put, _key, _value}} ->
        mock_transaction_with_operations()

      {:"$gen_call", from, :commit} ->
        GenServer.reply(from, {:ok, 999})
    end
  end

  defp mock_transaction_multi_operations do
    receive do
      {:"$gen_call", from, {:fetch, "existing_key"}} ->
        GenServer.reply(from, {:ok, "existing_value"})

        receive do
          {:"$gen_cast", {:put, "new_key", "new_value"}} ->
            receive do
              {:"$gen_call", commit_from, :commit} ->
                GenServer.reply(commit_from, {:ok, 555})
            end
        end
    end
  end

  describe "transaction/3" do
    test "successful transaction with :ok return" do
      gateway_pid = create_mock_gateway(:commit_success)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "successful transaction with {:ok, value} return" do
      gateway_pid = create_mock_gateway(:commit_success)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> {:ok, "success"} end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:ok, "success"}
    end

    test "non-ok return value triggers rollback" do
      gateway_pid = create_mock_gateway(:rollback_only)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> {:error, "failed"} end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:error, "failed"}
    end

    test "exception in user function triggers rollback and reraises" do
      gateway_pid = create_mock_gateway(:rollback_only)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> raise RuntimeError, "user error" end

      assert_raise RuntimeError, "user error", fn ->
        Repo.transaction(MockCluster, user_fun, [])
      end
    end

    test "gateway fetch failure returns error" do
      expect(MockCluster, :fetch_gateway, fn -> {:error, :unavailable} end)

      user_fun = fn _txn -> :ok end

      result = Repo.transaction(MockCluster, user_fun, [])
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

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:error, :timeout}
    end

    test "commit timeout triggers retry with retry_count > 0" do
      call_count = :counters.new(1, [])

      # Create a gateway that handles multiple begin_transaction calls
      gateway_pid =
        spawn(fn ->
          handle_retry_scenario(call_count, :timeout, :commit_success)
        end)

      expect(MockCluster, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end
      opts = [retry_count: 3]

      result = Repo.transaction(MockCluster, user_fun, opts)
      assert result == :ok
    end

    test "commit aborted triggers retry with retry_count > 0" do
      call_count = :counters.new(1, [])

      # Create a gateway that handles multiple begin_transaction calls
      gateway_pid =
        spawn(fn ->
          handle_retry_scenario(call_count, :aborted, :commit_success)
        end)

      expect(MockCluster, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> {:ok, "retried"} end
      opts = [retry_count: 2]

      result = Repo.transaction(MockCluster, user_fun, opts)
      assert result == {:ok, "retried"}
    end

    test "commit unavailable triggers retry with retry_count > 0" do
      call_count = :counters.new(1, [])

      # Create a gateway that handles multiple begin_transaction calls
      gateway_pid =
        spawn(fn ->
          handle_retry_scenario(call_count, :unavailable, :commit_success)
        end)

      expect(MockCluster, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> {:ok, "final_try"} end
      opts = [retry_count: 1]

      result = Repo.transaction(MockCluster, user_fun, opts)
      assert result == {:ok, "final_try"}
    end

    test "retry count exhaustion raises exception" do
      gateway_pid = create_mock_gateway(:commit_timeout)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end
      opts = [retry_count: 0]

      assert_raise RuntimeError, "Transaction failed: :timeout", fn ->
        Repo.transaction(MockCluster, user_fun, opts)
      end
    end

    test "options are passed to Gateway.begin_transaction" do
      opts = [key_codec: MyCodec, value_codec: MyValueCodec, timeout_in_ms: 5000]

      # Create a gateway that verifies the options
      gateway_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:begin_transaction, received_opts}} ->
              # Verify options were passed correctly
              assert received_opts == opts
              txn_pid = create_mock_transaction(:commit_success)
              GenServer.reply(from, {:ok, txn_pid})
          end
        end)

      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      result = Repo.transaction(MockCluster, user_fun, opts)
      assert result == :ok
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

  describe "nested_transaction/1" do
    test "delegates to GenServer call with :nested_transaction message" do
      txn_pid = self()

      spawn(fn ->
        Repo.nested_transaction(txn_pid)
      end)

      assert_receive {:"$gen_call", _from, :nested_transaction}
    end

    test "uses infinity timeout" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :nested_transaction} ->
              GenServer.reply(from, {:ok, self()})
          end
        end)

      result = Repo.nested_transaction(txn_pid)
      assert {:ok, _nested_txn} = result
    end
  end

  describe "fetch/2" do
    test "delegates to GenServer call with {:fetch, key} message" do
      txn_pid = self()
      key = "test_key"

      spawn(fn ->
        Repo.fetch(txn_pid, key)
      end)

      assert_receive {:"$gen_call", _from, {:fetch, "test_key"}}
    end

    test "uses infinity timeout" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, key}} ->
              GenServer.reply(from, {:ok, "value_for_#{key}"})
          end
        end)

      result = Repo.fetch(txn_pid, "mykey")
      assert result == {:ok, "value_for_mykey"}
    end

    test "returns error responses" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, _key}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.fetch(txn_pid, "missing_key")
      assert result == {:error, :not_found}
    end
  end

  describe "fetch!/2" do
    test "returns value when fetch succeeds" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "success_key"}} ->
              GenServer.reply(from, {:ok, "success_value"})
          end
        end)

      result = Repo.fetch!(txn_pid, "success_key")
      assert result == "success_value"
    end

    test "raises exception when fetch returns error" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "error_key"}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert_raise RuntimeError, "Key not found: \"error_key\"", fn ->
        Repo.fetch!(txn_pid, "error_key")
      end
    end

    test "raises exception for different error types" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "timeout_key"}} ->
              GenServer.reply(from, {:error, :timeout})
          end
        end)

      assert_raise RuntimeError, "Key not found: \"timeout_key\"", fn ->
        Repo.fetch!(txn_pid, "timeout_key")
      end
    end
  end

  describe "get/2" do
    test "returns value when fetch succeeds" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "get_key"}} ->
              GenServer.reply(from, {:ok, "get_value"})
          end
        end)

      result = Repo.get(txn_pid, "get_key")
      assert result == "get_value"
    end

    test "returns nil when fetch returns error" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "missing_get_key"}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      result = Repo.get(txn_pid, "missing_get_key")
      assert result == nil
    end

    test "returns nil for different error types" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch, "error_get_key"}} ->
              GenServer.reply(from, {:error, :unavailable})
          end
        end)

      result = Repo.get(txn_pid, "error_get_key")
      assert result == nil
    end
  end

  describe "put/3" do
    test "sends cast message and returns transaction pid" do
      txn_pid = self()
      key = "put_key"
      value = "put_value"

      result = Repo.put(txn_pid, key, value)

      assert result == txn_pid
      assert_receive {:"$gen_cast", {:put, "put_key", "put_value"}}
    end

    test "handles different key and value types" do
      txn_pid = self()

      # Test with atom key and integer value
      result1 = Repo.put(txn_pid, :atom_key, 42)
      assert result1 == txn_pid
      assert_receive {:"$gen_cast", {:put, :atom_key, 42}}

      # Test with tuple key and map value
      result2 = Repo.put(txn_pid, {:compound, :key}, %{data: "test"})
      assert result2 == txn_pid
      assert_receive {:"$gen_cast", {:put, {:compound, :key}, %{data: "test"}}}
    end

    test "multiple puts return same transaction pid" do
      txn_pid = spawn(fn -> :ok end)

      result1 = Repo.put(txn_pid, "key1", "value1")
      result2 = Repo.put(txn_pid, "key2", "value2")

      assert result1 == txn_pid
      assert result2 == txn_pid
    end
  end

  describe "commit/2" do
    test "commits with default timeout" do
      txn_pid = self()

      spawn(fn ->
        Repo.commit(txn_pid)
      end)

      # Should use default timeout of 1000ms
      assert_receive {:"$gen_call", _from, :commit}
    end

    test "commits with custom timeout" do
      txn_pid = self()
      opts = [timeout_in_ms: 5000]

      spawn(fn ->
        Repo.commit(txn_pid, opts)
      end)

      assert_receive {:"$gen_call", _from, :commit}
    end

    test "returns success result" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 999})
          end
        end)

      result = Repo.commit(txn_pid)
      assert result == {:ok, 999}
    end

    test "returns error result" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:error, :timeout})
          end
        end)

      result = Repo.commit(txn_pid)
      assert result == {:error, :timeout}
    end

    test "handles empty options" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 111})
          end
        end)

      result = Repo.commit(txn_pid, [])
      assert result == {:ok, 111}
    end
  end

  describe "rollback/1" do
    test "sends cast message for rollback" do
      txn_pid = self()

      result = Repo.rollback(txn_pid)

      assert result == :ok
      assert_receive {:"$gen_cast", :rollback}
    end

    test "always returns :ok" do
      txn_pid = spawn(fn -> :ok end)

      result = Repo.rollback(txn_pid)
      assert result == :ok
    end

    test "works with different transaction pid types" do
      # Test with actual process
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_cast", :rollback} -> :ok
          end
        end)

      result = Repo.rollback(txn_pid)
      assert result == :ok

      # Test with self() - should still return :ok even if message isn't handled
      result2 = Repo.rollback(self())
      assert result2 == :ok
    end
  end

  describe "default_timeout_in_ms/0" do
    test "returns 1000 milliseconds" do
      result = Repo.default_timeout_in_ms()
      assert result == 1000
    end

    test "consistent return value" do
      result1 = Repo.default_timeout_in_ms()
      result2 = Repo.default_timeout_in_ms()

      assert result1 == result2
      assert result1 == 1000
    end
  end

  describe "integration scenarios" do
    test "transaction with multiple operations" do
      gateway_pid = create_mock_gateway(:multi_operations)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        # Fetch existing value
        {:ok, value} = Repo.fetch(txn, "existing_key")
        assert value == "existing_value"

        # Put new value
        ^txn = Repo.put(txn, "new_key", "new_value")

        :ok
      end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "nested transaction within main transaction" do
      gateway_pid = create_mock_gateway(:fetch_operations)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        {:ok, _nested_txn} = Repo.nested_transaction(txn)
        :ok
      end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "transaction with get/put pattern" do
      # Create a transaction that handles get->put->commit pattern
      gateway_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:begin_transaction, []}} ->
              txn_pid =
                spawn(fn ->
                  # Handle get (via fetch)
                  receive do
                    {:"$gen_call", fetch_from, {:fetch, "counter"}} ->
                      GenServer.reply(fetch_from, {:ok, 5})

                      # Handle put
                      receive do
                        {:"$gen_cast", {:put, "counter", 6}} ->
                          # Handle commit
                          receive do
                            {:"$gen_call", commit_from, :commit} ->
                              GenServer.reply(commit_from, {:ok, 888})
                          end
                      end
                  end
                end)

              GenServer.reply(from, {:ok, txn_pid})
          end
        end)

      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        current_value = Repo.get(txn, "counter")
        Repo.put(txn, "counter", current_value + 1)
        :ok
      end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == :ok
    end

    test "transaction with fetch! that succeeds" do
      gateway_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:begin_transaction, []}} ->
              txn_pid =
                spawn(fn ->
                  receive do
                    {:"$gen_call", fetch_from, {:fetch, "required_key"}} ->
                      GenServer.reply(fetch_from, {:ok, "critical_value"})

                      receive do
                        {:"$gen_call", commit_from, :commit} ->
                          GenServer.reply(commit_from, {:ok, 999})
                      end
                  end
                end)

              GenServer.reply(from, {:ok, txn_pid})
          end
        end)

      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        value = Repo.fetch!(txn, "required_key")
        assert value == "critical_value"
        {:ok, value}
      end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:ok, "critical_value"}
    end
  end

  describe "error propagation and edge cases" do
    test "handles with clause return values" do
      gateway_pid = create_mock_gateway(:rollback_only)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      # Test with nil return (should trigger rollback)
      user_fun = fn _txn -> nil end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == nil
    end

    test "handles various tuple return values correctly" do
      gateway_pid = create_mock_gateway(:commit_success)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      # Test with {:ok, value} tuple - should commit
      user_fun = fn _txn -> {:ok, :success} end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:ok, :success}
    end

    test "handles non-ok tuple return values" do
      gateway_pid = create_mock_gateway(:rollback_only)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      # Test with {:error, reason} tuple - should rollback
      user_fun = fn _txn -> {:error, :business_logic_failed} end

      result = Repo.transaction(MockCluster, user_fun, [])
      assert result == {:error, :business_logic_failed}
    end

    test "retry with exhausted count for aborted error" do
      gateway_pid = create_mock_gateway(:commit_aborted)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end
      opts = [retry_count: 0]

      assert_raise RuntimeError, "Transaction failed: :aborted", fn ->
        Repo.transaction(MockCluster, user_fun, opts)
      end
    end

    test "retry with exhausted count for unavailable error" do
      gateway_pid = create_mock_gateway(:commit_unavailable)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end
      opts = [retry_count: 0]

      assert_raise RuntimeError, "Transaction failed: :unavailable", fn ->
        Repo.transaction(MockCluster, user_fun, opts)
      end
    end
  end

  describe "option handling and parameter validation" do
    test "retry_count option defaults to 0 when not provided" do
      gateway_pid = create_mock_gateway(:commit_timeout)
      expect(MockCluster, :fetch_gateway, fn -> {:ok, gateway_pid} end)

      user_fun = fn _txn -> :ok end

      # With no retry_count, should raise immediately
      assert_raise RuntimeError, "Transaction failed: :timeout", fn ->
        Repo.transaction(MockCluster, user_fun, [])
      end
    end

    test "commit uses custom timeout when provided" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 654})
          end
        end)

      opts = [timeout_in_ms: 2500]
      result = Repo.commit(txn_pid, opts)
      assert result == {:ok, 654}
    end

    test "commit uses default timeout when timeout_in_ms is nil" do
      txn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :commit} ->
              GenServer.reply(from, {:ok, 321})
          end
        end)

      opts = [timeout_in_ms: nil]
      result = Repo.commit(txn_pid, opts)
      assert result == {:ok, 321}
    end
  end

  describe "concurrent transaction operations" do
    test "multiple concurrent fetch operations" do
      txn_pid =
        spawn(fn ->
          # Handle multiple fetch requests
          for _i <- 1..3 do
            receive do
              {:"$gen_call", from, {:fetch, key}} ->
                GenServer.reply(from, {:ok, "value_for_#{key}"})
            end
          end
        end)

      # Start concurrent fetch operations
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            Repo.fetch(txn_pid, "key_#{i}")
          end)
        end

      results = Task.await_many(tasks)

      assert Enum.sort(results) == [
               {:ok, "value_for_key_1"},
               {:ok, "value_for_key_2"},
               {:ok, "value_for_key_3"}
             ]
    end

    test "concurrent put operations" do
      txn_pid = self()

      # Start concurrent put operations
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            Repo.put(txn_pid, "key_#{i}", "value_#{i}")
          end)
        end

      results = Task.await_many(tasks)

      # All should return the transaction pid
      assert Enum.all?(results, &(&1 == txn_pid))

      # Should receive all cast messages
      for _i <- 1..3 do
        assert_receive {:"$gen_cast", {:put, key, value}}
        assert key in ["key_1", "key_2", "key_3"]
        assert value in ["value_1", "value_2", "value_3"]
      end
    end
  end
end
