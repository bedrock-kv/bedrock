defmodule Bedrock.Internal.TransactionManagerStorageRetryTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Internal.Repo
  alias Bedrock.TransactionError

  # Set up mock for cluster behavior
  defmock(MockClusterStorage, for: Bedrock.Cluster)

  setup :verify_on_exit!

  # Helper to create a mock gateway that handles begin_transaction
  defp create_mock_gateway_with_storage_error(error_count) do
    counter = :counters.new(1, [])

    spawn(fn ->
      gateway_loop_with_storage_error(error_count, counter)
    end)
  end

  defp gateway_loop_with_storage_error(error_count, counter) do
    receive do
      {:"$gen_call", from, {:begin_transaction, _opts}} ->
        txn_pid = create_mock_transaction_with_shared_counter(error_count, counter)
        GenServer.reply(from, {:ok, txn_pid})
        gateway_loop_with_storage_error(error_count, counter)
    end
  end

  # Helper to create a transaction that shares a counter across retries
  defp create_mock_transaction_with_shared_counter(error_count, counter) do
    spawn(fn ->
      transaction_loop_with_storage_error(counter, error_count)
    end)
  end

  defp transaction_loop_with_storage_error(counter, max_errors) do
    receive do
      {:"$gen_call", from, {:get, _key, _opts}} ->
        count = :counters.get(counter, 1)
        :counters.add(counter, 1, 1)

        if count < max_errors do
          # Return :unavailable error as failure map which will be retryable
          GenServer.reply(from, {:failure, :unavailable})
        else
          # Success on final attempt
          GenServer.reply(from, {:ok, "success_value"})
        end

        transaction_loop_with_storage_error(counter, max_errors)

      {:"$gen_call", from, :commit} ->
        GenServer.reply(from, {:ok, 123})

      # Transaction completed, don't recurse

      {:"$gen_cast", :rollback} ->
        transaction_loop_with_storage_error(counter, max_errors)
    end
  end

  describe "storage error retry" do
    test "retries transaction on StorageError with retry_count > 0" do
      # Will fail once with StorageError, then succeed
      gateway_pid = create_mock_gateway_with_storage_error(1)

      # Expect fetch_gateway to be called twice (initial + 1 retry)
      expect(MockClusterStorage, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        # This will trigger StorageError on first attempt
        value = Repo.get(txn, "test_key")
        {:ok, value}
      end

      result = Repo.transaction(MockClusterStorage, user_fun)
      assert result == {:ok, "success_value"}
    end

    test "does not retry on TransactionError" do
      gateway_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:begin_transaction, _opts}} ->
              txn_pid =
                spawn(fn ->
                  receive do
                    {:"$gen_call", from2, {:get, _key, _opts}} ->
                      # Return error that will become TransactionError
                      GenServer.reply(from2, {:error, :invalid_key})
                  end
                end)

              GenServer.reply(from, {:ok, txn_pid})
          end
        end)

      # Should only be called once (no retry for TransactionError)
      expect(MockClusterStorage, :fetch_gateway, 1, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        Repo.get(txn, "test_key")
        :ok
      end

      assert_raise TransactionError, fn ->
        Repo.transaction(MockClusterStorage, user_fun)
      end
    end

    test "retries on timeout StorageError" do
      counter = :counters.new(1, [])

      gateway_loop_with_timeout = fn gateway_loop_fn, counter ->
        receive do
          {:"$gen_call", from, {:begin_transaction, _opts}} ->
            count = :counters.get(counter, 1)
            :counters.add(counter, 1, 1)

            txn_pid =
              if count == 0 do
                # First attempt - timeout error
                spawn(fn ->
                  receive do
                    {:"$gen_call", from2, {:get, _key, _opts}} ->
                      GenServer.reply(from2, {:failure, :timeout})

                    {:"$gen_cast", :rollback} ->
                      :ok
                  end
                end)
              else
                # Second attempt - success
                spawn(fn ->
                  receive do
                    {:"$gen_call", from2, {:get, _key, _opts}} ->
                      GenServer.reply(from2, {:ok, "timeout_recovered"})

                      receive do
                        {:"$gen_call", from3, :commit} ->
                          GenServer.reply(from3, {:ok, 456})

                        {:"$gen_cast", :rollback} ->
                          :ok
                      end

                    {:"$gen_cast", :rollback} ->
                      :ok
                  end
                end)
              end

            GenServer.reply(from, {:ok, txn_pid})
            gateway_loop_fn.(gateway_loop_fn, counter)
        end
      end

      gateway_pid =
        spawn(fn ->
          gateway_loop_with_timeout.(gateway_loop_with_timeout, counter)
        end)

      expect(MockClusterStorage, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        value = Repo.get(txn, "test_key")
        {:ok, value}
      end

      result = Repo.transaction(MockClusterStorage, user_fun)
      assert result == {:ok, "timeout_recovered"}
    end
  end

  describe "mixed error scenarios" do
    test "handles storage error followed by successful commit" do
      counter = :counters.new(1, [])

      gateway_loop_mixed = fn gateway_loop_fn, counter ->
        receive do
          {:"$gen_call", from, {:begin_transaction, _opts}} ->
            count = :counters.get(counter, 1)
            :counters.add(counter, 1, 1)

            txn_pid =
              if count == 0 do
                # First attempt - storage error
                create_failing_transaction(:unavailable)
              else
                # Second attempt - success
                create_successful_transaction()
              end

            GenServer.reply(from, {:ok, txn_pid})
            gateway_loop_fn.(gateway_loop_fn, counter)
        end
      end

      gateway_pid =
        spawn(fn ->
          gateway_loop_mixed.(gateway_loop_mixed, counter)
        end)

      expect(MockClusterStorage, :fetch_gateway, 2, fn -> {:ok, gateway_pid} end)

      user_fun = fn txn ->
        # First call will fail, second will succeed
        value = Repo.get(txn, "test_key")
        {:ok, value}
      end

      result = Repo.transaction(MockClusterStorage, user_fun)
      assert result == {:ok, "success"}
    end
  end

  # Helper functions
  defp create_failing_transaction(error_reason) do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get, _key, _opts}} ->
          GenServer.reply(from, {:failure, error_reason})

        {:"$gen_call", from, :commit} ->
          GenServer.reply(from, {:ok, 789})

        {:"$gen_cast", :rollback} ->
          :ok
      end
    end)
  end

  defp create_successful_transaction do
    spawn(fn ->
      receive do
        {:"$gen_call", from, {:get, _key, _opts}} ->
          GenServer.reply(from, {:ok, "success"})

          receive do
            {:"$gen_call", from2, :commit} ->
              GenServer.reply(from2, {:ok, 789})

            {:"$gen_cast", :rollback} ->
              :ok
          end
      end
    end)
  end
end
