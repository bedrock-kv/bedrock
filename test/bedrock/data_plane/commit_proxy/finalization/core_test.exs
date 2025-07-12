defmodule Bedrock.DataPlane.CommitProxy.FinalizationCoreTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias FinalizationTestSupport, as: Support

  describe "finalize_batch/2" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      Support.ensure_process_killed(log_server)
      
      %{transaction_system_layout: transaction_system_layout, log_server: log_server}
    end

    test "exits when resolver is unavailable", %{transaction_system_layout: transaction_system_layout} do
      batch = Support.create_test_batch(100, 99)

      # Test error case when resolve_transactions fails
      result = catch_exit(
        Finalization.finalize_batch(batch, transaction_system_layout)
      )

      # Should exit due to unavailable resolver
      assert result == {:resolver_unavailable, :unavailable}
    end

    test "handles batch with aborted transactions", %{transaction_system_layout: transaction_system_layout} do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 2,
        buffer: [
          {reply_fn1, {nil, %{<<"key1">> => <<"value1">>}}},  # index 0 - will be aborted
          {reply_fn2, {nil, %{<<"key2">> => <<"value2">>}}}   # index 1 - success
        ]
      }

      # Mock resolver that aborts first transaction
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, [1]}  # Abort transaction at index 1 (which corresponds to reply1)
      end

      # Mock log push function that succeeds
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, all_logs_fn, _opts ->
        all_logs_fn.(100)
        :ok
      end

      result = Finalization.finalize_batch(
        batch, 
        transaction_system_layout,
        resolver_fn: mock_resolver_fn,
        batch_log_push_fn: mock_log_push_fn
      )

      # Should get 1 abort, 1 success
      assert {:ok, 1, 1} = result

      # First transaction should be aborted
      assert_receive {:reply1, {:error, :aborted}}
      # Second transaction should succeed
      assert_receive {:reply2, {:ok, 100}}
    end

    test "handles empty batch", %{transaction_system_layout: transaction_system_layout} do
      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 0,
        buffer: []
      }

      # Mock resolver that succeeds with no aborts
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, []}
      end

      # Mock log push function that succeeds (for empty transactions)
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, all_logs_fn, _opts ->
        all_logs_fn.(100)
        :ok
      end

      result = Finalization.finalize_batch(
        batch, 
        transaction_system_layout,
        resolver_fn: mock_resolver_fn,
        batch_log_push_fn: mock_log_push_fn
      )
      
      assert {:ok, 0, 0} = result
    end

    test "handles all transactions aborted", %{transaction_system_layout: transaction_system_layout} do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 2,
        buffer: [
          {reply_fn1, {nil, %{<<"key1">> => <<"value1">>}}},
          {reply_fn2, {nil, %{<<"key2">> => <<"value2">>}}}
        ]
      }

      # Mock resolver that aborts all transactions
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, [0, 1]}  # Abort both transactions
      end

      # Mock log push function for empty transactions (all aborted)
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, all_logs_fn, _opts ->
        all_logs_fn.(100)
        :ok
      end

      result = Finalization.finalize_batch(
        batch, 
        transaction_system_layout,
        resolver_fn: mock_resolver_fn,
        batch_log_push_fn: mock_log_push_fn
      )
      
      assert {:ok, 2, 0} = result

      # Both should be aborted
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:error, :aborted}}
    end

    test "handles log failure with exit", %{transaction_system_layout: transaction_system_layout} do
      batch = Support.create_test_batch(100, 99)

      # Mock resolver that succeeds
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, []}  # No aborts
      end

      # Mock log push function that fails
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _all_logs_fn, _opts ->
        {:error, {:log_failures, [{"log_1", :timeout}]}}
      end

      # Should exit when logs fail
      result = catch_exit(
        Finalization.finalize_batch(
          batch, 
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )
      )

      assert result == {:log_failures, [{"log_1", :timeout}]}

      # Transaction should be aborted due to log failure
      assert_receive {:reply, {:error, :aborted}}
    end
  end

  describe "finalize_batch/3 with dependency injection" do
    test "uses injected abort_reply_fn" do
      transaction_system_layout = %{
        resolvers: [{<<0>>, :test_resolver}],
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}},
        storage_teams: [%{tag: 0, key_range: {<<>>, :end}, storage_ids: ["storage_1"]}]
      }

      reply_fn = fn result -> send(self(), {:reply, result}) end
      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 1,
        buffer: [{reply_fn, {nil, %{<<"key">> => <<"value">>}}}]
      }

      # Track calls to custom abort function
      test_pid = self()
      custom_abort_fn = fn reply_fns ->
        send(test_pid, {:custom_abort_called, length(reply_fns)})
        Enum.each(reply_fns, &(&1.({:error, :custom_abort})))
      end

      # Mock resolver that fails
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:error, :timeout}
      end

      result = Finalization.finalize_batch(
        batch,
        transaction_system_layout,
        resolver_fn: mock_resolver_fn,
        abort_reply_fn: custom_abort_fn
      )

      assert {:error, :timeout} = result
      assert_receive {:custom_abort_called, 1}
      assert_receive {:reply, {:error, :custom_abort}}
    end
  end
end