defmodule Bedrock.DataPlane.CommitProxy.FinalizationCoreTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  # Helper functions for common patterns
  defp create_test_transaction(key, value) do
    %{
      mutations: [{:set, key, value}],
      write_conflicts: [{key, key <> <<0>>}],
      read_conflicts: [],
      read_version: nil
    }
  end

  defp create_reply_fn(test_pid, tag) do
    fn result -> send(test_pid, {tag, result}) end
  end

  defp create_batch_with_transactions(commit_version, last_version, transactions) do
    buffer =
      transactions
      |> Enum.with_index()
      |> Enum.map(fn {{reply_fn, tx_binary, task}, index} ->
        {index, reply_fn, tx_binary, task}
      end)

    %Batch{
      commit_version: Version.from_integer(commit_version),
      last_commit_version: Version.from_integer(last_version),
      n_transactions: length(transactions),
      buffer: buffer
    }
  end

  defp mock_resolver_with_aborts(aborted_indices) do
    fn resolver, epoch, last_version, commit_version, summaries, _metadata_per_tx, opts ->
      assert resolver == :test_resolver
      assert epoch == 1
      assert last_version == Version.from_integer(99)
      assert commit_version == Version.from_integer(100)
      assert is_list(summaries)
      assert Keyword.has_key?(opts, :timeout)
      {:ok, aborted_indices, []}
    end
  end

  defp mock_successful_log_push do
    fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end
  end

  defp mock_sequencer_notify do
    fn sequencer, commit_version, _opts ->
      assert sequencer == :test_sequencer
      assert commit_version == Version.from_integer(100)
      :ok
    end
  end

  describe "finalize_batch/2" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)

      %{transaction_system_layout: transaction_system_layout, log_server: log_server, routing_data: routing_data}
    end

    test "exits when resolver is unavailable", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      batch = Support.create_test_batch(100, 99)

      # Test error case when resolve_transactions fails
      result =
        Finalization.finalize_batch(batch,
          epoch: 1,
          sequencer: :test_sequencer,
          resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
          routing_data: routing_data
        )

      assert result == {:error, {:resolver_unavailable, :unavailable}}
    end

    test "handles batch with aborted transactions", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      # Create test transactions
      tx1_map = create_test_transaction(<<"key1">>, <<"value1">>)
      tx2_map = create_test_transaction(<<"key2">>, <<"value2">>)
      tx1_binary = Transaction.encode(tx1_map)
      tx2_binary = Transaction.encode(tx2_map)

      # Create reply functions and tasks
      reply_fn1 = create_reply_fn(self(), :reply1)
      reply_fn2 = create_reply_fn(self(), :reply2)
      task1 = Task.async(fn -> %{:test_resolver => tx1_binary} end)
      task2 = Task.async(fn -> %{:test_resolver => tx2_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          # index 0 - will be aborted
          {reply_fn1, tx1_binary, task1},
          # index 1 - success
          {reply_fn2, tx2_binary, task2}
        ])

      # Mock resolver that aborts first transaction (index 0)
      mock_resolver_fn =
        [0]
        |> mock_resolver_with_aborts()
        |> then(fn base_fn ->
          fn resolver, epoch, last_version, commit_version, summaries, metadata_per_tx, opts ->
            assert length(summaries) == 2
            base_fn.(resolver, epoch, last_version, commit_version, summaries, metadata_per_tx, opts)
          end
        end)

      assert {:ok, 1, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_successful_log_push(),
                 sequencer_notify_fn: mock_sequencer_notify(),
                 routing_data: routing_data
               )

      expected_version = Version.from_integer(100)
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:ok, ^expected_version, _}}
    end

    test "handles empty batch", %{transaction_system_layout: transaction_system_layout, routing_data: routing_data} do
      batch = create_batch_with_transactions(100, 99, [])

      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        assert resolver == :test_resolver
        {:ok, [], []}
      end

      assert {:ok, 0, 0, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_successful_log_push(),
                 sequencer_notify_fn: fn sequencer, _commit_version, _opts ->
                   assert sequencer == :test_sequencer
                   :ok
                 end,
                 routing_data: routing_data
               )
    end

    test "handles all transactions aborted", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      # Create test transactions
      tx1_map = create_test_transaction(<<"key1">>, <<"value1">>)
      tx2_map = create_test_transaction(<<"key2">>, <<"value2">>)
      tx1_binary = Transaction.encode(tx1_map)
      tx2_binary = Transaction.encode(tx2_map)

      # Create reply functions and tasks
      reply_fn1 = create_reply_fn(self(), :reply1)
      reply_fn2 = create_reply_fn(self(), :reply2)
      task1 = Task.async(fn -> %{:test_resolver => tx1_binary} end)
      task2 = Task.async(fn -> %{:test_resolver => tx2_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn1, tx1_binary, task1},
          {reply_fn2, tx2_binary, task2}
        ])

      # Mock resolver that aborts both transactions
      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        assert resolver == :test_resolver
        {:ok, [0, 1], []}
      end

      assert {:ok, 2, 0, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_successful_log_push(),
                 sequencer_notify_fn: fn sequencer, _commit_version, _opts ->
                   assert sequencer == :test_sequencer
                   :ok
                 end,
                 routing_data: routing_data
               )

      # Both should be aborted
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:error, :aborted}}
    end

    test "handles log failure and returns error", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      batch = Support.create_test_batch(100, 99)

      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        assert resolver == :test_resolver
        {:ok, [], []}
      end

      mock_log_push_fn = fn _last_version, _tx_by_log, _commit_version, _opts ->
        {:error, {:log_failures, [{"log_1", :timeout}]}}
      end

      assert {:error, {:log_failures, [{"log_1", :timeout}]}} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_log_push_fn,
                 routing_data: routing_data
               )

      # Transaction should be aborted due to log failure
      assert_receive {:reply, {:error, :aborted}}
    end

    test "returns insufficient_acknowledgments error when not all logs respond", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      batch = Support.create_test_batch(100, 99)

      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        assert resolver == :test_resolver
        {:ok, [], []}
      end

      mock_log_push_fn = fn _last_version, _tx_by_log, _commit_version, _opts ->
        {:error, {:insufficient_acknowledgments, 2, 3, [{"log_3", :timeout}]}}
      end

      assert {:error, {:insufficient_acknowledgments, 2, 3, [{"log_3", :timeout}]}} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_log_push_fn,
                 routing_data: routing_data
               )

      # Transaction should be aborted due to insufficient acknowledgments
      assert_receive {:reply, {:error, :aborted}}
    end

    test "passes correct last_commit_version from batch to resolvers and logs", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      # Create batch with NON-SEQUENTIAL version numbers to test version chain integrity
      commit_version = 150
      # Intentional gap to test proper version chain
      last_commit_version = 142

      batch = Support.create_test_batch(commit_version, last_commit_version)
      commit_version_binary = Version.from_integer(commit_version)
      last_commit_version_binary = Version.from_integer(last_commit_version)
      test_pid = self()

      # Mock resolver that captures version parameters
      mock_resolver_fn = fn _epoch,
                            _resolvers,
                            last_version,
                            received_commit_version,
                            _summaries,
                            _metadata_per_tx,
                            _opts ->
        send(test_pid, {:resolver_called, last_version, received_commit_version})
        {:ok, [], []}
      end

      # Mock log push function that captures version parameters
      mock_log_push_fn = fn last_version, _tx_by_log, received_commit_version, _opts ->
        send(test_pid, {:log_push_called, last_version, received_commit_version})
        :ok
      end

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: mock_log_push_fn,
                 sequencer_notify_fn: fn sequencer, _commit_version, _opts ->
                   assert sequencer == :test_sequencer
                   :ok
                 end,
                 routing_data: routing_data
               )

      # Verify both resolver and log push received correct versions
      assert_receive {:resolver_called, ^last_commit_version_binary, ^commit_version_binary}
      assert_receive {:log_push_called, ^last_commit_version_binary, ^commit_version_binary}
      assert_receive {:reply, {:ok, ^commit_version_binary, _}}
    end
  end

  describe "finalize_batch/4 with dependency injection" do
    test "uses injected abort_reply_fn" do
      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<0>>, :test_resolver}],
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}},
        storage_teams: [%{tag: 0, key_range: {<<>>, <<0xFF, 0xFF>>}, storage_ids: ["storage_1"]}]
      }

      routing_data = Support.build_routing_data(transaction_system_layout)

      # Create test transaction
      transaction_map = create_test_transaction(<<"key">>, <<"value">>)
      binary_transaction = Transaction.encode(transaction_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => binary_transaction} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, binary_transaction, task}
        ])

      # Custom abort function that tracks calls
      test_pid = self()

      custom_abort_fn = fn reply_fns ->
        send(test_pid, {:custom_abort_called, length(reply_fns)})
        Enum.each(reply_fns, & &1.({:error, :custom_abort}))
      end

      # Mock resolver that fails
      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        assert resolver == :test_resolver
        {:error, :timeout}
      end

      assert {:error, {:resolver_unavailable, :timeout}} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 abort_reply_fn: custom_abort_fn,
                 routing_data: routing_data
               )

      assert_receive {:custom_abort_called, 1}
      assert_receive {:reply, {:error, :custom_abort}}
    end
  end
end
