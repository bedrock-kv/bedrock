defmodule Bedrock.DataPlane.CommitProxy.Finalization.ResolverRetryTest do
  @moduledoc """
  Tests for resolver retry logic in the finalization pipeline.

  These tests verify:
  - Retry behavior on transient failures (timeout, unavailable)
  - Retry exhaustion after max_attempts
  - Non-retryable errors fail immediately
  - Exponential backoff timeout calculation
  - Telemetry events during retry
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

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

  describe "resolver retry on transient failures" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)
      %{transaction_system_layout: transaction_system_layout, routing_data: routing_data}
    end

    test "retries on timeout and succeeds on subsequent attempt", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      # Track call attempts
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        if attempt < 2 do
          {:error, :timeout}
        else
          {:ok, [], []}
        end
      end

      batch = Support.create_test_batch(100, 99)

      # Should succeed after retry
      assert {:ok, 0, 1, _metadata} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: fn _layout, _last_version, _tx_by_tag, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _commit_version, _opts -> :ok end,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      # Verify retry happened
      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
      refute_receive {:resolver_attempt, 3}
    end

    test "retries on unavailable and succeeds on subsequent attempt", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        if attempt < 2 do
          {:error, :unavailable}
        else
          {:ok, [], []}
        end
      end

      batch = Support.create_test_batch(100, 99)

      assert {:ok, 0, 1, _metadata} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: fn _layout, _last_version, _tx_by_tag, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _commit_version, _opts -> :ok end,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
    end

    test "handles failure tuple format with retry", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        if attempt < 2 do
          # Alternative failure format used by some resolver implementations
          {:failure, :timeout, :test_resolver}
        else
          {:ok, [], []}
        end
      end

      batch = Support.create_test_batch(100, 99)

      assert {:ok, 0, 1, _metadata} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: fn _layout, _last_version, _tx_by_tag, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _commit_version, _opts -> :ok end,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
    end
  end

  describe "retry exhaustion" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)
      %{transaction_system_layout: transaction_system_layout, routing_data: routing_data}
    end

    test "fails after max_attempts exhausted with timeout", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        # Always fail with timeout
        {:error, :timeout}
      end

      batch = Support.create_test_batch(100, 99)

      assert {:error, {:resolver_unavailable, :timeout}} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      # Should have attempted exactly 3 times
      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
      assert_receive {:resolver_attempt, 3}
      refute_receive {:resolver_attempt, 4}

      # Transaction should be aborted
      assert_receive {:reply, {:error, :aborted}}
    end

    test "fails after max_attempts exhausted with unavailable", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        {:error, :unavailable}
      end

      batch = Support.create_test_batch(100, 99)

      assert {:error, {:resolver_unavailable, :unavailable}} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 max_attempts: 2,
                 routing_data: routing_data
               )

      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
      refute_receive {:resolver_attempt, 3}
    end

    test "fails after max_attempts with failure tuple format", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        {:failure, :unavailable, :test_resolver}
      end

      batch = Support.create_test_batch(100, 99)

      assert {:error, {:resolver_unavailable, :unavailable}} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      assert_receive {:resolver_attempt, 1}
      assert_receive {:resolver_attempt, 2}
      assert_receive {:resolver_attempt, 3}
    end
  end

  describe "non-retryable errors" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)
      %{transaction_system_layout: transaction_system_layout, routing_data: routing_data}
    end

    test "epoch_mismatch fails immediately without retry", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        {:error, {:epoch_mismatch, expected: 2, received: 1}}
      end

      batch = Support.create_test_batch(100, 99)

      assert {:error, {:epoch_mismatch, expected: 2, received: 1}} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      # Should fail immediately - only 1 attempt
      assert_receive {:resolver_attempt, 1}
      refute_receive {:resolver_attempt, 2}
    end

    test "other errors fail immediately without retry", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt})

        {:error, :some_internal_error}
      end

      batch = Support.create_test_batch(100, 99)

      assert {:error, :some_internal_error} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      # Should fail immediately
      assert_receive {:resolver_attempt, 1}
      refute_receive {:resolver_attempt, 2}
    end
  end

  describe "exponential backoff" do
    test "default_timeout_fn calculates exponential backoff" do
      # 500 * 2^0 = 500
      assert Finalization.default_timeout_fn(0) == 500
      # 500 * 2^1 = 1000
      assert Finalization.default_timeout_fn(1) == 1000
      # 500 * 2^2 = 2000
      assert Finalization.default_timeout_fn(2) == 2000
      # 500 * 2^3 = 4000
      assert Finalization.default_timeout_fn(3) == 4000
    end

    test "custom timeout_fn is used for retry timeouts" do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)

      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        timeout = Keyword.get(opts, :timeout)
        send(test_pid, {:resolver_timeout, attempt, timeout})

        if attempt < 3 do
          {:error, :timeout}
        else
          {:ok, [], []}
        end
      end

      # Custom timeout function that uses fixed timeouts
      custom_timeout_fn = fn
        0 -> 100
        1 -> 200
        2 -> 300
        _ -> 400
      end

      batch = Support.create_test_batch(100, 99)

      assert {:ok, 0, 1, _metadata} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: fn _layout, _last_version, _tx_by_tag, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _commit_version, _opts -> :ok end,
                 timeout_fn: custom_timeout_fn,
                 max_attempts: 5,
                 routing_data: routing_data
               )

      # Verify timeouts were passed correctly
      assert_receive {:resolver_timeout, 1, 100}
      assert_receive {:resolver_timeout, 2, 200}
      assert_receive {:resolver_timeout, 3, 300}
    end
  end

  describe "retry with multiple transactions" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)
      %{transaction_system_layout: transaction_system_layout, routing_data: routing_data}
    end

    test "retry preserves all transactions in batch", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        send(test_pid, {:resolver_attempt, attempt, length(summaries)})

        if attempt < 2 do
          {:error, :timeout}
        else
          {:ok, [], []}
        end
      end

      # Create batch with multiple transactions
      tx1_map = create_test_transaction(<<"key1">>, <<"value1">>)
      tx2_map = create_test_transaction(<<"key2">>, <<"value2">>)
      tx1_binary = Transaction.encode(tx1_map)
      tx2_binary = Transaction.encode(tx2_map)

      reply_fn1 = create_reply_fn(self(), :reply1)
      reply_fn2 = create_reply_fn(self(), :reply2)
      task1 = Task.async(fn -> %{:test_resolver => tx1_binary} end)
      task2 = Task.async(fn -> %{:test_resolver => tx2_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn1, tx1_binary, task1},
          {reply_fn2, tx2_binary, task2}
        ])

      assert {:ok, 0, 2, _metadata} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 [],
                 epoch: 1,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 batch_log_push_fn: fn _layout, _last_version, _tx_by_tag, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _commit_version, _opts -> :ok end,
                 max_attempts: 3,
                 routing_data: routing_data
               )

      # Both attempts should see 2 transactions
      assert_receive {:resolver_attempt, 1, 2}
      assert_receive {:resolver_attempt, 2, 2}

      # Both transactions should succeed
      expected_version = Version.from_integer(100)
      assert_receive {:reply1, {:ok, ^expected_version, _}}
      assert_receive {:reply2, {:ok, ^expected_version, _}}
    end
  end

  describe "telemetry during retry" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(transaction_system_layout)
      %{transaction_system_layout: transaction_system_layout, routing_data: routing_data}
    end

    test "emits telemetry events during retry cycle", %{
      transaction_system_layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()
      call_count = :counters.new(1, [])

      # Attach telemetry handler - note the telemetry path does NOT include :data_plane
      :telemetry.attach_many(
        "retry-test-handler",
        [
          [:bedrock, :commit_proxy, :resolver, :retry],
          [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded]
        ],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach("retry-test-handler")
      end)

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        attempt = :counters.get(call_count, 1) + 1
        :counters.put(call_count, 1, attempt)

        {:error, :timeout}
      end

      batch = Support.create_test_batch(100, 99)

      capture_log(fn ->
        assert {:error, {:resolver_unavailable, :timeout}} =
                 Finalization.finalize_batch(
                   batch,
                   transaction_system_layout,
                   [],
                   epoch: 1,
                   resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                   resolver_fn: mock_resolver_fn,
                   max_attempts: 3,
                   routing_data: routing_data
                 )
      end)

      # Should receive retry telemetry events (2 retries before max exhausted)
      assert_receive {:telemetry, [:bedrock, :commit_proxy, :resolver, :retry], _, _}
      assert_receive {:telemetry, [:bedrock, :commit_proxy, :resolver, :retry], _, _}

      # Should receive max retries exceeded event
      assert_receive {:telemetry, [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded], _, _}
    end
  end
end
