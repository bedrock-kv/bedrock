defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Log.Transaction
  alias FinalizationTestSupport, as: Support

  describe "push_transaction_to_logs" do
    test "uses custom timeout option" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key">> => <<"value">>})
      }

      all_logs_reached = fn _version -> :ok end

      # Mock that tracks timeout usage
      test_pid = self()

      mock_async_stream_fn = fn _logs, _fun, opts ->
        timeout = Keyword.get(opts, :timeout, :default)
        send(test_pid, {:timeout_used, timeout})
        # Return successful result
        [ok: {"log_1", :ok}]
      end

      Finalization.push_transaction_to_logs(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        async_stream_fn: mock_async_stream_fn,
        # Custom timeout
        timeout: 2500
      )

      assert_receive {:timeout_used, 2500}
    end

    test "uses custom log_push_fn with success and failure scenarios" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key">> => <<"value">>})
      }

      all_logs_reached = fn _version -> :ok end
      test_pid = self()

      # Test success case
      custom_log_push_fn_success = fn service_descriptor, encoded_transaction, last_version ->
        send(
          test_pid,
          {:custom_push_called, service_descriptor, encoded_transaction, last_version}
        )

        :ok
      end

      result_success =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached,
          log_push_fn: custom_log_push_fn_success
        )

      assert result_success == :ok
      assert_receive {:custom_push_called, _service_descriptor, _encoded_transaction, 99}

      # Test failure case
      custom_log_push_fn_failure = fn _service_descriptor, _encoded_transaction, _last_version ->
        {:error, :custom_failure}
      end

      result_failure =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached,
          log_push_fn: custom_log_push_fn_failure
        )

      assert {:error, {:log_failures, [{"log_1", :custom_failure}]}} = result_failure
    end

    test "aborts immediately on first log failure" do
      transaction_system_layout = %{
        logs: %{
          "log_1" => [0],
          "log_2" => [1],
          "log_3" => [2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, self()}},
          "log_2" => %{kind: :log, status: {:up, self()}},
          "log_3" => %{kind: :log, status: {:up, self()}}
        }
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key1">> => <<"value1">>})
      }

      all_logs_reached = fn _version -> :ok end

      # Mock that returns first log failure, others would succeed
      mock_async_stream_fn =
        Support.mock_async_stream_with_responses(%{
          "log_1" => {:error, :first_failure},
          "log_2" => :ok,
          "log_3" => :ok
        })

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached,
          async_stream_fn: mock_async_stream_fn
        )

      # Should fail immediately on first error
      assert {:error, {:log_failures, [{"log_1", :first_failure}]}} = result
    end

    test "pushes transactions to single log and calls callback" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, :ok)
          end
        end)

      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, log_server}}}
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key">> => <<"value">>})
      }

      test_pid = self()
      all_logs_reached = Support.create_all_logs_reached_callback(test_pid)

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached
        )

      assert result == :ok
      assert_receive {:all_logs_reached, 100}
      Support.ensure_process_killed(log_server)
    end

    test "handles empty transactions_by_tag (all aborted)" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, :ok)
          end
        end)

      transaction_system_layout = %{
        logs: %{"log_1" => [0, 1]},
        services: %{"log_1" => %{kind: :log, status: {:up, log_server}}}
      }

      # Empty transactions_by_tag - all transactions were aborted
      transactions_by_tag = %{}

      test_pid = self()
      all_logs_reached = Support.create_all_logs_reached_callback(test_pid)

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached
        )

      assert result == :ok
      assert_receive {:all_logs_reached, 100}
      Support.ensure_process_killed(log_server)
    end

    test "handles multiple logs requiring ALL to succeed and failures" do
      transaction_system_layout = Support.multi_log_transaction_system_layout()

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key1">> => <<"value1">>}),
        1 => Transaction.new(100, %{<<"key2">> => <<"value2">>})
      }

      test_pid = self()
      all_logs_reached = Support.create_all_logs_reached_callback(test_pid)

      # Mock async stream that simulates 2/3 success (but we need ALL now)
      mock_async_stream_fn =
        Support.mock_async_stream_with_responses(%{
          "log_1" => :ok,
          "log_2" => :ok,
          # One failure
          "log_3" => {:error, :unavailable}
        })

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          99,
          transactions_by_tag,
          100,
          all_logs_reached,
          async_stream_fn: mock_async_stream_fn
        )

      # Should fail because we need ALL logs now, not just majority
      assert {:error, {:log_failures, [{"log_3", :unavailable}]}} = result
      # All logs must succeed
      refute_receive {:all_logs_reached, 100}
    end
  end
end
