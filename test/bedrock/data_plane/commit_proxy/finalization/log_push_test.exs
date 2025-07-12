defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Log.Transaction
  alias FinalizationTestSupport, as: Support

  describe "push_transaction_to_logs/5" do
    test "pushes transactions to single log and calls callback" do
      log_server = spawn(fn ->
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

      result = Finalization.push_transaction_to_logs(
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

    test "handles multiple logs requiring ALL to succeed" do
      transaction_system_layout = Support.multi_log_transaction_system_layout()

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key1">> => <<"value1">>}),
        1 => Transaction.new(100, %{<<"key2">> => <<"value2">>})
      }

      test_pid = self()
      all_logs_reached = Support.create_all_logs_reached_callback(test_pid)

      # Mock async stream that simulates 2/3 success (but we need ALL now)
      mock_async_stream_fn = Support.mock_async_stream_with_responses(%{
        "log_1" => :ok,
        "log_2" => :ok,
        "log_3" => {:error, :unavailable}  # One failure
      })

      result = Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        async_stream_fn: mock_async_stream_fn
      )

      # Should fail because we need ALL logs now, not just majority
      assert {:error, {:log_failures, [{"log_3", :unavailable}]}} = result
      refute_receive {:all_logs_reached, 100}  # All logs must succeed
    end

    test "handles empty transactions_by_tag (all aborted)" do
      log_server = spawn(fn ->
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

      result = Finalization.push_transaction_to_logs(
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

    test "returns error when not enough logs respond" do
      transaction_system_layout = %{
        logs: %{
          "log_1" => [0],
          "log_2" => [1]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, self()}},
          "log_2" => %{kind: :log, status: {:up, self()}}
        }
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key1">> => <<"value1">>})
      }

      all_logs_reached = fn _version -> :ok end

      # Mock async stream that simulates timeouts/failures
      mock_async_stream_fn = Support.mock_async_stream_with_responses(%{
        "log_1" => {:error, :timeout},      # Simulate timeout
        "log_2" => {:error, :unavailable}   # Simulate failure
      })

      result = Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        async_stream_fn: mock_async_stream_fn
      )

      # Should fail due to not enough successful responses (0/2, need 2/2)
      assert {:error, {:log_failures, [{"log_1", :timeout}]}} = result
    end

    test "succeeds when all logs respond successfully" do
      transaction_system_layout = %{
        logs: %{
          "log_1" => [0],
          "log_2" => [1]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, self()}},
          "log_2" => %{kind: :log, status: {:up, self()}}
        }
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key1">> => <<"value1">>}),
        1 => Transaction.new(100, %{<<"key2">> => <<"value2">>})
      }

      test_pid = self()
      all_logs_reached = Support.create_all_logs_reached_callback(test_pid)

      # Mock async stream that simulates all success
      mock_async_stream_fn = Support.mock_async_stream_with_responses(%{
        "log_1" => :ok,
        "log_2" => :ok
      })

      result = Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        async_stream_fn: mock_async_stream_fn
      )

      assert result == :ok
      assert_receive {:all_logs_reached, 100}
    end
  end

  describe "push_transaction_to_logs_with_opts/6" do
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

      Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        async_stream_fn: mock_async_stream_fn,
        timeout: 2500  # Custom timeout
      )

      assert_receive {:timeout_used, 2500}
    end

    test "uses custom log_push_fn" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key">> => <<"value">>})
      }

      all_logs_reached = fn _version -> :ok end

      # Custom log push function that always succeeds
      test_pid = self()
      custom_log_push_fn = fn service_descriptor, encoded_transaction, last_version ->
        send(test_pid, {:custom_push_called, service_descriptor, encoded_transaction, last_version})
        :ok
      end

      result = Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        log_push_fn: custom_log_push_fn
      )

      assert result == :ok
      assert_receive {:custom_push_called, _service_descriptor, _encoded_transaction, 99}
    end

    test "handles failures from custom log_push_fn" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key">> => <<"value">>})
      }

      all_logs_reached = fn _version -> :ok end

      # Custom log push function that always fails
      custom_log_push_fn = fn _service_descriptor, _encoded_transaction, _last_version ->
        {:error, :custom_failure}
      end

      result = Finalization.push_transaction_to_logs_with_opts(
        transaction_system_layout,
        99,
        transactions_by_tag,
        100,
        all_logs_reached,
        log_push_fn: custom_log_push_fn
      )

      assert {:error, {:log_failures, [{"log_1", :custom_failure}]}} = result
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
      mock_async_stream_fn = Support.mock_async_stream_with_responses(%{
        "log_1" => {:error, :first_failure},
        "log_2" => :ok,
        "log_3" => :ok
      })

      result = Finalization.push_transaction_to_logs_with_opts(
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
  end
end