defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogOperationsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.TransactionTestSupport
  alias Bedrock.DataPlane.Version
  alias FinalizationTestSupport, as: Support

  describe "push_transaction_to_logs_direct/5" do
    test "pushes pre-built transactions directly to logs" do
      # Create mock log servers
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      # Set up transaction system layout
      layout = %{
        logs: %{
          "log_1" => [0, 1],
          "log_2" => [1, 2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, log_server_1}},
          "log_2" => %{kind: :log, status: {:up, log_server_2}}
        }
      }

      # Pre-built transactions for each log (this would come from build_transactions_for_logs)
      transactions_by_log = %{
        "log_1" =>
          TransactionTestSupport.new_log_transaction(100, %{
            "key_0" => "value_0",
            "key_1" => "value_1"
          }),
        "log_2" =>
          TransactionTestSupport.new_log_transaction(100, %{
            "key_1" => "value_1",
            "key_2" => "value_2"
          })
      }

      result =
        Finalization.push_transaction_to_logs_direct(
          layout,
          # last_commit_version
          Version.from_integer(99),
          transactions_by_log,
          # commit_version (unused in direct version)
          Version.from_integer(100),
          []
        )

      assert result == :ok
    end

    test "handles empty transactions" do
      layout = %{
        logs: %{
          "log_1" => [0]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, Support.create_mock_log_server()}}
        }
      }

      # Empty transaction
      transactions_by_log = %{
        "log_1" => TransactionTestSupport.new_log_transaction(100, %{})
      }

      result =
        Finalization.push_transaction_to_logs_direct(
          layout,
          Version.from_integer(99),
          transactions_by_log,
          Version.from_integer(100),
          []
        )

      assert result == :ok
    end

    test "returns error when log server fails" do
      # Create a failing log server
      failing_log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      Support.ensure_process_killed(failing_log_server)

      layout = %{
        logs: %{
          "log_1" => [0]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, failing_log_server}}
        }
      }

      transactions_by_log = %{
        "log_1" => TransactionTestSupport.new_log_transaction(100, %{"key" => "value"})
      }

      result =
        Finalization.push_transaction_to_logs_direct(
          layout,
          Version.from_integer(99),
          transactions_by_log,
          Version.from_integer(100),
          []
        )

      assert {:error, {:log_failures, [{"log_1", :disk_full}]}} = result
    end
  end

  describe "try_to_push_transaction_to_log/3" do
    test "succeeds when log server responds with :ok" do
      log_server = Support.create_mock_log_server()

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == :ok
    end

    test "returns error when log server is down" do
      service_descriptor = %{kind: :log, status: {:down, :some_reason}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :unavailable}
    end

    test "returns error when log server responds with error" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      Support.ensure_process_killed(log_server)

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :disk_full}
    end

    test "handles log server process exit" do
      # Create a log server that will exit immediately
      log_server = spawn(fn -> exit(:normal) end)
      # Give it time to exit
      Process.sleep(100)

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      # Should handle process exit gracefully
      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      # Should get an error when the process is dead
      assert {:error, _reason} = result
    end
  end
end
