defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogOperationsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  # Common test setup helpers
  defp create_single_log_layout(log_server \\ nil) do
    log_server = log_server || Support.create_mock_log_server()

    %{
      logs: %{"log_1" => [0]},
      services: %{"log_1" => %{kind: :log, status: {:up, log_server}}}
    }
  end

  defp create_multi_log_layout do
    log_server_1 = Support.create_mock_log_server()
    log_server_2 = Support.create_mock_log_server()

    %{
      logs: %{
        "log_1" => [0, 1],
        "log_2" => [1, 2]
      },
      services: %{
        "log_1" => %{kind: :log, status: {:up, log_server_1}},
        "log_2" => %{kind: :log, status: {:up, log_server_2}}
      }
    }
  end

  defp create_failing_log_server(error_reason) do
    server =
      spawn(fn ->
        receive do
          {:"$gen_call", from, {:push, _authority, _transaction, _last_version, _kcv}} ->
            GenServer.reply(from, {:error, error_reason})
        end
      end)

    Support.ensure_process_killed(server)
    server
  end

  defp call_push_direct(layout, transactions_by_log, commit_version \\ 100, last_version \\ 99) do
    # Build log_services from layout
    log_services =
      layout.logs
      |> Map.keys()
      |> Enum.reduce(%{}, fn log_id, acc ->
        case Map.get(layout.services, log_id) do
          %{kind: :log, status: {:up, pid}} -> Map.put(acc, log_id, pid)
          _ -> acc
        end
      end)

    Finalization.push_transaction_to_logs_direct(
      Version.from_integer(last_version),
      transactions_by_log,
      Version.from_integer(commit_version),
      log_services: log_services,
      recovery_authority: %{generation: 1, recovery_id: "commit-proxy-test"}
    )
  end

  describe "push_transaction_to_logs_direct/4" do
    test "pushes pre-built transactions directly to logs" do
      layout = create_multi_log_layout()

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

      assert :ok = call_push_direct(layout, transactions_by_log)
    end

    test "handles empty transactions" do
      layout = create_single_log_layout()
      transactions_by_log = %{"log_1" => TransactionTestSupport.new_log_transaction(100, %{})}

      assert :ok = call_push_direct(layout, transactions_by_log)
    end

    test "returns error when log server fails" do
      failing_server = create_failing_log_server(:disk_full)
      layout = create_single_log_layout(failing_server)
      transactions_by_log = %{"log_1" => TransactionTestSupport.new_log_transaction(100, %{"key" => "value"})}

      assert {:error, {:log_failures, [{"log_1", :disk_full}]}} = call_push_direct(layout, transactions_by_log)
    end
  end
end
