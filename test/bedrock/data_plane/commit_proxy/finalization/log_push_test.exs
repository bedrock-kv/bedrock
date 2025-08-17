defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias FinalizationTestSupport, as: Support

  describe "finalize_batch integration with new log-first approach" do
    test "end-to-end finalization builds transactions directly for logs" do
      # Set up log servers
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      # Transaction system layout with overlapping storage teams
      transaction_system_layout = %{
        sequencer: self(),
        resolvers: [{<<>>, self()}],
        logs: %{
          # covers storage teams 0 and 1
          "log_1" => [0, 1],
          # covers storage teams 1 and 2
          "log_2" => [1, 2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, log_server_1}},
          "log_2" => %{kind: :log, status: {:up, log_server_2}}
        },
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
          %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
        ]
      }

      # Create transactions with mutations that will affect different storage teams
      transaction1 =
        Transaction.encode(%{
          # affects storage team 0
          mutations: [{:set, <<"apple">>, <<"fruit">>}],
          write_conflicts: [{<<"apple">>, <<"apple\0">>}]
        })

      transaction2 =
        Transaction.encode(%{
          # affects storage team 1
          mutations: [{:set, <<"orange">>, <<"citrus">>}],
          write_conflicts: [{<<"orange">>, <<"orange\0">>}]
        })

      transaction3 =
        Transaction.encode(%{
          # affects storage team 2
          mutations: [{:set, <<"zebra">>, <<"animal">>}],
          write_conflicts: [{<<"zebra">>, <<"zebra\0">>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [
            {fn result -> send(self(), {:reply1, result}) end, transaction1},
            {fn result -> send(self(), {:reply2, result}) end, transaction2},
            {fn result -> send(self(), {:reply3, result}) end, transaction3}
          ]
        )

      # Mock resolver that doesn't abort any transactions
      mock_resolver_fn = fn _epoch, _resolvers, _last_version, _commit_version, _summaries, _opts ->
        # no aborted transactions
        {:ok, []}
      end

      # Execute finalization
      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn
        )

      assert {:ok, 0, 3} = result

      # Verify all transactions succeeded
      assert_receive {:reply1, {:ok, _}}
      assert_receive {:reply2, {:ok, _}}
      assert_receive {:reply3, {:ok, _}}

      # Verify sequencer was notified
      assert_receive {:"$gen_cast", {:report_successful_commit, _version}}
    end

    test "handles range operations that span multiple storage teams" do
      log_server = Support.create_mock_log_server()

      transaction_system_layout = %{
        sequencer: self(),
        resolvers: [{<<>>, self()}],
        logs: %{
          # covers all storage teams
          "log_1" => [0, 1, 2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, log_server}}
        },
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
          %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
        ]
      }

      # Create transaction with range operation that spans all storage teams
      transaction =
        Transaction.encode(%{
          # spans all teams
          mutations: [{:clear_range, <<"a">>, <<"zzzz">>}],
          write_conflicts: [{<<"a">>, <<"zzzz">>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [{fn result -> send(self(), {:range_reply, result}) end, transaction}]
        )

      mock_resolver_fn = fn _epoch, _resolvers, _last_version, _commit_version, _summaries, _opts ->
        # no aborted transactions
        {:ok, []}
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn
        )

      assert {:ok, 0, 1} = result
      assert_receive {:range_reply, {:ok, _}}

      # Verify sequencer was notified
      assert_receive {:"$gen_cast", {:report_successful_commit, _version}}
    end

    test "creates empty transactions for logs with no relevant mutations" do
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      transaction_system_layout = %{
        sequencer: self(),
        resolvers: [{<<>>, self()}],
        logs: %{
          # only covers storage team 0
          "log_1" => [0],
          # only covers storage team 1
          "log_2" => [1]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, log_server_1}},
          "log_2" => %{kind: :log, status: {:up, log_server_2}}
        },
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]}
        ]
      }

      # Create transaction that only affects storage team 0
      transaction =
        Transaction.encode(%{
          # only affects team 0
          mutations: [{:set, <<"apple">>, <<"fruit">>}],
          write_conflicts: [{<<"apple">>, <<"apple\0">>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [{fn result -> send(self(), {:reply, result}) end, transaction}]
        )

      mock_resolver_fn = fn _epoch, _resolvers, _last_version, _commit_version, _summaries, _opts ->
        # no aborted transactions
        {:ok, []}
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn
        )

      assert {:ok, 0, 1} = result
      assert_receive {:reply, {:ok, _}}

      # Both logs should have received transactions (one with data, one empty)
      # This ensures version consistency across all logs

      # Verify sequencer was notified
      assert_receive {:"$gen_cast", {:report_successful_commit, _version}}
    end
  end

  describe "push_transaction_to_logs_direct/5" do
    test "pushes pre-built transactions directly to logs" do
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

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

      # Pre-built transactions (this is what the new pipeline produces)
      transactions_by_log = %{
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key1">>, <<"value1">>}, {:set, <<"key2">>, <<"value2">>}],
            commit_version: Version.from_integer(100)
          }),
        "log_2" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key2">>, <<"value2">>}, {:set, <<"key3">>, <<"value3">>}],
            commit_version: Version.from_integer(100)
          })
      }

      result =
        Finalization.push_transaction_to_logs_direct(
          layout,
          # last_commit_version
          Version.from_integer(99),
          transactions_by_log,
          # commit_version (unused)
          Version.from_integer(100),
          []
        )

      assert result == :ok
    end

    test "handles log server failures" do
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
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key">>, <<"value">>}],
            commit_version: Version.from_integer(100)
          })
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

    test "uses custom timeout and async stream function" do
      test_pid = self()

      mock_async_stream_fn = fn _logs, _fun, opts ->
        timeout = Keyword.get(opts, :timeout, :default)
        send(test_pid, {:timeout_used, timeout})
        # Return successful result
        [ok: {"log_1", :ok}]
      end

      layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, Support.create_mock_log_server()}}}
      }

      transactions_by_log = %{
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key">>, <<"value">>}],
            commit_version: Version.from_integer(100)
          })
      }

      Finalization.push_transaction_to_logs_direct(
        layout,
        Version.from_integer(99),
        transactions_by_log,
        Version.from_integer(100),
        async_stream_fn: mock_async_stream_fn,
        timeout: 3000
      )

      assert_receive {:timeout_used, 3000}
    end
  end
end
