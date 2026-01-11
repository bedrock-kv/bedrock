defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  # Common test helpers
  defp build_log_services(layout) do
    layout.logs
    |> Map.keys()
    |> Enum.reduce(%{}, fn log_id, acc ->
      case Map.get(layout.services, log_id) do
        %{kind: :log, status: {:up, pid}} when is_pid(pid) -> Map.put(acc, log_id, pid)
        _ -> acc
      end
    end)
  end

  defp create_standard_layout(log_servers) do
    %{
      sequencer: :test_sequencer,
      resolvers: [{<<>>, :test_resolver}],
      logs: %{
        "log_1" => [0, 1],
        "log_2" => [1, 2]
      },
      services: %{
        "log_1" => %{kind: :log, status: {:up, Enum.at(log_servers, 0)}},
        "log_2" => %{kind: :log, status: {:up, Enum.at(log_servers, 1)}}
      },
      storage_teams: [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
        %{tag: 2, key_range: {<<"z">>, <<0xFF, 0xFF>>}, storage_ids: ["storage_3"]}
      ]
    }
  end

  defp create_simple_transaction(key, value) do
    Transaction.encode(%{
      mutations: [{:set, key, value}],
      write_conflicts: [{key, key <> <<0>>}]
    })
  end

  defp expect_standard_calls(_test_pid) do
    {
      fn :test_sequencer, _commit_version, _opts -> :ok end,
      fn :test_resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        {:ok, [], []}
      end
    }
  end

  describe "finalize_batch integration with new log-first approach" do
    test "end-to-end finalization builds transactions directly for logs" do
      log_servers = [Support.create_mock_log_server(), Support.create_mock_log_server()]
      {sequencer_notify_fn, resolver_fn} = expect_standard_calls(self())
      transaction_system_layout = create_standard_layout(log_servers)

      transactions = [
        create_simple_transaction(<<"apple">>, <<"fruit">>),
        create_simple_transaction(<<"orange">>, <<"citrus">>),
        create_simple_transaction(<<"zebra">>, <<"animal">>)
      ]

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [
            {fn result -> send(self(), {:reply1, result}) end, Enum.at(transactions, 0)},
            {fn result -> send(self(), {:reply2, result}) end, Enum.at(transactions, 1)},
            {fn result -> send(self(), {:reply3, result}) end, Enum.at(transactions, 2)}
          ]
        )

      routing_data = Support.build_routing_data(transaction_system_layout)

      assert {:ok, 0, 3, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn,
                 routing_data: routing_data
               )

      assert_receive {:reply1, {:ok, _, _}}
      assert_receive {:reply2, {:ok, _, _}}
      assert_receive {:reply3, {:ok, _, _}}
    end

    test "handles range operations that span multiple storage teams" do
      log_server = Support.create_mock_log_server()
      {sequencer_notify_fn, resolver_fn} = expect_standard_calls(self())

      layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{"log_1" => [0, 1, 2]},
        services: %{"log_1" => %{kind: :log, status: {:up, log_server}}},
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
          %{tag: 2, key_range: {<<"z">>, <<0xFF, 0xFF>>}, storage_ids: ["storage_3"]}
        ]
      }

      transaction =
        Transaction.encode(%{
          mutations: [{:clear_range, <<"a">>, <<"zzzz">>}],
          write_conflicts: [{<<"a">>, <<"zzzz">>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [{fn result -> send(self(), {:range_reply, result}) end, transaction}]
        )

      routing_data = Support.build_routing_data(layout)

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: layout.sequencer,
                 resolver_layout: ResolverLayout.from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn,
                 routing_data: routing_data
               )

      assert_receive {:range_reply, {:ok, _, _}}
    end

    test "creates empty transactions for logs with no relevant mutations" do
      log_servers = [Support.create_mock_log_server(), Support.create_mock_log_server()]
      {sequencer_notify_fn, resolver_fn} = expect_standard_calls(self())

      layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{"log_1" => [0], "log_2" => [1]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, Enum.at(log_servers, 0)}},
          "log_2" => %{kind: :log, status: {:up, Enum.at(log_servers, 1)}}
        },
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]}
        ]
      }

      transaction = create_simple_transaction(<<"apple">>, <<"fruit">>)

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [{fn result -> send(self(), {:reply, result}) end, transaction}]
        )

      routing_data = Support.build_routing_data(layout)

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: layout.sequencer,
                 resolver_layout: ResolverLayout.from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn,
                 routing_data: routing_data
               )

      assert_receive {:reply, {:ok, _, _}}
      # Both logs receive transactions (one with data, one empty) for version consistency
    end

    test "integration test with resolver aborting some transactions" do
      log_servers = [Support.create_mock_log_server(), Support.create_mock_log_server()]
      layout = create_standard_layout(log_servers)
      expected_version = Version.from_integer(100)
      last_version = Version.from_integer(99)

      # Mock resolver that aborts the second transaction (index 1) with comprehensive validation
      resolver_fn = fn :test_resolver, 1, ^last_version, ^expected_version, summaries, _metadata_per_tx, opts ->
        assert length(summaries) == 3
        assert Keyword.has_key?(opts, :timeout)
        # Abort middle transaction
        {:ok, [1], []}
      end

      sequencer_notify_fn = fn :test_sequencer, ^expected_version, _opts -> :ok end

      transactions = [
        Transaction.encode(%{
          mutations: [{:set, <<"apple">>, <<"apple">>}],
          read_conflicts: [],
          write_conflicts: [{<<"apple">>, <<"apple", 0>>}]
        }),
        Transaction.encode(%{
          mutations: [{:set, <<"orange">>, <<"orange">>}],
          read_conflicts: [],
          write_conflicts: [{<<"orange">>, <<"orange", 0>>}]
        }),
        Transaction.encode(%{
          mutations: [{:set, <<"zebra">>, <<"zebra">>}],
          read_conflicts: [],
          write_conflicts: [{<<"zebra">>, <<"zebra", 0>>}]
        })
      ]

      batch =
        Support.create_test_batch(
          expected_version,
          last_version,
          [
            {fn result -> send(self(), {:reply1, result}) end, Enum.at(transactions, 0)},
            {fn result -> send(self(), {:reply2, result}) end, Enum.at(transactions, 1)},
            {fn result -> send(self(), {:reply3, result}) end, Enum.at(transactions, 2)}
          ]
        )

      routing_data = Support.build_routing_data(layout)

      # Should have 1 abort (transaction 1) and 2 successes (transactions 0, 2)
      assert {:ok, 1, 2, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: layout.sequencer,
                 resolver_layout: ResolverLayout.from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn,
                 routing_data: routing_data
               )

      # Verify correct replies - transaction 1 should be aborted, others succeed
      assert_receive {:reply1, {:ok, ^expected_version, _}}
      assert_receive {:reply2, {:error, :aborted}}
      assert_receive {:reply3, {:ok, ^expected_version, _}}
    end
  end

  describe "push_transaction_to_logs_direct/4" do
    test "pushes pre-built transactions directly to logs" do
      log_servers = [Support.create_mock_log_server(), Support.create_mock_log_server()]

      layout = %{
        logs: %{"log_1" => [0, 1], "log_2" => [1, 2]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, Enum.at(log_servers, 0)}},
          "log_2" => %{kind: :log, status: {:up, Enum.at(log_servers, 1)}}
        }
      }

      commit_version = Version.from_integer(100)

      transactions_by_log = %{
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key1">>, <<"value1">>}, {:set, <<"key2">>, <<"value2">>}],
            commit_version: commit_version
          }),
        "log_2" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key2">>, <<"value2">>}, {:set, <<"key3">>, <<"value3">>}],
            commit_version: commit_version
          })
      }

      assert :ok =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )
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
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, failing_log_server}}}
      }

      commit_version = Version.from_integer(100)

      transactions_by_log = %{
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key">>, <<"value">>}],
            commit_version: commit_version
          })
      }

      assert {:error, {:log_failures, [{"log_1", :disk_full}]}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )
    end

    test "uses custom timeout and async stream function" do
      test_pid = self()

      mock_async_stream_fn = fn _logs, _fun, opts ->
        timeout = Keyword.get(opts, :timeout, :default)
        send(test_pid, {:timeout_used, timeout})
        [ok: {"log_1", :ok}]
      end

      layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, Support.create_mock_log_server()}}}
      }

      commit_version = Version.from_integer(100)

      transactions_by_log = %{
        "log_1" =>
          Transaction.encode(%{
            mutations: [{:set, <<"key">>, <<"value">>}],
            commit_version: commit_version
          })
      }

      Finalization.push_transaction_to_logs_direct(
        Version.from_integer(99),
        transactions_by_log,
        commit_version,
        log_services: build_log_services(layout),
        async_stream_fn: mock_async_stream_fn,
        timeout: 3000
      )

      assert_receive {:timeout_used, 3000}
    end
  end

  describe "partial log failures" do
    test "fails when first log of two fails" do
      failing_log =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      success_log = Support.create_mock_log_server()

      Support.ensure_process_killed(failing_log)

      layout = %{
        logs: %{"log_1" => [0], "log_2" => [1]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, failing_log}},
          "log_2" => %{kind: :log, status: {:up, success_log}}
        }
      }

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary}

      assert {:error, {:log_failures, errors}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )

      assert Enum.any?(errors, fn {log_id, _reason} -> log_id == "log_1" end)
    end

    test "fails when second log of two fails" do
      success_log = Support.create_mock_log_server()

      failing_log =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :timeout})
          end
        end)

      Support.ensure_process_killed(failing_log)

      layout = %{
        logs: %{"log_1" => [0], "log_2" => [1]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, success_log}},
          "log_2" => %{kind: :log, status: {:up, failing_log}}
        }
      }

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary}

      assert {:error, {:log_failures, errors}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )

      assert Enum.any?(errors, fn {log_id, _reason} -> log_id == "log_2" end)
    end

    test "fails when middle log of three fails" do
      success_log1 = Support.create_mock_log_server()
      success_log3 = Support.create_mock_log_server()

      failing_log =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :network_error})
          end
        end)

      Support.ensure_process_killed(failing_log)

      layout = %{
        logs: %{"log_1" => [0], "log_2" => [1], "log_3" => [2]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, success_log1}},
          "log_2" => %{kind: :log, status: {:up, failing_log}},
          "log_3" => %{kind: :log, status: {:up, success_log3}}
        }
      }

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary, "log_3" => tx_binary}

      assert {:error, {:log_failures, errors}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )

      assert Enum.any?(errors, fn {log_id, _reason} -> log_id == "log_2" end)
    end

    test "returns insufficient_acknowledgments when stream exhausted before all acks" do
      # Simulate a scenario where async stream returns fewer results than expected
      mock_async_stream_fn = fn _logs, _fun, _opts ->
        # Only return one result when two are expected
        [{:ok, {"log_1", :ok}}]
      end

      log_services = %{"log_1" => self(), "log_2" => self()}

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary}

      assert {:error, {:insufficient_acknowledgments, 1, 2, []}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: log_services,
                 async_stream_fn: mock_async_stream_fn
               )
    end

    test "handles log service marked as down" do
      success_log = Support.create_mock_log_server()

      # When a log service is down, it won't be in log_services
      # This simulates that scenario: only log_1 is available
      log_services = %{"log_1" => success_log}

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary}

      # With only one log service, push succeeds (acks = log_services count)
      result =
        Finalization.push_transaction_to_logs_direct(
          Version.from_integer(99),
          transactions_by_log,
          commit_version,
          log_services: log_services
        )

      # Should succeed because we only have 1 log service (the one that's up)
      assert result == :ok
    end

    test "reports all failures when multiple logs fail" do
      failing_log1 =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      failing_log2 =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :timeout})
          end
        end)

      Support.ensure_process_killed(failing_log1)
      Support.ensure_process_killed(failing_log2)

      layout = %{
        logs: %{"log_1" => [0], "log_2" => [1]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, failing_log1}},
          "log_2" => %{kind: :log, status: {:up, failing_log2}}
        }
      }

      commit_version = Version.from_integer(100)
      tx_binary = Transaction.encode(%{mutations: [{:set, <<"key">>, <<"value">>}], commit_version: commit_version})

      transactions_by_log = %{"log_1" => tx_binary, "log_2" => tx_binary}

      # Since reduce_while halts on first failure, we'll get one error
      assert {:error, {:log_failures, [_error]}} =
               Finalization.push_transaction_to_logs_direct(
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 log_services: build_log_services(layout)
               )
    end
  end

  describe "partial log failures in full finalization pipeline" do
    test "aborts all pending transactions when log push fails" do
      success_log = Support.create_mock_log_server()

      failing_log =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      Support.ensure_process_killed(failing_log)

      layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{"log_1" => [0], "log_2" => [1]},
        services: %{
          "log_1" => %{kind: :log, status: {:up, success_log}},
          "log_2" => %{kind: :log, status: {:up, failing_log}}
        },
        storage_teams: [
          %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
          %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]}
        ]
      }

      transactions = [
        create_simple_transaction(<<"apple">>, <<"fruit">>),
        create_simple_transaction(<<"orange">>, <<"citrus">>)
      ]

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [
            {fn result -> send(self(), {:reply1, result}) end, Enum.at(transactions, 0)},
            {fn result -> send(self(), {:reply2, result}) end, Enum.at(transactions, 1)}
          ]
        )

      resolver_fn = fn :test_resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        {:ok, [], []}
      end

      routing_data = Support.build_routing_data(layout)

      assert {:error, {:log_failures, _}} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: layout.sequencer,
                 resolver_layout: ResolverLayout.from_layout(layout),
                 resolver_fn: resolver_fn,
                 routing_data: routing_data
               )

      # Both transactions should be aborted due to log failure
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:error, :aborted}}
    end
  end
end
