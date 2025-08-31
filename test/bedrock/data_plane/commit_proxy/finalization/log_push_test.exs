defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias FinalizationTestSupport, as: Support

  describe "finalize_batch integration with new log-first approach" do
    test "end-to-end finalization builds transactions directly for logs" do
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      mock_sequencer_notify_fn = fn sequencer, _commit_version ->
        # Assert the correct sequencer would be called
        assert sequencer == :test_sequencer
        :ok
      end

      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{
          "log_1" => [0, 1],
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

      transaction1 =
        Transaction.encode(%{
          mutations: [{:set, <<"apple">>, <<"fruit">>}],
          write_conflicts: [{<<"apple">>, <<"apple\0">>}]
        })

      transaction2 =
        Transaction.encode(%{
          mutations: [{:set, <<"orange">>, <<"citrus">>}],
          write_conflicts: [{<<"orange">>, <<"orange\0">>}]
        })

      transaction3 =
        Transaction.encode(%{
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

      mock_resolver_fn = fn _epoch, _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, []}
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn,
          sequencer_notify_fn: mock_sequencer_notify_fn
        )

      assert {:ok, 0, 3} = result

      assert_receive {:reply1, {:ok, _}}
      assert_receive {:reply2, {:ok, _}}
      assert_receive {:reply3, {:ok, _}}
    end

    test "handles range operations that span multiple storage teams" do
      log_server = Support.create_mock_log_server()

      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _transaction_summaries, _opts ->
        assert resolver == :test_resolver
        {:ok, []}
      end

      mock_sequencer_notify_fn = fn sequencer, _commit_version ->
        assert sequencer == :test_sequencer
        :ok
      end

      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{
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

      # Range operation spanning all storage teams
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

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn,
          sequencer_notify_fn: mock_sequencer_notify_fn
        )

      assert {:ok, 0, 1} = result
      assert_receive {:range_reply, {:ok, _}}
    end

    test "creates empty transactions for logs with no relevant mutations" do
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      mock_resolver_fn = fn resolver, _epoch, _last_version, _commit_version, _transaction_summaries, _opts ->
        assert resolver == :test_resolver
        {:ok, []}
      end

      mock_sequencer_notify_fn = fn sequencer, _commit_version ->
        assert sequencer == :test_sequencer
        :ok
      end

      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{
          "log_1" => [0],
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

      transaction =
        Transaction.encode(%{
          mutations: [{:set, <<"apple">>, <<"fruit">>}],
          write_conflicts: [{<<"apple">>, <<"apple\0">>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [{fn result -> send(self(), {:reply, result}) end, transaction}]
        )

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn,
          sequencer_notify_fn: mock_sequencer_notify_fn
        )

      assert {:ok, 0, 1} = result
      assert_receive {:reply, {:ok, _}}

      # Both logs receive transactions (one with data, one empty) for version consistency
    end

    test "integration test with resolver aborting some transactions" do
      log_server_1 = Support.create_mock_log_server()
      log_server_2 = Support.create_mock_log_server()

      # Mock resolver that aborts the second transaction (index 1)
      mock_resolver_fn = fn resolver, epoch, last_version, commit_version, transaction_summaries, opts ->
        # Comprehensive parameter assertions
        assert resolver == :test_resolver
        assert epoch == 1
        assert last_version == Version.from_integer(99)
        assert commit_version == Version.from_integer(100)
        assert length(transaction_summaries) == 3
        assert Keyword.has_key?(opts, :timeout)

        # Abort the middle transaction (index 1)
        {:ok, [1]}
      end

      mock_sequencer_notify_fn = fn sequencer, commit_version ->
        assert sequencer == :test_sequencer
        assert commit_version == Version.from_integer(100)
        :ok
      end

      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<>>, :test_resolver}],
        logs: %{
          "log_1" => [0, 1],
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

      # Create transactions with different keys for different storage ranges
      transaction1 =
        Transaction.encode(%{
          # Range 0 (a-m)
          mutations: [{:set, <<"apple">>, <<"apple">>}],
          read_conflicts: [],
          write_conflicts: [{<<"apple">>, <<"apple", 0>>}]
        })

      transaction2 =
        Transaction.encode(%{
          # Range 1 (m-z)
          mutations: [{:set, <<"orange">>, <<"orange">>}],
          read_conflicts: [],
          write_conflicts: [{<<"orange">>, <<"orange", 0>>}]
        })

      transaction3 =
        Transaction.encode(%{
          # Range 2 (z-end)
          mutations: [{:set, <<"zebra">>, <<"zebra">>}],
          read_conflicts: [],
          write_conflicts: [{<<"zebra">>, <<"zebra", 0>>}]
        })

      batch =
        Support.create_test_batch(
          Version.from_integer(100),
          Version.from_integer(99),
          [
            {fn result -> send(self(), {:reply1, result}) end, transaction1},
            # Will be aborted
            {fn result -> send(self(), {:reply2, result}) end, transaction2},
            {fn result -> send(self(), {:reply3, result}) end, transaction3}
          ]
        )

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          epoch: 1,
          resolver_fn: mock_resolver_fn,
          sequencer_notify_fn: mock_sequencer_notify_fn
        )

      # Should have 1 abort (transaction 1) and 2 successes (transactions 0, 2)
      assert {:ok, 1, 2} = result

      # Verify correct replies - transaction 1 should be aborted, others succeed
      expected_version = Version.from_integer(100)
      assert_receive {:reply1, {:ok, ^expected_version}}
      assert_receive {:reply2, {:error, :aborted}}
      assert_receive {:reply3, {:ok, ^expected_version}}
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
          Version.from_integer(99),
          transactions_by_log,
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
