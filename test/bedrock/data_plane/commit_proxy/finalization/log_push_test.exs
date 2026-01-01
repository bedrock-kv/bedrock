defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.LayoutOptimization
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  # Common test helpers
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
      fn :test_sequencer, _commit_version -> :ok end,
      fn :test_resolver, _epoch, _last_version, _commit_version, _summaries, _opts -> {:ok, []} end
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

      assert {:ok, 0, 3} =
               Finalization.finalize_batch(
                 batch,
                 transaction_system_layout,
                 epoch: 1,
                 precomputed: LayoutOptimization.precompute_from_layout(transaction_system_layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn
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

      assert {:ok, 0, 1} =
               Finalization.finalize_batch(
                 batch,
                 layout,
                 epoch: 1,
                 precomputed: LayoutOptimization.precompute_from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn
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

      assert {:ok, 0, 1} =
               Finalization.finalize_batch(
                 batch,
                 layout,
                 epoch: 1,
                 precomputed: LayoutOptimization.precompute_from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn
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
      resolver_fn = fn :test_resolver, 1, ^last_version, ^expected_version, summaries, opts ->
        assert length(summaries) == 3
        assert Keyword.has_key?(opts, :timeout)
        # Abort middle transaction
        {:ok, [1]}
      end

      sequencer_notify_fn = fn :test_sequencer, ^expected_version -> :ok end

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

      # Should have 1 abort (transaction 1) and 2 successes (transactions 0, 2)
      assert {:ok, 1, 2} =
               Finalization.finalize_batch(
                 batch,
                 layout,
                 epoch: 1,
                 precomputed: LayoutOptimization.precompute_from_layout(layout),
                 resolver_fn: resolver_fn,
                 sequencer_notify_fn: sequencer_notify_fn
               )

      # Verify correct replies - transaction 1 should be aborted, others succeed
      assert_receive {:reply1, {:ok, ^expected_version, _}}
      assert_receive {:reply2, {:error, :aborted}}
      assert_receive {:reply3, {:ok, ^expected_version, _}}
    end
  end

  describe "push_transaction_to_logs_direct/5" do
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
                 layout,
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 []
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
                 layout,
                 Version.from_integer(99),
                 transactions_by_log,
                 commit_version,
                 []
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
        layout,
        Version.from_integer(99),
        transactions_by_log,
        commit_version,
        async_stream_fn: mock_async_stream_fn,
        timeout: 3000
      )

      assert_receive {:timeout_used, 3000}
    end
  end
end
