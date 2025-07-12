defmodule Bedrock.DataPlane.CommitProxy.FinalizationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Log.Transaction

  # Mock cluster module for testing
  defmodule TestCluster do
    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end
  end

  describe "resolve_transactions with injectable functions" do
    test "calls timeout function with correct attempt numbers" do
      resolvers = [{<<0>>, :test_resolver}]
      last_version = 100
      commit_version = 101
      transaction_summaries = [{nil, [<<1>>]}]
      opts = []

      test_pid = self()

      # Mock timeout function that captures attempt numbers
      timeout_fn = fn attempt ->
        send(test_pid, {:timeout_called, attempt})
        # Return a reasonable timeout
        1000
      end

      # Mock exit function that doesn't actually exit
      exit_fn = fn reason ->
        send(test_pid, {:exit_called, reason})
        throw({:test_exit, reason})
      end

      # Test that function goes through all retry attempts
      assert catch_throw(
               Finalization.resolve_transactions_with_functions(
                 resolvers,
                 last_version,
                 commit_version,
                 transaction_summaries,
                 opts,
                 timeout_fn,
                 exit_fn
               )
             ) == {:test_exit, :unavailable}

      # Verify timeout function was called for each attempt (0, 1, 2)
      assert_receive {:timeout_called, 0}
      assert_receive {:timeout_called, 1}
      assert_receive {:timeout_called, 2}
      assert_receive {:exit_called, :unavailable}
    end

    test "default timeout function provides exponential backoff" do
      # Test the default timeout function directly
      # 500 * 2^0 = 500
      assert Finalization.default_timeout_fn(0) == 500
      # 500 * 2^1 = 1000
      assert Finalization.default_timeout_fn(1) == 1000
      # 500 * 2^2 = 2000
      assert Finalization.default_timeout_fn(2) == 2000
    end

    test "emits telemetry events during retries" do
      test_pid = self()
      Process.put(:test_pid, test_pid)

      # Attach telemetry handlers using module functions to avoid warnings
      :telemetry.attach(
        "test-retry-telemetry",
        [:bedrock, :commit_proxy, :resolver, :retry],
        &__MODULE__.handle_retry_telemetry/4,
        nil
      )

      :telemetry.attach(
        "test-max-retries-telemetry",
        [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
        &__MODULE__.handle_max_retries_telemetry/4,
        nil
      )

      resolvers = [{<<0>>, :test_resolver}]
      last_version = 100
      commit_version = 101
      transaction_summaries = [{nil, [<<1>>]}]
      opts = []

      # Short timeout for faster test
      timeout_fn = fn _attempt -> 100 end
      exit_fn = fn reason -> throw({:test_exit, reason}) end

      catch_throw(
        Finalization.resolve_transactions_with_functions(
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          opts,
          timeout_fn,
          exit_fn
        )
      )

      # Should receive telemetry for retry attempts and final failure
      assert_receive {:telemetry, :retry, %{attempt: 1}, %{reason: :unavailable}}
      assert_receive {:telemetry, :retry, %{attempt: 2}, %{reason: :unavailable}}
      assert_receive {:telemetry, :max_retries, %{total_attempts: 3}, %{reason: :unavailable}}

      :telemetry.detach("test-retry-telemetry")
      :telemetry.detach("test-max-retries-telemetry")
    end
  end

  # Telemetry handlers to avoid performance warnings
  def handle_retry_telemetry(_event, measurements, metadata, _config) do
    test_pid = Process.get(:test_pid)
    send(test_pid, {:telemetry, :retry, measurements, metadata})
  end

  def handle_max_retries_telemetry(_event, measurements, metadata, _config) do
    test_pid = Process.get(:test_pid)
    send(test_pid, {:telemetry, :max_retries, measurements, metadata})
  end

  describe "key_to_tag/2" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0xFF>>, :end}, storage_ids: ["storage_3", "storage_4"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps key to correct tag for first range", %{storage_teams: storage_teams} do
      assert {:ok, 0} = Finalization.key_to_tag(<<0x01>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0x80>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE>>, storage_teams)
    end

    test "maps key to correct tag for second range", %{storage_teams: storage_teams} do
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF, 0x01>>, storage_teams)
    end

    test "returns error for empty storage teams" do
      assert {:error, :no_matching_team} = Finalization.key_to_tag(<<"any_key">>, [])
    end

    test "handles boundary conditions correctly", %{storage_teams: storage_teams} do
      # Key exactly at range boundary should belong to second range
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)

      # Key just before boundary should belong to first range
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE, 0xFF>>, storage_teams)
    end
  end

  describe "group_writes_by_tag/2" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, :end}, storage_ids: ["storage_2"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "groups writes by their target storage team tags", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>,
        <<"orange">> => <<"citrus">>,
        <<"zebra">> => <<"animal">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected = %{
        0 => %{
          <<"apple">> => <<"fruit">>,
          <<"banana">> => <<"yellow">>
        },
        1 => %{
          <<"orange">> => <<"citrus">>,
          <<"zebra">> => <<"animal">>
        }
      }

      assert result == expected
    end

    test "handles empty writes map", %{storage_teams: storage_teams} do
      result = Finalization.group_writes_by_tag(%{}, storage_teams)
      assert result == %{}
    end

    test "handles writes that all belong to same tag", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected = %{
        0 => %{
          <<"apple">> => <<"fruit">>,
          <<"banana">> => <<"yellow">>
        }
      }

      assert result == expected
    end
  end

  describe "merge_writes_by_tag/2" do
    test "merges write maps for same tags" do
      acc = %{
        0 => %{<<"key1">> => <<"value1">>},
        1 => %{<<"key2">> => <<"value2">>}
      }

      new_writes = %{
        0 => %{<<"key3">> => <<"value3">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      result = Finalization.merge_writes_by_tag(acc, new_writes)

      expected = %{
        0 => %{<<"key1">> => <<"value1">>, <<"key3">> => <<"value3">>},
        1 => %{<<"key2">> => <<"value2">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      assert result == expected
    end

    test "handles overlapping keys by taking new value" do
      acc = %{
        0 => %{<<"key1">> => <<"old_value">>}
      }

      new_writes = %{
        0 => %{<<"key1">> => <<"new_value">>}
      }

      result = Finalization.merge_writes_by_tag(acc, new_writes)

      expected = %{
        0 => %{<<"key1">> => <<"new_value">>}
      }

      assert result == expected
    end
  end

  describe "build_log_transactions/3" do
    test "builds transaction for each log based on tag coverage" do
      log_descriptors = %{
        # covers tags 0 and 1
        "log_1" => [0, 1],
        # covers tags 1 and 2
        "log_2" => [1, 2],
        # covers tag 3 only
        "log_3" => [3]
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key_0">> => <<"value_0">>}),
        1 => Transaction.new(100, %{<<"key_1">> => <<"value_1">>}),
        2 => Transaction.new(100, %{<<"key_2">> => <<"value_2">>})
      }

      result = Finalization.build_log_transactions(log_descriptors, transactions_by_tag, 100)

      # log_1 should get writes for tags 0 and 1
      log_1_writes = Transaction.key_values(result["log_1"])
      assert log_1_writes == %{<<"key_0">> => <<"value_0">>, <<"key_1">> => <<"value_1">>}

      # log_2 should get writes for tags 1 and 2
      log_2_writes = Transaction.key_values(result["log_2"])
      assert log_2_writes == %{<<"key_1">> => <<"value_1">>, <<"key_2">> => <<"value_2">>}

      # log_3 should get empty transaction (no matching tags)
      log_3_writes = Transaction.key_values(result["log_3"])
      assert log_3_writes == %{}

      # All transactions should have same version
      assert Transaction.version(result["log_1"]) == 100
      assert Transaction.version(result["log_2"]) == 100
      assert Transaction.version(result["log_3"]) == 100
    end

    test "handles case where no tags match any logs" do
      log_descriptors = %{
        # tags that don't exist in transactions
        "log_1" => [10, 11]
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key_0">> => <<"value_0">>})
      }

      result = Finalization.build_log_transactions(log_descriptors, transactions_by_tag, 100)

      # log_1 should get empty transaction
      log_1_writes = Transaction.key_values(result["log_1"])
      assert log_1_writes == %{}
      assert Transaction.version(result["log_1"]) == 100
    end
  end

  describe "prepare_transaction_to_log/4" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, :end}, storage_ids: ["storage_2"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "handles no aborted transactions case", %{storage_teams: storage_teams} do
      transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>, <<"orange">> => <<"citrus">>}}},
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>, <<"zebra">> => <<"animal">>}}}
      ]

      {oks, aborts, transactions_by_tag} =
        Finalization.prepare_transaction_to_log(transactions, [], 100, storage_teams)

      # Should have all transactions as successful
      assert length(oks) == 2
      assert length(aborts) == 0

      # Should group writes by tags
      assert Map.has_key?(transactions_by_tag, 0)
      assert Map.has_key?(transactions_by_tag, 1)

      # Check tag 0 writes (keys < "m")
      tag_0_writes = Transaction.key_values(transactions_by_tag[0])
      assert tag_0_writes == %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}

      # Check tag 1 writes (keys >= "m")
      tag_1_writes = Transaction.key_values(transactions_by_tag[1])
      assert tag_1_writes == %{<<"orange">> => <<"citrus">>, <<"zebra">> => <<"animal">>}

      # All transactions should have correct version
      assert Transaction.version(transactions_by_tag[0]) == 100
      assert Transaction.version(transactions_by_tag[1]) == 100
    end

    test "handles aborted transactions case", %{storage_teams: storage_teams} do
      from_1 = make_ref()
      from_2 = make_ref()
      from_3 = make_ref()

      transactions = [
        # index 0 - will be aborted
        {from_1, {nil, %{<<"apple">> => <<"red">>}}},
        # index 1 - success
        {from_2, {nil, %{<<"banana">> => <<"yellow">>}}},
        # index 2 - success
        {from_3, {nil, %{<<"orange">> => <<"citrus">>}}}
      ]

      # abort first transaction
      aborts = [0]

      {oks, aborts_result, transactions_by_tag} =
        Finalization.prepare_transaction_to_log(transactions, aborts, 100, storage_teams)

      # Should have 2 successful, 1 aborted
      assert length(oks) == 2
      assert length(aborts_result) == 1
      assert aborts_result == [from_1]

      # Should only include writes from successful transactions
      tag_0_writes = Transaction.key_values(transactions_by_tag[0])
      assert tag_0_writes == %{<<"banana">> => <<"yellow">>}

      tag_1_writes = Transaction.key_values(transactions_by_tag[1])
      assert tag_1_writes == %{<<"orange">> => <<"citrus">>}

      # apple should not be included since that transaction was aborted
      refute Map.has_key?(tag_0_writes, <<"apple">>)
    end

    test "handles case with all transactions aborted", %{storage_teams: storage_teams} do
      transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>}}},
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>}}}
      ]

      # abort all transactions
      aborts = [0, 1]

      {oks, aborts_result, transactions_by_tag} =
        Finalization.prepare_transaction_to_log(transactions, aborts, 100, storage_teams)

      # Should have no successful transactions
      assert length(oks) == 0
      assert length(aborts_result) == 2

      # Should have empty transaction groups
      assert transactions_by_tag == %{}
    end

    test "handles transactions with reads", %{storage_teams: storage_teams} do
      transactions = [
        {make_ref(), {{50, [<<"read_key">>]}, %{<<"write_key">> => <<"value">>}}}
      ]

      {oks, aborts, transactions_by_tag} =
        Finalization.prepare_transaction_to_log(transactions, [], 100, storage_teams)

      # Should handle the transaction normally (reads are ignored for logging)
      assert length(oks) == 1
      assert length(aborts) == 0
      # "write_key" should map to tag 1
      assert Map.has_key?(transactions_by_tag, 1)

      tag_1_writes = Transaction.key_values(transactions_by_tag[1])
      assert tag_1_writes == %{<<"write_key">> => <<"value">>}
    end
  end

  describe "integration with existing functions" do
    test "resolve_log_descriptors works correctly" do
      log_descriptors = %{
        "log_1" => [0, 1],
        "log_2" => [1, 2]
      }

      services = %{
        "log_1" => %{kind: :log, status: {:up, self()}},
        "log_2" => %{kind: :log, status: {:up, self()}},
        # missing service
        "log_3" => nil
      }

      result = Finalization.resolve_log_descriptors(log_descriptors, services)

      expected = %{
        "log_1" => %{kind: :log, status: {:up, self()}},
        "log_2" => %{kind: :log, status: {:up, self()}}
      }

      assert result == expected
    end

    test "reply functions work correctly" do
      reply_fn = fn result ->
        send(self(), {:reply, result})
        :ok
      end

      # Test successful reply
      Finalization.send_reply_with_commit_version([reply_fn], 100)
      assert_receive {:reply, {:ok, 100}}

      # Test aborted reply
      Finalization.reply_to_all_clients_with_aborted_transactions([reply_fn])
      assert_receive {:reply, {:error, :aborted}}
    end

    test "reply functions handle empty lists" do
      # Test empty successful replies
      Finalization.send_reply_with_commit_version([], 100)
      # Should not crash

      # Test empty aborted replies
      Finalization.reply_to_all_clients_with_aborted_transactions([])
      # Should not crash
    end
  end

  describe "transform_transactions_for_resolution/1" do
    test "transforms transactions with reads and writes" do
      transactions = [
        {make_ref(), {{50, [<<"read1">>, <<"read2">>]}, %{<<"write1">> => <<"value1">>}}},
        {make_ref(), {nil, %{<<"write2">> => <<"value2">>, <<"write3">> => <<"value3">>}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected = [
        {{50, [<<"read1">>, <<"read2">>]}, [<<"write1">>]},
        {nil, [<<"write2">>, <<"write3">>]}
      ]

      assert result == expected
    end

    test "handles transactions with duplicate reads" do
      transactions = [
        {make_ref(),
         {{50, [<<"read1">>, <<"read1">>, <<"read2">>]}, %{<<"write1">> => <<"value1">>}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected = [
        {{50, [<<"read1">>, <<"read2">>]}, [<<"write1">>]}
      ]

      assert result == expected
    end

    test "handles transactions with no reads" do
      transactions = [
        {make_ref(), {nil, %{<<"write1">> => <<"value1">>, <<"write2">> => <<"value2">>}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected = [
        {nil, [<<"write1">>, <<"write2">>]}
      ]

      assert result == expected
    end
  end

  describe "try_to_push_transaction_to_log/3" do
    test "pushes to available log server" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, :ok)
          end
        end)

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = %{version: 100, data: "test"}
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == :ok
      Process.exit(log_server, :kill)
    end

    test "returns error for unavailable log" do
      service_descriptor = %{kind: :log, status: :down}
      encoded_transaction = %{version: 100, data: "test"}
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :unavailable}
    end

    test "returns error for non-log service" do
      service_descriptor = %{kind: :storage, status: {:up, self()}}
      encoded_transaction = %{version: 100, data: "test"}
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :unavailable}
    end
  end

  describe "edge cases and error handling" do
    test "key_to_tag handles keys at exact boundaries" do
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
        %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
      ]

      # Test exact boundaries
      assert {:ok, 0} = Finalization.key_to_tag(<<"a">>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<"l">>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<"m">>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<"y">>, storage_teams)
      assert {:ok, 2} = Finalization.key_to_tag(<<"z">>, storage_teams)
      assert {:ok, 2} = Finalization.key_to_tag(<<"zz">>, storage_teams)
    end

    test "group_writes_by_tag exits on keys that don't match any team" do
      storage_teams = [
        %{tag: 0, key_range: {<<"b">>, <<"y">>}, storage_ids: ["storage_1"]}
      ]

      writes = %{
        # before any range - should cause exit
        <<"a">> => <<"before_range">>,
        # in range
        <<"m">> => <<"in_range">>,
        # after range - would also cause exit
        <<"z">> => <<"after_range">>
      }

      # Should exit on first unmatched key to trigger recovery
      assert catch_exit(
        Finalization.group_writes_by_tag(writes, storage_teams)
      ) == {:storage_team_coverage_error, <<"a">>}
    end

    test "prepare_transaction_to_log handles empty transactions" do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, :end}, storage_ids: ["storage_1"]}
      ]

      # Empty transactions list
      {oks, aborts, transactions_by_tag} =
        Finalization.prepare_transaction_to_log([], [], 100, storage_teams)

      assert oks == []
      assert aborts == []
      assert transactions_by_tag == %{}
    end

    test "build_log_transactions handles empty inputs" do
      # Empty log descriptors
      result = Finalization.build_log_transactions(%{}, %{}, 100)
      assert result == %{}

      # Empty transactions by tag
      log_descriptors = %{"log_1" => [0, 1]}
      result = Finalization.build_log_transactions(log_descriptors, %{}, 100)

      # Should create empty transaction for the log
      assert Map.has_key?(result, "log_1")
      assert Transaction.key_values(result["log_1"]) == %{}
      assert Transaction.version(result["log_1"]) == 100
    end
  end
end
