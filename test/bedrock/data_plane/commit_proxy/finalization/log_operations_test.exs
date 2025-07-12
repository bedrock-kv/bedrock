defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogOperationsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Log.Transaction
  alias FinalizationTestSupport, as: Support

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

    test "handles empty transactions_by_tag" do
      log_descriptors = %{
        "log_1" => [0, 1],
        "log_2" => [2, 3]
      }

      result = Finalization.build_log_transactions(log_descriptors, %{}, 100)

      # All logs should get empty transactions
      assert Transaction.key_values(result["log_1"]) == %{}
      assert Transaction.key_values(result["log_2"]) == %{}
      assert Transaction.version(result["log_1"]) == 100
      assert Transaction.version(result["log_2"]) == 100
    end

    test "handles overlapping tag coverage" do
      log_descriptors = %{
        "log_1" => [0, 1],
        "log_2" => [1, 2],
        # Overlaps with both log_1 and log_2
        "log_3" => [0, 2]
      }

      transactions_by_tag = %{
        0 => Transaction.new(100, %{<<"key_0">> => <<"value_0">>}),
        1 => Transaction.new(100, %{<<"key_1">> => <<"value_1">>}),
        2 => Transaction.new(100, %{<<"key_2">> => <<"value_2">>})
      }

      result = Finalization.build_log_transactions(log_descriptors, transactions_by_tag, 100)

      # Verify each log gets correct writes
      assert Transaction.key_values(result["log_1"]) == %{
               <<"key_0">> => <<"value_0">>,
               <<"key_1">> => <<"value_1">>
             }

      assert Transaction.key_values(result["log_2"]) == %{
               <<"key_1">> => <<"value_1">>,
               <<"key_2">> => <<"value_2">>
             }

      assert Transaction.key_values(result["log_3"]) == %{
               <<"key_0">> => <<"value_0">>,
               <<"key_2">> => <<"value_2">>
             }
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

      {:ok, {oks, aborts, transactions_by_tag}} =
        Finalization.prepare_transaction_to_log(transactions, [], 100, storage_teams)

      # Should have all transactions as successful
      assert length(oks) == 2
      assert length(aborts) == 0

      # Should group writes by tags correctly
      expected_tag_0_writes = %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}
      expected_tag_1_writes = %{<<"orange">> => <<"citrus">>, <<"zebra">> => <<"animal">>}

      assert Transaction.key_values(transactions_by_tag[0]) == expected_tag_0_writes
      assert Transaction.key_values(transactions_by_tag[1]) == expected_tag_1_writes
      assert Transaction.version(transactions_by_tag[0]) == 100
      assert Transaction.version(transactions_by_tag[1]) == 100
    end

    test "handles some aborted transactions", %{storage_teams: storage_teams} do
      transactions = [
        # index 0
        {make_ref(), {nil, %{<<"apple">> => <<"red">>}}},
        # index 1 - will be aborted
        {make_ref(), {nil, %{<<"orange">> => <<"citrus">>}}},
        # index 2
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>}}}
      ]

      # Abort the second transaction
      aborted_indices = [1]

      {:ok, {oks, aborts, transactions_by_tag}} =
        Finalization.prepare_transaction_to_log(transactions, aborted_indices, 100, storage_teams)

      # Should have 2 successful, 1 aborted
      assert length(oks) == 2
      assert length(aborts) == 1

      # Only non-aborted writes should be in the tag groups
      expected_tag_0_writes = %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}

      assert Transaction.key_values(transactions_by_tag[0]) == expected_tag_0_writes
      # No tag 1 writes since orange was aborted
      assert Map.has_key?(transactions_by_tag, 1) == false
    end

    test "handles all transactions aborted", %{storage_teams: storage_teams} do
      transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>}}},
        {make_ref(), {nil, %{<<"orange">> => <<"citrus">>}}}
      ]

      # Abort all transactions
      aborted_indices = [0, 1]

      {:ok, {oks, aborts, transactions_by_tag}} =
        Finalization.prepare_transaction_to_log(transactions, aborted_indices, 100, storage_teams)

      assert length(oks) == 0
      assert length(aborts) == 2
      assert transactions_by_tag == %{}
    end

    test "handles empty transactions list", %{storage_teams: storage_teams} do
      {:ok, {oks, aborts, transactions_by_tag}} =
        Finalization.prepare_transaction_to_log([], [], 100, storage_teams)

      assert length(oks) == 0
      assert length(aborts) == 0
      assert transactions_by_tag == %{}
    end
  end

  describe "try_to_push_transaction_to_log/3" do
    test "succeeds when log server responds with :ok" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, :ok)
          end
        end)

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
      Support.ensure_process_killed(log_server)
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
      Support.ensure_process_killed(log_server)
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
