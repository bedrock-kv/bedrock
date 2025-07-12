defmodule Bedrock.DataPlane.CommitProxy.FinalizationHelpersTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Log.Transaction

  describe "notify_aborts_and_extract_oks/3" do
    test "immediately notifies aborted transactions and extracts successful ones" do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end
      reply_fn3 = fn result -> send(self(), {:reply3, result}) end

      transactions = [
        # index 0 - will be aborted
        {reply_fn1, {nil, %{<<"key1">> => <<"value1">>}}},
        # index 1 - success
        {reply_fn2, {nil, %{<<"key2">> => <<"value2">>}}},
        # index 2 - will be aborted
        {reply_fn3, {nil, %{<<"key3">> => <<"value3">>}}}
      ]

      # Abort transactions at indices 0 and 2
      aborted_indices = [0, 2]

      # Mock abort reply function that tracks calls
      test_pid = self()

      abort_reply_fn = fn aborts ->
        send(test_pid, {:abort_calls, aborts})
        Enum.each(aborts, fn reply_fn -> reply_fn.({:error, :aborted}) end)
        :ok
      end

      {successful_transactions, n_aborts} =
        Finalization.notify_aborts_and_extract_oks(transactions, aborted_indices, abort_reply_fn)

      # Should have 1 successful transaction and 2 aborts
      assert n_aborts == 2
      assert length(successful_transactions) == 1

      # The successful transaction should be the middle one (index 1)
      [{_success_reply_fn, {nil, writes}}] = successful_transactions
      assert writes == %{<<"key2">> => <<"value2">>}

      # Aborted transactions should have been notified immediately
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply3, {:error, :aborted}}

      # Should not have received reply for successful transaction yet
      refute_receive {:reply2, _}

      # Abort function should have been called with correct reply functions
      assert_receive {:abort_calls, abort_fns}
      assert length(abort_fns) == 2
    end

    test "handles case with no aborts" do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      transactions = [
        {reply_fn1, {nil, %{<<"key1">> => <<"value1">>}}},
        {reply_fn2, {nil, %{<<"key2">> => <<"value2">>}}}
      ]

      abort_reply_fn = fn aborts ->
        assert aborts == []
        :ok
      end

      {successful_transactions, n_aborts} =
        Finalization.notify_aborts_and_extract_oks(transactions, [], abort_reply_fn)

      assert n_aborts == 0
      assert length(successful_transactions) == 2

      # No abort notifications should be sent
      refute_receive {:reply1, {:error, :aborted}}
      refute_receive {:reply2, {:error, :aborted}}
    end

    test "handles case with all aborts" do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      transactions = [
        {reply_fn1, {nil, %{<<"key1">> => <<"value1">>}}},
        {reply_fn2, {nil, %{<<"key2">> => <<"value2">>}}}
      ]

      abort_reply_fn = fn aborts ->
        Enum.each(aborts, fn reply_fn -> reply_fn.({:error, :aborted}) end)
        :ok
      end

      {successful_transactions, n_aborts} =
        Finalization.notify_aborts_and_extract_oks(transactions, [0, 1], abort_reply_fn)

      assert n_aborts == 2
      assert successful_transactions == []

      # All should be aborted
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:error, :aborted}}
    end

    test "handles empty transaction list" do
      abort_reply_fn = fn aborts ->
        assert aborts == []
        :ok
      end

      {successful_transactions, n_aborts} =
        Finalization.notify_aborts_and_extract_oks([], [], abort_reply_fn)

      assert n_aborts == 0
      assert successful_transactions == []
    end
  end

  describe "prepare_successful_transactions_for_log/3" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, :end}, storage_ids: ["storage_2"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "groups successful transactions by storage team tags", %{storage_teams: storage_teams} do
      successful_transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>, <<"orange">> => <<"citrus">>}}},
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>, <<"zebra">> => <<"animal">>}}}
      ]

      {:ok, result} =
        Finalization.prepare_successful_transactions_for_log(
          successful_transactions,
          100,
          storage_teams
        )

      # Should have transactions for both tags
      assert Map.has_key?(result, 0)
      assert Map.has_key?(result, 1)

      # Check tag 0 (keys < "m")
      tag_0_writes = Transaction.key_values(result[0])
      expected_tag_0 = %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}
      assert tag_0_writes == expected_tag_0
      assert Transaction.version(result[0]) == 100

      # Check tag 1 (keys >= "m")  
      tag_1_writes = Transaction.key_values(result[1])
      expected_tag_1 = %{<<"orange">> => <<"citrus">>, <<"zebra">> => <<"animal">>}
      assert tag_1_writes == expected_tag_1
      assert Transaction.version(result[1]) == 100
    end

    test "handles empty successful transactions", %{storage_teams: storage_teams} do
      {:ok, result} = Finalization.prepare_successful_transactions_for_log([], 100, storage_teams)
      assert result == %{}
    end

    test "handles successful transactions for single tag", %{storage_teams: storage_teams} do
      successful_transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>}}},
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>}}}
      ]

      {:ok, result} =
        Finalization.prepare_successful_transactions_for_log(
          successful_transactions,
          100,
          storage_teams
        )

      # Should only have tag 0
      assert Map.keys(result) == [0]

      tag_0_writes = Transaction.key_values(result[0])
      expected = %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}
      assert tag_0_writes == expected
      assert Transaction.version(result[0]) == 100
    end

    test "handles conflicting writes in same transaction", %{storage_teams: storage_teams} do
      # Transaction with write to same key - later write should win
      successful_transactions = [
        {make_ref(), {nil, %{<<"apple">> => <<"red">>}}},
        # Overwrites previous
        {make_ref(), {nil, %{<<"apple">> => <<"green">>}}}
      ]

      {:ok, result} =
        Finalization.prepare_successful_transactions_for_log(
          successful_transactions,
          100,
          storage_teams
        )

      tag_0_writes = Transaction.key_values(result[0])
      # Later write should win
      assert tag_0_writes == %{<<"apple">> => <<"green">>}
    end

    test "preserves transaction ordering within tags", %{storage_teams: storage_teams} do
      # Multiple writes to same key across transactions
      successful_transactions = [
        {make_ref(), {nil, %{<<"key">> => <<"value1">>}}},
        {make_ref(), {nil, %{<<"key">> => <<"value2">>}}},
        {make_ref(), {nil, %{<<"key">> => <<"value3">>}}}
      ]

      {:ok, result} =
        Finalization.prepare_successful_transactions_for_log(
          successful_transactions,
          100,
          storage_teams
        )

      tag_0_writes = Transaction.key_values(result[0])
      # Last write should win (transaction ordering preserved)
      assert tag_0_writes == %{<<"key">> => <<"value3">>}
    end

    test "handles transactions with read-write conflicts", %{storage_teams: storage_teams} do
      # Transactions with both reads and writes
      successful_transactions = [
        {make_ref(), {%{<<"read_key">> => <<"read_val">>}, %{<<"apple">> => <<"red">>}}},
        {make_ref(), {nil, %{<<"banana">> => <<"yellow">>}}}
      ]

      {:ok, result} =
        Finalization.prepare_successful_transactions_for_log(
          successful_transactions,
          100,
          storage_teams
        )

      # Only writes should be included in the log transaction
      tag_0_writes = Transaction.key_values(result[0])
      expected = %{<<"apple">> => <<"red">>, <<"banana">> => <<"yellow">>}
      assert tag_0_writes == expected

      # Read information should not be in the transaction
      refute Map.has_key?(tag_0_writes, <<"read_key">>)
    end
  end
end
