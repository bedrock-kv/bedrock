defmodule Bedrock.DataPlane.CommitProxy.BatchTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch

  describe "new_batch/3" do
    test "creates a new batch with initial values" do
      started_at = 1000
      last_commit_version = <<1, 0, 0, 0, 0, 0, 0, 0>>
      commit_version = <<2, 0, 0, 0, 0, 0, 0, 0>>

      batch = Batch.new_batch(started_at, last_commit_version, commit_version)

      assert batch.started_at == started_at
      assert batch.last_commit_version == last_commit_version
      assert batch.commit_version == commit_version
      assert batch.n_transactions == 0
      assert batch.buffer == []
    end
  end

  describe "add_transaction/4" do
    test "adds transaction to batch buffer" do
      batch = Batch.new_batch(1000, <<1::64>>, <<2::64>>)
      transaction = <<1, 2, 3>>
      reply_fn = fn _ -> :ok end
      batch = Batch.add_transaction(batch, transaction, reply_fn, :user)

      assert batch.n_transactions == 1
      assert length(batch.buffer) == 1
      assert [{0, ^reply_fn, ^transaction, :user}] = batch.buffer
    end

    test "increments transaction count and index" do
      batch = Batch.new_batch(1000, <<1::64>>, <<2::64>>)
      reply_fn = fn _ -> :ok end

      batch =
        batch
        |> Batch.add_transaction(<<1>>, reply_fn, :user)
        |> Batch.add_transaction(<<2>>, reply_fn, :user)
        |> Batch.add_transaction(<<3>>, reply_fn, :user)

      assert batch.n_transactions == 3
      # Buffer is in reverse order (most recent first)
      assert [{2, _, <<3>>, :user}, {1, _, <<2>>, :user}, {0, _, <<1>>, :user}] = batch.buffer
    end
  end

  describe "transactions_in_order/1" do
    test "returns transactions in insertion order" do
      batch = Batch.new_batch(1000, <<1::64>>, <<2::64>>)
      reply_fn = fn _ -> :ok end

      batch =
        batch
        |> Batch.add_transaction(<<1>>, reply_fn, :user)
        |> Batch.add_transaction(<<2>>, reply_fn, :user)
        |> Batch.add_transaction(<<3>>, reply_fn, :user)

      # Should reverse the buffer to get original insertion order
      transactions = Batch.transactions_in_order(batch)

      assert [{0, _, <<1>>, :user}, {1, _, <<2>>, :user}, {2, _, <<3>>, :user}] = transactions
    end
  end

  describe "all_callers/1" do
    test "returns all reply functions" do
      batch = Batch.new_batch(1000, <<1::64>>, <<2::64>>)
      reply_fn_1 = fn _ -> :reply_1 end
      reply_fn_2 = fn _ -> :reply_2 end
      reply_fn_3 = fn _ -> :reply_3 end

      batch =
        batch
        |> Batch.add_transaction(<<1>>, reply_fn_1, :user)
        |> Batch.add_transaction(<<2>>, reply_fn_2, :user)
        |> Batch.add_transaction(<<3>>, reply_fn_3, :user)

      callers = Batch.all_callers(batch)

      # Buffer is reversed, so callers will be in reverse order too
      assert [^reply_fn_3, ^reply_fn_2, ^reply_fn_1] = callers
    end
  end

  describe "transaction_count/1" do
    test "returns the number of transactions" do
      batch = Batch.new_batch(1000, <<1::64>>, <<2::64>>)

      assert Batch.transaction_count(batch) == 0

      batch = Batch.add_transaction(batch, <<1>>, fn _ -> :ok end, :user)
      assert Batch.transaction_count(batch) == 1

      batch = Batch.add_transaction(batch, <<2>>, fn _ -> :ok end, :user)
      assert Batch.transaction_count(batch) == 2
    end
  end
end
