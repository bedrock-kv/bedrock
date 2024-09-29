defmodule Bedrock.Service.TransactionLog.Limestone.TransactionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.TransactionLog.Limestone.Transactions

  setup context do
    context
    |> Enum.reduce(context, fn
      {:with_empty, true}, context ->
        t = Transactions.new(:"transactions_#{Faker.random_between(0, 10_000)}")
        context |> Map.put(:transactions, t)

      _, context ->
        context
    end)
  end

  describe "Limestone.Transactions.new/1" do
    test "it returns an appropriately configured result" do
      assert %Transactions{ets: :transactions} =
               Transactions.new(:transactions)
    end
  end

  describe "Limestone.Transactions.append!/1" do
    @tag :with_empty
    test "it will append a single transaction correctly", %{transactions: t} do
      t1 = Transaction.new(<<1>>, [{"test", "me"}])
      assert :ok = t |> Transactions.append!(t1)
      assert [t1] == t.ets |> :ets.tab2list()
    end

    @tag :with_empty
    test "it will append a pair of single transactions correctly", %{transactions: t} do
      t1 = Transaction.new(<<1>>, [{"test", "me"}])
      t2 = Transaction.new(<<2>>, [{"test", "you"}])

      assert :ok = t |> Transactions.append!(t1)
      assert :ok = t |> Transactions.append!(t2)

      assert [
               {<<1>>, [{"test", "me"}]},
               {<<2>>, [{"test", "you"}]}
             ] == t.ets |> :ets.tab2list()
    end

    @tag :with_empty
    test "it will append a pair of transactions in a single call correctly", %{transactions: t} do
      t1 = Transaction.new(<<1>>, [{"test", "me"}])
      t2 = Transaction.new(<<2>>, [{"test", "you"}])

      assert :ok = t |> Transactions.append!([t1, t2])

      assert [
               {<<1>>, [{"test", "me"}]},
               {<<2>>, [{"test", "you"}]}
             ] == t.ets |> :ets.tab2list()
    end
  end

  describe "Limestone.Transactions.get/3" do
    @tag :with_empty
    test "it will return transactions correctly", %{transactions: t} do
      t1 = Transaction.new(<<1>>, [{"test", "me"}])
      t2 = Transaction.new(<<2>>, [{"test", "you"}])
      t3 = Transaction.new(<<3>>, [{"test", "boo"}])
      t4 = Transaction.new(<<4>>, [{"test", "baz"}])

      assert :ok = t |> Transactions.append!([t1, t2, t3, t4])

      assert [
               {<<2>>, [{"test", "you"}]},
               {<<3>>, [{"test", "boo"}]},
               {<<4>>, [{"test", "baz"}]}
             ] ==
               Transactions.get(t, <<1>>, 5)

      assert [
               {<<3>>, [{"test", "boo"}]},
               {<<4>>, [{"test", "baz"}]}
             ] ==
               Transactions.get(t, <<2>>, 2)

      assert [] ==
               Transactions.get(t, <<4>>, 2)
    end
  end
end
