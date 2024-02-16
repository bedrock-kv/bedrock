defmodule Bedrock.DataPlane.LogSystem.Engine.Limestone.TransactionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.LogSystem.Engine.Limestone.Transactions

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
end
