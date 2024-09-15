defmodule Bedrock.Service.StorageWorker.Basalt.MultiversionConcurrencyControlTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.StorageWorker.Basalt.MultiversionConcurrencyControl, as: MVCC

  def new_random_mvcc, do: MVCC.new(:"mvcc_#{Faker.random_between(0, 10_000)}", 0)

  def with_mvcc(context) do
    {:ok, context |> Map.put(:mvcc, new_random_mvcc())}
  end

  def with_transactions_applied(%{mvcc: mvcc} = context) do
    MVCC.apply_transactions!(
      mvcc,
      [
        {1, [{"j", "d"}, {"n", "1"}, {"a", nil}, {"c", "c"}]},
        {2, [{"n", nil}, {"a", "b"}]},
        {3, [{"c", "x"}]}
      ]
    )

    {:ok, context}
  end

  describe "MultiversionConcurrencyControl.apply_one_transaction!/2" do
    setup :with_mvcc

    test "apply_transaction/2 can apply a single transaction correctly", %{mvcc: mvcc} do
      assert :ok =
               MVCC.apply_one_transaction!(mvcc, {1, [{"c", "d"}, {"e", nil}, {"a", "b"}]})

      assert [
               {:newest_version, 1},
               {:oldest_version, 0},
               {{"a", 1}, "b"},
               {{"c", 1}, "d"},
               {{"e", 1}, nil}
             ] ==
               mvcc |> :ets.tab2list()
    end
  end

  describe "MultiversionConcurrencyControl.apply_transactions!/2" do
    setup :with_mvcc

    test "it can apply multiple transactions correctly", %{mvcc: mvcc} do
      assert 2 =
               MVCC.apply_transactions!(
                 mvcc,
                 [
                   {1, [{"c", "d"}, {"e", nil}, {"a", "b"}]},
                   {2, [{"c", nil}, {"e", "f"}, {"a", "b2"}]}
                 ]
               )

      assert [
               {:newest_version, 2},
               {:oldest_version, 0},
               {{"a", 1}, "b"},
               {{"a", 2}, "b2"},
               {{"c", 1}, "d"},
               {{"c", 2}, nil},
               {{"e", 1}, nil},
               {{"e", 2}, "f"}
             ] ==
               mvcc |> :ets.tab2list()
    end

    test "it will raise an exception if transactions are out of order", %{
      mvcc: mvcc
    } do
      assert_raise RuntimeError, "Transactions must be applied in order (new 1, old 2)", fn ->
        refute 2 =
                 MVCC.apply_transactions!(
                   mvcc,
                   [
                     {2, [{"c", nil}, {"e", "f"}, {"a", "b2"}]},
                     {1, [{"c", "d"}, {"e", nil}, {"a", "b"}]}
                   ]
                 )
      end
    end
  end

  describe "MultiversionConcurrencyControl.insert_read/4" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it will set a value for a given key/version", %{
      mvcc: mvcc
    } do
      assert :ok = mvcc |> MVCC.insert_read("x", 1, "x")
      assert {:ok, "x"} = MVCC.lookup(mvcc, "x", 1)
    end

    test "it will do nothing when asked to set a new value for an existing key/version", %{
      mvcc: mvcc
    } do
      assert :ok = mvcc |> MVCC.insert_read("x", 1, "x")
      assert :ok = mvcc |> MVCC.insert_read("x", 1, "y")
      assert {:ok, "x"} = MVCC.lookup(mvcc, "x", 1)
    end
  end

  describe "MultiversionConcurrencyControl.lookup/3" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it will return the correct value, if given a key that was set at the exact version", %{
      mvcc: mvcc
    } do
      assert {:ok, "d"} = MVCC.lookup(mvcc, "j", 1)
    end

    test "it will return the correct value, if given a key that was set at a lower version", %{
      mvcc: mvcc
    } do
      assert {:ok, "d"} = MVCC.lookup(mvcc, "j", 2)
    end

    test "it will return an error for cleared keys, if given a key that has been cleared at the exact version",
         %{
           mvcc: mvcc
         } do
      assert {:error, :not_found} = MVCC.lookup(mvcc, "a", 1)
    end

    test "it will return the correct value for keys cleared at a lower version",
         %{
           mvcc: mvcc
         } do
      assert {:error, :not_found} = MVCC.lookup(mvcc, "n", 3)
    end
  end

  describe "MultiversionConcurrencyControl.transaction_at_version/2" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it returns the correct value when given :latest",
         %{
           mvcc: mvcc
         } do
      assert {3,
              [
                {"a", "b"},
                {"c", "x"},
                {"j", "d"},
                {"n", nil}
              ]} =
               MVCC.transaction_at_version(mvcc, :latest)
    end

    test "it returns the correct value",
         %{
           mvcc: mvcc
         } do
      assert {0, []} =
               MVCC.transaction_at_version(mvcc, 0)

      assert {1,
              [
                {"a", nil},
                {"c", "c"},
                {"j", "d"},
                {"n", "1"}
              ]} =
               MVCC.transaction_at_version(mvcc, 1)

      assert {2,
              [
                {"a", "b"},
                {"c", "c"},
                {"j", "d"},
                {"n", nil}
              ]} =
               MVCC.transaction_at_version(mvcc, 2)

      assert {3,
              [
                {"a", "b"},
                {"c", "x"},
                {"j", "d"},
                {"n", nil}
              ]} =
               MVCC.transaction_at_version(mvcc, 3)
    end

    test "it returns the correct value, even if read entries are present",
         %{
           mvcc: mvcc
         } do
      mvcc |> MVCC.insert_read("a", 0, "x")
      mvcc |> MVCC.insert_read("x", 2, "x")

      assert {0, []} =
               MVCC.transaction_at_version(mvcc, 0)

      assert {1, [{"a", nil}, {"c", "c"}, {"j", "d"}, {"n", "1"}]} =
               MVCC.transaction_at_version(mvcc, 1)

      assert {2, [{"a", "b"}, {"c", "c"}, {"j", "d"}, {"n", nil}]} =
               MVCC.transaction_at_version(mvcc, 2)

      assert {3, [{"a", "b"}, {"c", "x"}, {"j", "d"}, {"n", nil}]} =
               MVCC.transaction_at_version(mvcc, 3)
    end
  end

  describe "MultiversionConcurrencyControl.purge_keys_older_than_version/2" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it succeeds when there are no keys to purge", %{mvcc: mvcc} do
      assert {:ok, 0} = MVCC.purge_keys_older_than_version(mvcc, 1)

      assert {1, [{"a", nil}, {"c", "c"}, {"j", "d"}, {"n", "1"}]} =
               MVCC.transaction_at_version(mvcc, 1)
    end

    test "it succeeds for transactions less than 2", %{mvcc: mvcc} do
      assert {:ok, 4} = MVCC.purge_keys_older_than_version(mvcc, 2)

      assert {2, [{"a", "b"}, {"n", nil}]} =
               MVCC.transaction_at_version(mvcc, 2)
    end

    test "it succeeds for transactions less than 3", %{mvcc: mvcc} do
      assert {:ok, 6} = MVCC.purge_keys_older_than_version(mvcc, 3)

      assert {3, [{"c", "x"}]} =
               MVCC.transaction_at_version(mvcc, 3)
    end
  end
end
