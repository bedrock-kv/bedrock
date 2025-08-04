defmodule Bedrock.DataPlane.Storage.Basalt.MultiVersionConcurrencyControlTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Basalt.MultiVersionConcurrencyControl, as: MVCC
  alias Bedrock.DataPlane.Version

  def new_random_mvcc, do: MVCC.new(:"mvcc_#{Faker.random_between(0, 10_000)}", Version.zero())

  def with_mvcc(context) do
    {:ok, context |> Map.put(:mvcc, new_random_mvcc())}
  end

  def with_transactions_applied(%{mvcc: mvcc} = context) do
    MVCC.apply_transactions!(
      mvcc,
      [
        {Version.from_integer(1), %{"j" => "d", "n" => "1", "a" => nil, "c" => "c"}},
        {Version.from_integer(2), %{"n" => nil, "a" => "b"}},
        {Version.from_integer(3), %{"c" => "x"}}
      ]
    )

    {:ok, context}
  end

  describe "apply_one_transaction!/2" do
    setup :with_mvcc

    test "can apply a single transaction correctly", %{mvcc: mvcc} do
      assert :ok =
               MVCC.apply_one_transaction!(
                 mvcc,
                 {Version.from_integer(1), %{"c" => "d", "e" => nil, "a" => "b"}}
               )

      assert %{
               :newest_version => Version.from_integer(1),
               :oldest_version => Version.zero(),
               {"a", Version.from_integer(1)} => "b",
               {"c", Version.from_integer(1)} => "d",
               {"e", Version.from_integer(1)} => nil
             } ==
               mvcc |> :ets.tab2list() |> Map.new()
    end
  end

  describe "apply_transactions!/2" do
    setup :with_mvcc

    test "can apply multiple transactions correctly", %{mvcc: mvcc} do
      result =
        MVCC.apply_transactions!(
          mvcc,
          [
            {Version.from_integer(1), %{"c" => "d", "e" => nil, "a" => "b"}},
            {Version.from_integer(2), %{"c" => nil, "e" => "f", "a" => "b2"}}
          ]
        )

      assert result == Version.from_integer(2)

      assert %{
               :newest_version => Version.from_integer(2),
               :oldest_version => Version.zero(),
               {"a", Version.from_integer(1)} => "b",
               {"a", Version.from_integer(2)} => "b2",
               {"c", Version.from_integer(1)} => "d",
               {"c", Version.from_integer(2)} => nil,
               {"e", Version.from_integer(1)} => nil,
               {"e", Version.from_integer(2)} => "f"
             } ==
               mvcc |> :ets.tab2list() |> Map.new()
    end

    test "it will raise an exception if transactions are out of order", %{
      mvcc: mvcc
    } do
      assert_raise RuntimeError, ~r/Transactions must be applied in order/, fn ->
        result =
          MVCC.apply_transactions!(
            mvcc,
            [
              {Version.from_integer(2), %{"c" => nil, "e" => "f", "a" => "b2"}},
              {Version.from_integer(1), %{"c" => "d", "e" => nil, "a" => "b"}}
            ]
          )

        refute result == Version.from_integer(2)
      end
    end
  end

  describe "insert_read/4" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it will set a value for a given key/version", %{
      mvcc: mvcc
    } do
      assert :ok = mvcc |> MVCC.insert_read("x", Version.from_integer(1), "x")
      assert {:ok, "x"} = MVCC.fetch(mvcc, "x", Version.from_integer(1))
    end

    test "it will do nothing when asked to set a new value for an existing key/version", %{
      mvcc: mvcc
    } do
      assert :ok = mvcc |> MVCC.insert_read("x", Version.from_integer(1), "x")
      assert :ok = mvcc |> MVCC.insert_read("x", Version.from_integer(1), "y")
      assert {:ok, "x"} = MVCC.fetch(mvcc, "x", Version.from_integer(1))
    end
  end

  describe "fetch/3" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it will return the correct value, if given a key that was set at the exact version", %{
      mvcc: mvcc
    } do
      assert {:ok, "d"} = MVCC.fetch(mvcc, "j", Version.from_integer(1))
    end

    test "it will return the correct value, if given a key that was set at a lower version", %{
      mvcc: mvcc
    } do
      assert {:ok, "d"} = MVCC.fetch(mvcc, "j", Version.from_integer(2))
    end

    test "it will return an error for cleared keys, if given a key that has been cleared at the exact version",
         %{
           mvcc: mvcc
         } do
      assert {:error, :not_found} = MVCC.fetch(mvcc, "a", Version.from_integer(1))
    end

    test "it will return the correct value for keys cleared at a lower version",
         %{
           mvcc: mvcc
         } do
      assert {:error, :not_found} = MVCC.fetch(mvcc, "n", Version.from_integer(3))
    end
  end

  describe "transaction_at_version/2" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it returns the correct value when given :latest",
         %{
           mvcc: mvcc
         } do
      result = MVCC.transaction_at_version(mvcc, :latest)

      expected =
        {Version.from_integer(3),
         %{
           "a" => "b",
           "c" => "x",
           "j" => "d",
           "n" => nil
         }}

      assert result == expected
    end

    test "it returns the correct value",
         %{
           mvcc: mvcc
         } do
      assert MVCC.transaction_at_version(mvcc, Version.zero()) == {Version.zero(), %{}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(1)) ==
               {Version.from_integer(1), %{"a" => nil, "c" => "c", "j" => "d", "n" => "1"}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(2)) ==
               {Version.from_integer(2), %{"a" => "b", "c" => "c", "j" => "d", "n" => nil}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(3)) ==
               {Version.from_integer(3), %{"a" => "b", "c" => "x", "j" => "d", "n" => nil}}
    end

    test "it returns the correct value, even if read entries are present",
         %{
           mvcc: mvcc
         } do
      mvcc |> MVCC.insert_read("a", Version.zero(), "x")
      mvcc |> MVCC.insert_read("x", Version.from_integer(2), "x")

      assert MVCC.transaction_at_version(mvcc, Version.zero()) == {Version.zero(), %{}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(1)) ==
               {Version.from_integer(1), %{"a" => nil, "c" => "c", "j" => "d", "n" => "1"}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(2)) ==
               {Version.from_integer(2), %{"a" => "b", "c" => "c", "j" => "d", "n" => nil}}

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(3)) ==
               {Version.from_integer(3), %{"a" => "b", "c" => "x", "j" => "d", "n" => nil}}
    end
  end

  describe "purge_keys_older_than_version/2" do
    setup [:with_mvcc, :with_transactions_applied]

    test "it succeeds when there are no keys to purge", %{mvcc: mvcc} do
      assert {:ok, 0} = MVCC.purge_keys_older_than_version(mvcc, Version.from_integer(1))

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(1)) ==
               {Version.from_integer(1), %{"a" => nil, "c" => "c", "j" => "d", "n" => "1"}}
    end

    test "it succeeds for transactions less than 2", %{mvcc: mvcc} do
      assert {:ok, 4} = MVCC.purge_keys_older_than_version(mvcc, Version.from_integer(2))

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(2)) ==
               {Version.from_integer(2), %{"a" => "b", "n" => nil}}
    end

    test "it succeeds for transactions less than 3", %{mvcc: mvcc} do
      assert {:ok, 6} = MVCC.purge_keys_older_than_version(mvcc, Version.from_integer(3))

      assert MVCC.transaction_at_version(mvcc, Version.from_integer(3)) ==
               {Version.from_integer(3), %{"c" => "x"}}
    end
  end
end
