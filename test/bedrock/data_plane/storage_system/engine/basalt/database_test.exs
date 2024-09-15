defmodule Bedrock.Service.StorageWorker.Basalt.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.StorageWorker.Basalt.Database
  alias Bedrock.DataPlane.Transaction

  def random_name, do: "basalt_database_#{Faker.random_between(0, 10_000)}" |> String.to_atom()

  describe "Basalt.Database.open/2" do
    @tag :tmp_dir
    test "can open a database successfully", %{tmp_dir: tmp_dir} do
      file_name = Path.join(tmp_dir, "a")
      assert {:ok, db} = Database.open(random_name(), file_name)
      assert db
      assert File.exists?(file_name)
    end
  end

  describe "Basalt.Database.close/1" do
    @tag :tmp_dir
    test "can close a newly-created database successfully", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "b"))

      assert :ok = Database.close(db)
    end
  end

  describe "Basalt.Database.last_durable_version/1" do
    @tag :tmp_dir
    test "returns :undefined on a newly created database", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "c"))
      assert :undefined == Database.last_durable_version(db)
    end
  end

  describe "Basalt.Database" do
    @tag :tmp_dir
    test "can durably store transactions correctly", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "d"))

      # Write a series of transactions to the DB, each overwriting the previous
      # transaction.
      assert 1 == Database.apply_transactions(db, [Transaction.new(1, [{"foo", "bar"}])])

      assert 2 ==
               Database.apply_transactions(db, [
                 Transaction.new(2, [{"foo", "baz"}, {"boo", "bif"}])
               ])

      assert 3 ==
               Database.apply_transactions(db, [
                 Transaction.new(3, [{"foo", "biz"}, {"bam", "bom"}])
               ])

      assert :undefined == Database.last_durable_version(db)
      assert 3 == Database.last_committed_version(db)
      assert 0 == Database.info(db, :n_keys)

      # Ensure durability of the first transaction and check that the last
      # durable version and value is correct.
      assert :ok == Database.ensure_durability_to_version(db, 1)
      assert 1 == Database.last_durable_version(db)
      assert 1 == Database.info(db, :n_keys)
      assert {:ok, "bar"} == Database.lookup(db, "foo", 1)
      assert {:ok, "baz"} == Database.lookup(db, "foo", 2)
      assert {:ok, "biz"} == Database.lookup(db, "foo", 3)
      assert {:ok, "bif"} == Database.lookup(db, "boo", 2)

      # Ensure durability of the second transaction and check that the last
      # durable version and value is correct.
      assert :ok == Database.ensure_durability_to_version(db, 2)
      assert 2 == Database.last_durable_version(db)
      assert 2 == Database.info(db, :n_keys)
      assert {:error, :transaction_too_old} == Database.lookup(db, "foo", 1)
      assert {:ok, "baz"} == Database.lookup(db, "foo", 2)
      assert {:ok, "biz"} == Database.lookup(db, "foo", 3)
      assert {:ok, "bif"} == Database.lookup(db, "boo", 2)

      # Ensure durability of the third transaction and check that the last
      # durable version and value is correct.
      assert :ok == Database.ensure_durability_to_version(db, 3)
      assert 3 == Database.last_durable_version(db)
      assert 3 == Database.info(db, :n_keys)
      assert {:error, :transaction_too_old} == Database.lookup(db, "foo", 1)
      assert {:error, :transaction_too_old} == Database.lookup(db, "foo", 2)
      assert {:ok, "biz"} == Database.lookup(db, "foo", 3)
      assert {:error, :transaction_too_old} == Database.lookup(db, "boo", 2)
      assert {:ok, "bif"} == Database.lookup(db, "boo", 3)
    end
  end
end
