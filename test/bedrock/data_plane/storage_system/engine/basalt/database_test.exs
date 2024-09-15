defmodule Bedrock.Service.StorageWorker.Basalt.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.StorageWorker.Basalt.Database
  alias Bedrock.DataPlane.Transaction

  describe "Basalt.Database.open/2" do
    @tag :tmp_dir
    test "can open a database successfully", %{tmp_dir: tmp_dir} do
      file_name = Path.join(tmp_dir, "a")
      assert {:ok, db} = Database.open(:basalt_database_a, file_name)
      assert db
      assert File.exists?(file_name)
    end
  end

  describe "Basalt.Database.close/1" do
    @tag :tmp_dir
    test "can close a newly-created database successfully", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(:basalt_database_b, Path.join(tmp_dir, "b"))

      assert :ok = Database.close(db)
    end
  end

  describe "Basalt.Database.last_durable_version/1" do
    @tag :tmp_dir
    test "returns :undefined on a newly created database", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(:basalt_database_c, Path.join(tmp_dir, "c"))
      assert :undefined == Database.last_durable_version(db)
    end

    @tag :tmp_dir
    test "returns the correct version after storing one transaction", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(:basalt_database_d, Path.join(tmp_dir, "d"))

      Database.apply_transactions(db, [Transaction.new(1, foo: :bar)])

      assert 1 == Database.last_durable_version(db)
    end
  end
end
