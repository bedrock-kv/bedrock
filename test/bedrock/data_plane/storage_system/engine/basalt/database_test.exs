defmodule Bedrock.DataPlane.StorageSystem.Engine.Basalt.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.StorageSystem.Engine.Basalt.Database

  describe "Basalt.Database.open/2" do
    @tag :tmp_dir
    test "can open a database successfully", %{tmp_dir: tmp_dir} do
      assert {:ok, db} = Database.open(:basalt_database_a, Path.join(tmp_dir, "a"))
      assert db
    end
  end
end
