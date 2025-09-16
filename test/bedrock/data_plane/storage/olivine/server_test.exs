defmodule Bedrock.DataPlane.Storage.Olivine.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Server

  describe "module structure" do
    test "all modules compile and load successfully" do
      modules = [Server, Database, IndexManager]

      for module <- modules do
        assert {:module, ^module} = Code.ensure_loaded(module)
      end
    end

    test "basic SQLite operations work" do
      temp_path = "/tmp/test_olivine_#{System.unique_integer([:positive])}.sqlite"

      assert {:ok, db} = Database.open(:test_olivine, temp_path)
      assert :ok = Database.close(db)

      File.rm(temp_path)
    end

    test "IndexManager can be created" do
      vm = IndexManager.new()
      assert %IndexManager{} = vm
    end

    test "basic telemetry events can be emitted" do
      alias Bedrock.DataPlane.Storage.Telemetry

      assert :ok = Telemetry.trace_startup_start(:test_server)
      assert :ok = Telemetry.trace_startup_complete(:test_server)
      assert :ok = Telemetry.trace_log_pull_start(<<1::64>>, <<2::64>>)
    end
  end

  describe "GenServer child_spec" do
    test "child_spec returns valid supervisor spec" do
      spec =
        Server.child_spec(
          otp_name: :test_olivine,
          foreman: self(),
          id: "test_id",
          path: "/tmp/test_olivine"
        )

      assert %{
               id: {Server, "test_id"},
               start: {GenServer, :start_link, _args}
             } = spec
    end

    test "child_spec raises when required options are missing" do
      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(foreman: self(), id: "test", path: "/tmp")
      end

      assert_raise RuntimeError, "Missing :foreman option", fn ->
        Server.child_spec(otp_name: :test, id: "test", path: "/tmp")
      end
    end
  end
end
