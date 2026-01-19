defmodule Bedrock.DataPlane.Demux.ServerTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux.Server
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    test_dir = Path.join(System.tmp_dir!(), "demux_server_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
    log_pid = self()

    # Start the Demux server (ShardServers are started linked directly)
    {:ok, server} =
      start_supervised({Server, cluster: "test-cluster", object_storage: backend, log: log_pid})

    on_exit(fn -> File.rm_rf!(test_dir) end)

    %{server: server, backend: backend, log: log_pid, test_dir: test_dir}
  end

  defp make_transaction(mutations, shard_index) do
    Transaction.encode(%{
      mutations: mutations,
      shard_index: shard_index,
      commit_version: <<0, 0, 0, 0, 0, 0, 10, 0>>
    })
  end

  describe "push/3" do
    test "routes transaction to correct ShardServer", %{server: server} do
      # Transaction with mutations for shard 0
      mutations = [{:set, "key1", "value1"}, {:set, "key2", "value2"}]
      txn = make_transaction(mutations, [{0, 2}])
      version = <<0, 0, 0, 0, 0, 0, 10, 0>>

      :ok = Server.push(server, version, txn)
      :timer.sleep(50)

      # Verify ShardServer was created and received data
      {:ok, shard_server} = Server.get_shard_server(server, 0)
      assert ShardServer.latest_version(shard_server) == version
    end

    test "routes to multiple ShardServers", %{server: server} do
      # Transaction touching shards 0 and 5
      mutations = [
        {:set, "shard0_key", "value"},
        {:set, "shard5_key", "value"}
      ]

      txn = make_transaction(mutations, [{0, 1}, {5, 1}])
      version = <<0, 0, 0, 0, 0, 0, 20, 0>>

      :ok = Server.push(server, version, txn)
      :timer.sleep(50)

      # Both ShardServers should exist and have data
      {:ok, shard0} = Server.get_shard_server(server, 0)
      {:ok, shard5} = Server.get_shard_server(server, 5)

      assert ShardServer.latest_version(shard0) == version
      assert ShardServer.latest_version(shard5) == version
    end
  end

  describe "get_shard_server/2" do
    test "creates ShardServer on demand", %{server: server} do
      {:ok, shard_server} = Server.get_shard_server(server, 42)
      assert is_pid(shard_server)
      assert Process.alive?(shard_server)
    end

    test "returns same pid on subsequent calls", %{server: server} do
      {:ok, pid1} = Server.get_shard_server(server, 10)
      {:ok, pid2} = Server.get_shard_server(server, 10)
      assert pid1 == pid2
    end
  end

  describe "durability tracking" do
    test "tracks minimum durable version", %{server: server} do
      # Initially nil (no shards)
      assert Server.min_durable_version(server) == nil

      # Create some shards by pushing data
      txn = make_transaction([{:set, "key", "value"}], [{0, 1}])
      Server.push(server, <<0, 0, 0, 0, 0, 0, 10, 0>>, txn)
      :timer.sleep(50)

      # Should have a min durable version now
      min_version = Server.min_durable_version(server)
      assert min_version
    end
  end
end
