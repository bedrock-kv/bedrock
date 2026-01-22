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

    # Use unique shard IDs per test to avoid global registration conflicts
    # ShardServer uses {:global, {ShardServer, shard_id}} by default
    shard_base = :erlang.unique_integer([:positive]) * 1000

    %{server: server, backend: backend, log: log_pid, test_dir: test_dir, shard_base: shard_base}
  end

  defp make_transaction(mutations, shard_index) do
    Transaction.encode(%{
      mutations: mutations,
      shard_index: shard_index,
      commit_version: <<0, 0, 0, 0, 0, 0, 10, 0>>
    })
  end

  describe "push/3" do
    test "routes transaction to correct ShardServer", %{server: server, shard_base: shard_base} do
      shard_id = shard_base + 1
      # Transaction with mutations for unique shard
      mutations = [{:set, "key1", "value1"}, {:set, "key2", "value2"}]
      txn = make_transaction(mutations, [{shard_id, 2}])
      version = <<0, 0, 0, 0, 0, 0, 10, 0>>

      :ok = Server.push(server, version, txn)
      :timer.sleep(50)

      # Verify ShardServer was created and received data
      {:ok, shard_server} = Server.get_shard_server(server, shard_id)
      assert ShardServer.latest_version(shard_server) == version
    end

    test "routes to multiple ShardServers", %{server: server, shard_base: shard_base} do
      shard_a = shard_base + 2
      shard_b = shard_base + 3
      # Transaction touching two unique shards
      mutations = [
        {:set, "shard_a_key", "value"},
        {:set, "shard_b_key", "value"}
      ]

      txn = make_transaction(mutations, [{shard_a, 1}, {shard_b, 1}])
      version = <<0, 0, 0, 0, 0, 0, 20, 0>>

      :ok = Server.push(server, version, txn)
      :timer.sleep(50)

      # Both ShardServers should exist and have data
      {:ok, server_a} = Server.get_shard_server(server, shard_a)
      {:ok, server_b} = Server.get_shard_server(server, shard_b)

      assert ShardServer.latest_version(server_a) == version
      assert ShardServer.latest_version(server_b) == version
    end
  end

  describe "get_shard_server/2" do
    test "creates ShardServer on demand", %{server: server, shard_base: shard_base} do
      shard_id = shard_base + 10
      {:ok, shard_server} = Server.get_shard_server(server, shard_id)
      assert is_pid(shard_server)
      assert Process.alive?(shard_server)
    end

    test "returns same pid on subsequent calls", %{server: server, shard_base: shard_base} do
      shard_id = shard_base + 11
      {:ok, pid1} = Server.get_shard_server(server, shard_id)
      {:ok, pid2} = Server.get_shard_server(server, shard_id)
      assert pid1 == pid2
    end
  end

  describe "durability tracking" do
    test "tracks minimum durable version", %{server: server, shard_base: shard_base} do
      shard_id = shard_base + 20
      # Initially nil (no shards)
      assert Server.min_durable_version(server) == nil

      # Create some shards by pushing data
      txn = make_transaction([{:set, "key", "value"}], [{shard_id, 1}])
      Server.push(server, <<0, 0, 0, 0, 0, 0, 10, 0>>, txn)
      :timer.sleep(50)

      # Should have a min durable version now
      min_version = Server.min_durable_version(server)
      assert min_version
    end
  end
end
