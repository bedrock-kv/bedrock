defmodule Bedrock.DataPlane.Demux.ShardServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    test_dir = Path.join(System.tmp_dir!(), "shard_server_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
    demux_pid = self()

    {:ok, pid} =
      ShardServer.start_link(
        shard_id: 0,
        demux: demux_pid,
        cluster: "test-cluster",
        object_storage: backend,
        # Large gap to prevent flushing in most tests
        version_gap: 1_000_000
      )

    on_exit(fn -> File.rm_rf!(test_dir) end)

    %{server: pid, backend: backend, demux: demux_pid, test_dir: test_dir}
  end

  defp make_slice(mutations) do
    Transaction.encode(%{
      mutations: mutations,
      commit_version: <<0, 0, 0, 0, 0, 0, 0, 1>>
    })
  end

  describe "start_link/1" do
    test "starts successfully with required options", %{backend: backend} do
      {:ok, pid} =
        ShardServer.start_link(
          shard_id: 99,
          demux: self(),
          cluster: "test",
          object_storage: backend
        )

      assert Process.alive?(pid)
    end
  end

  describe "push/3" do
    test "accepts transaction slices", %{server: server} do
      slice = make_slice([{:set, "key", "value"}])
      assert :ok = ShardServer.push(server, Version.from_integer(1000), slice)
    end

    test "updates latest_version", %{server: server} do
      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v2000 = Version.from_integer(2000)

      ShardServer.push(server, v1000, slice)
      # Give it time to process
      :timer.sleep(10)

      assert ShardServer.latest_version(server) == v1000

      ShardServer.push(server, v2000, slice)
      :timer.sleep(10)

      assert ShardServer.latest_version(server) == v2000
    end
  end

  describe "pull/3" do
    test "times out when no data available", %{server: server} do
      # Long-pull semantics: wait for data, timeout if none arrives
      assert {:error, :timeout} = ShardServer.pull(server, Version.from_integer(1000), timeout: 10, limit: 10)
    end

    test "returns buffered transactions", %{server: server} do
      slice1 = make_slice([{:set, "key1", "value1"}])
      slice2 = make_slice([{:set, "key2", "value2"}])
      v1000 = Version.from_integer(1000)
      v2000 = Version.from_integer(2000)

      ShardServer.push(server, v1000, slice1)
      ShardServer.push(server, v2000, slice2)
      # Give time to process
      :timer.sleep(50)

      {:ok, transactions} = ShardServer.pull(server, v1000, timeout: 100, limit: 10)

      assert length(transactions) == 2
      assert {^v1000, ^slice1} = Enum.find(transactions, fn {v, _} -> v == v1000 end)
      assert {^v2000, ^slice2} = Enum.find(transactions, fn {v, _} -> v == v2000 end)
    end

    test "respects from_version filter", %{server: server} do
      slice1 = make_slice([{:set, "key1", "value1"}])
      slice2 = make_slice([{:set, "key2", "value2"}])
      v1000 = Version.from_integer(1000)
      v1500 = Version.from_integer(1500)
      v2000 = Version.from_integer(2000)

      ShardServer.push(server, v1000, slice1)
      ShardServer.push(server, v2000, slice2)
      :timer.sleep(50)

      {:ok, transactions} = ShardServer.pull(server, v1500, timeout: 100, limit: 10)

      assert length(transactions) == 1
      assert {^v2000, _} = hd(transactions)
    end

    test "respects limit", %{server: server} do
      slice = make_slice([{:set, "key", "value"}])

      for v <- 1..10 do
        ShardServer.push(server, Version.from_integer(v * 1000), slice)
      end

      # Give more time for all to process
      :timer.sleep(100)

      {:ok, transactions} = ShardServer.pull(server, Version.zero(), timeout: 100, limit: 3)

      assert length(transactions) == 3
    end
  end

  describe "flushing" do
    test "flushes when version gap exceeded", %{test_dir: test_dir} do
      # Create a server with small version gap for flush testing
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)

      {:ok, flush_server} =
        ShardServer.start_link(
          shard_id: 99,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          # Small gap for testing flushing
          version_gap: 100
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1050 = Version.from_integer(1050)
      v1200 = Version.from_integer(1200)

      # Push transactions with gap < threshold
      ShardServer.push(flush_server, v1000, slice)
      ShardServer.push(flush_server, v1050, slice)
      :timer.sleep(10)

      # Should not have received durability report yet
      refute_received {:durable, 99, _}

      # Push transaction that exceeds the gap (version_gap: 100)
      ShardServer.push(flush_server, v1200, slice)

      # Should receive durability report (use assert_receive with timeout for reliability)
      assert_receive {:durable, 99, durable_version}, 1000
      assert durable_version >= v1000
    end
  end

  describe "waiting/notification" do
    test "notifies waiting pullers when data arrives", %{test_dir: test_dir} do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)

      {:ok, server} =
        ShardServer.start_link(
          shard_id: 1,
          demux: self(),
          cluster: "test",
          object_storage: backend,
          # High gap to prevent flushing
          version_gap: 1_000_000
        )

      test_pid = self()
      v1000 = Version.from_integer(1000)

      # Start a pull in a separate process that will wait
      spawn(fn ->
        result = ShardServer.pull(server, v1000, timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, result})
      end)

      # Give it time to register the wait
      :timer.sleep(50)

      # Push data
      slice = make_slice([{:set, "key", "value"}])
      ShardServer.push(server, v1000, slice)

      # Should receive the data
      assert_receive {:pull_result, {:ok, transactions}}, 1_000
      assert length(transactions) >= 1
    end
  end
end
