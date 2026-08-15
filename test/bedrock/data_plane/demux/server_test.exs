defmodule Bedrock.DataPlane.Demux.ServerTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux.Server
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
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
    make_transaction(mutations, shard_index, <<0, 0, 0, 0, 0, 0, 10, 0>>)
  end

  defp make_transaction(mutations, shard_index, commit_version) do
    Transaction.encode(%{
      mutations: mutations,
      shard_index: shard_index,
      commit_version: commit_version
    })
  end

  describe "push/3" do
    test "routes transaction to correct ShardServer", %{server: server, shard_base: shard_base} do
      shard_id = shard_base + 1
      # Transaction with mutations for unique shard
      mutations = [{:set, "key1", "value1"}, {:set, "key2", "value2"}]
      txn = make_transaction(mutations, [{shard_id, 2}])
      version = <<0, 0, 0, 0, 0, 0, 10, 0>>

      :ok = Server.push(server, version, txn, version)
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

      :ok = Server.push(server, version, txn, version)
      :timer.sleep(50)

      # Both ShardServers should exist and have data
      {:ok, server_a} = Server.get_shard_server(server, shard_a)
      {:ok, server_b} = Server.get_shard_server(server, shard_b)

      assert ShardServer.latest_version(server_a) == version
      assert ShardServer.latest_version(server_b) == version
    end

    test "applies shard_server_opts to newly created shard servers", %{
      backend: backend,
      log: log_pid,
      shard_base: shard_base
    } do
      {:ok, tuned_server} =
        Server.start_link(
          cluster: "test-cluster",
          object_storage: backend,
          log: log_pid,
          shard_server_opts: [
            persistence_retry_backoff_ms: 1,
            persistence_retry_tick_ms: 1
          ]
        )

      on_exit(fn ->
        try do
          if Process.alive?(tuned_server), do: GenServer.stop(tuned_server, :shutdown)
        catch
          :exit, _ -> :ok
        end
      end)

      shard_id = shard_base + 30
      txn = make_transaction([{:set, "k", "v"}], [{shard_id, 1}])
      version = <<0, 0, 0, 0, 0, 0, 3, 232>>

      :ok = Server.push(tuned_server, version, txn, version)

      {:ok, shard_server} = Server.get_shard_server(tuned_server, shard_id)
      assert %{persistence_worker: persistence_worker} = :sys.get_state(shard_server)

      assert %{
               retry_tick_ms: 1,
               queue: %{retry_base_backoff_ms: 1}
             } = :sys.get_state(persistence_worker)
    end
  end

  describe "deterministic cuts" do
    defp start_cut_server(backend, log_pid, cut_interval_us) do
      {:ok, server} =
        Server.start_link(
          cluster: "test-cluster",
          object_storage: backend,
          log: log_pid,
          cut_interval_us: cut_interval_us
        )

      # The demux dies with its log (the test process); this is best-effort
      # cleanup for the case where it outlives the exit signal.
      on_exit(fn -> safe_stop(server) end)

      server
    end

    defp safe_stop(server) do
      if Process.alive?(server), do: GenServer.stop(server, :shutdown)
    catch
      :exit, _ -> :ok
    end

    defp version_in_bucket(bucket, offset, interval), do: Version.from_integer(bucket * interval + offset)

    defp cut_of_bucket(bucket, interval), do: Version.from_integer((bucket + 1) * interval - 1)

    test "bucket crossing commands a flush and the confirmed cut reaches the log", %{
      backend: backend,
      shard_base: shard_base
    } do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)
      shard_id = shard_base + 40

      txn = fn v -> make_transaction([{:set, "k", "v"}], [{shard_id, 1}], v) end

      # Data lands in bucket 5; the push that crosses into bucket 6 closes it.
      v_in_bucket = version_in_bucket(5, 100, interval)
      v_crossing = version_in_bucket(6, 0, interval)
      cut = cut_of_bucket(5, interval)

      :ok = Server.push(server, v_in_bucket, txn.(v_in_bucket), v_in_bucket)
      :ok = Server.push(server, v_crossing, txn.(v_crossing), v_crossing)

      assert_receive {:min_durable_version, _, ^cut}, 1_500
    end

    test "a cut is deferred until the known-committed version reaches it", %{
      backend: backend,
      shard_base: shard_base
    } do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)
      shard_id = shard_base + 45

      txn = fn v -> make_transaction([{:set, "k", "v"}], [{shard_id, 1}], v) end

      v_in_bucket = version_in_bucket(5, 100, interval)
      v_crossing = version_in_bucket(6, 0, interval)
      v_later = version_in_bucket(6, 10, interval)
      cut = cut_of_bucket(5, interval)

      # The bucket closes, but the known-committed watermark still trails
      # behind the cut: nothing may become durable yet.
      :ok = Server.push(server, v_in_bucket, txn.(v_in_bucket), v_in_bucket)
      :ok = Server.push(server, v_crossing, txn.(v_crossing), v_in_bucket)

      refute_receive {:min_durable_version, _, _}, 200

      # The watermark catches up on a later push: the deferred cut fires.
      :ok = Server.push(server, v_later, txn.(v_later), v_crossing)

      assert_receive {:min_durable_version, _, ^cut}, 1_500
    end

    test "an uncommitted tail is never flushed (no shards)", %{backend: backend} do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)

      heartbeat = fn v -> make_transaction([], [], v) end

      v1 = version_in_bucket(3, 10, interval)
      v2 = version_in_bucket(4, 10, interval)

      # The watermark never reaches the cut: the floor must not advance.
      :ok = Server.push(server, v1, heartbeat.(v1), v1)
      :ok = Server.push(server, v2, heartbeat.(v2), v1)

      refute_receive {:min_durable_version, _, _}, 200
      assert Server.min_durable_version(server) == nil
    end

    test "heartbeat-only stream (no shards) still advances the floor", %{backend: backend} do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)

      heartbeat = fn v -> make_transaction([], [], v) end

      v1 = version_in_bucket(3, 10, interval)
      v2 = version_in_bucket(4, 10, interval)
      cut = cut_of_bucket(3, interval)

      :ok = Server.push(server, v1, heartbeat.(v1), v1)
      :ok = Server.push(server, v2, heartbeat.(v2), v2)

      assert_receive {:min_durable_version, _, ^cut}, 1_500
      assert Server.min_durable_version(server) == cut
    end

    test "a quiet stretch spanning several buckets is closed by one cut", %{backend: backend} do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)

      heartbeat = fn v -> make_transaction([], [], v) end

      v1 = version_in_bucket(1, 0, interval)
      v9 = version_in_bucket(9, 0, interval)
      cut = cut_of_bucket(8, interval)

      :ok = Server.push(server, v1, heartbeat.(v1), v1)
      :ok = Server.push(server, v9, heartbeat.(v9), v9)

      assert_receive {:min_durable_version, _, ^cut}, 1_500
    end

    test "a shard activated mid-bucket starts at the last completed cut, not a buffered version", %{
      backend: backend,
      shard_base: shard_base
    } do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)
      shard_id = shard_base + 41

      heartbeat = fn v -> make_transaction([], [], v) end
      txn = fn v -> make_transaction([{:set, "k", "v"}], [{shard_id, 1}], v) end

      # Establish a completed cut with heartbeats only
      v1 = version_in_bucket(1, 0, interval)
      v2 = version_in_bucket(2, 0, interval)
      cut1 = cut_of_bucket(1, interval)

      :ok = Server.push(server, v1, heartbeat.(v1), v1)
      :ok = Server.push(server, v2, heartbeat.(v2), v2)
      assert_receive {:min_durable_version, _, ^cut1}, 1_500

      # First slice for the shard arrives mid-bucket: its data is only
      # buffered, so the min must stay at the last completed cut.
      v_mid = version_in_bucket(2, 500, interval)
      :ok = Server.push(server, v_mid, txn.(v_mid), v_mid)
      :timer.sleep(50)

      assert Server.min_durable_version(server) == cut1
    end

    test "floor advancement telemetry names the pinning shard", %{backend: backend, shard_base: shard_base} do
      test_pid = self()
      handler_id = "demux-floor-telemetry-#{:erlang.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:bedrock, :demux, :durability, :floor_advanced],
          fn event, measurements, metadata, pid -> send(pid, {:telemetry, event, measurements, metadata}) end,
          test_pid
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      interval = 1_000
      server = start_cut_server(backend, self(), interval)
      shard_id = shard_base + 44

      txn = fn v -> make_transaction([{:set, "k", "v"}], [{shard_id, 1}], v) end

      v_in_bucket = version_in_bucket(5, 100, interval)
      v_crossing = version_in_bucket(6, 0, interval)
      cut = cut_of_bucket(5, interval)

      :ok = Server.push(server, v_in_bucket, txn.(v_in_bucket), v_in_bucket)
      :ok = Server.push(server, v_crossing, txn.(v_crossing), v_crossing)

      assert_receive {:telemetry, [:bedrock, :demux, :durability, :floor_advanced], measurements, metadata}, 1_500
      assert metadata.min_durable_version == cut
      assert metadata.pinning_shard_id == shard_id
      assert measurements.active_shards == 1
    end

    test "idle shards do not pin the floor", %{backend: backend, shard_base: shard_base} do
      interval = 1_000
      server = start_cut_server(backend, self(), interval)
      idle_shard = shard_base + 42
      busy_shard = shard_base + 43

      txn = fn shard, v -> make_transaction([{:set, "k", "v"}], [{shard, 1}], v) end

      # The idle shard receives data once, in bucket 1
      v_idle = version_in_bucket(1, 100, interval)
      :ok = Server.push(server, v_idle, txn.(idle_shard, v_idle), v_idle)

      # The busy shard keeps receiving data through buckets 2..4
      for bucket <- 2..4 do
        v = version_in_bucket(bucket, 100, interval)
        :ok = Server.push(server, v, txn.(busy_shard, v), v)
      end

      # The idle shard confirmed every cut without new data: the floor is the
      # last completed cut, not the idle shard's last flush.
      cut = cut_of_bucket(3, interval)
      assert_receive {:min_durable_version, _, ^cut}, 1_500
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
      Server.push(server, <<0, 0, 0, 0, 0, 0, 10, 0>>, txn, <<0, 0, 0, 0, 0, 0, 10, 0>>)
      :timer.sleep(50)

      # Should have a min durable version now
      min_version = Server.min_durable_version(server)
      assert min_version
    end
  end
end
