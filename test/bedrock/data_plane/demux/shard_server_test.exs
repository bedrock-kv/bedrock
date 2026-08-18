defmodule Bedrock.DataPlane.Demux.ShardServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem

  defmodule DelayedLocalFilesystem do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)

    @impl true
    def get(config, key), do: LocalFilesystem.get(config, key)

    @impl true
    def delete(config, key), do: LocalFilesystem.delete(config, key)

    @impl true
    def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)

    @impl true
    def put_if_not_exists(config, key, data, opts \\ []) do
      delay_ms = Keyword.get(config, :delay_ms, 0)
      Process.sleep(delay_ms)
      LocalFilesystem.put_if_not_exists(config, key, data, opts)
    end

    @impl true
    def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)

    @impl true
    def put_if_version_matches(config, key, version_token, data, opts \\ []) do
      LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
    end
  end

  defmodule FlakyLocalFilesystem do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)

    @impl true
    def get(config, key), do: LocalFilesystem.get(config, key)

    @impl true
    def delete(config, key), do: LocalFilesystem.delete(config, key)

    @impl true
    def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)

    @impl true
    def put_if_not_exists(config, key, data, opts \\ []) do
      attempts = Keyword.fetch!(config, :attempts)

      attempt =
        Agent.get_and_update(attempts, fn count ->
          next = count + 1
          {next, next}
        end)

      if attempt == 1 do
        {:error, :transient_write_failure}
      else
        LocalFilesystem.put_if_not_exists(config, key, data, opts)
      end
    end

    @impl true
    def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)

    @impl true
    def put_if_version_matches(config, key, version_token, data, opts \\ []) do
      LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
    end
  end

  setup do
    test_dir = Path.join(System.tmp_dir!(), "shard_server_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)
    backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
    demux_pid = self()

    registry_name = :"shard_server_test_registry_#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Registry.start_link(keys: :unique, name: registry_name)

    {:ok, pid} =
      ShardServer.start_link(
        shard_id: 0,
        demux: demux_pid,
        cluster: "test-cluster",
        object_storage: backend,
        registry: registry_name
      )

    on_exit(fn -> File.rm_rf!(test_dir) end)

    %{server: pid, backend: backend, demux: demux_pid, test_dir: test_dir, registry: registry_name}
  end

  defp make_slice(mutations) do
    Transaction.encode(%{
      mutations: mutations,
      commit_version: <<0, 0, 0, 0, 0, 0, 0, 1>>
    })
  end

  describe "start_link/1" do
    test "starts successfully with required options", %{backend: backend, registry: registry} do
      {:ok, pid} =
        ShardServer.start_link(
          shard_id: 99,
          demux: self(),
          cluster: "test",
          object_storage: backend,
          registry: registry
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

      {:ok, transactions, %{high_water: ^v2000}} = ShardServer.pull(server, v1000, timeout: 100, limit: 10)

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

      {:ok, transactions, _} = ShardServer.pull(server, v1500, timeout: 100, limit: 10)

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

      {:ok, transactions, _} = ShardServer.pull(server, Version.zero(), timeout: 100, limit: 3)

      assert length(transactions) == 3
    end

    test "returns storage_read_failed when chunk data is corrupted", %{server: server, backend: backend} do
      corrupted_key = Keys.chunk_path("0", 1_000)
      :ok = ObjectStorage.put(backend, corrupted_key, <<1, 2, 3, 4>>)

      assert {:error, {:storage_read_failed, :truncated_chunk}} =
               ShardServer.pull(server, Version.zero(), timeout: 100, limit: 10)
    end

    test "replays persisted chunks from object storage using version binaries", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 700

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1_000)
      v1200 = Version.from_integer(1_200)
      v1400 = Version.from_integer(1_400)
      cut = Version.from_integer(1_300)

      ShardServer.push(server, v1000, slice)
      ShardServer.push(server, v1200, slice)
      ShardServer.push(server, v1400, slice)
      ShardServer.flush(server, cut)

      assert_receive {:durable, ^shard_id, ^cut}, 1_500

      :ok = GenServer.stop(server)

      {:ok, replay_server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      on_exit(fn ->
        if Process.alive?(replay_server), do: GenServer.stop(replay_server)
      end)

      assert {:ok, txns, _} = ShardServer.pull(replay_server, Version.from_integer(900), timeout: 500, limit: 10)

      versions =
        txns
        |> Enum.map(fn {version, _slice} -> Version.to_integer(version) end)
        |> Enum.sort()

      assert 1_000 in versions
      assert 1_200 in versions
    end
  end

  describe "commanded cuts" do
    test "flushes buffered data at or below the commanded cut and reports the cut", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)

      {:ok, flush_server} =
        ShardServer.start_link(
          shard_id: 99,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1050 = Version.from_integer(1050)
      cut = Version.from_integer(1099)

      ShardServer.push(flush_server, v1000, slice)
      ShardServer.push(flush_server, v1050, slice)
      :timer.sleep(10)

      # No flush without a commanded cut
      refute_received {:durable, 99, _}

      ShardServer.flush(flush_server, cut)

      # The contribution is the CUT version, not the max flushed data version
      assert_receive {:durable, 99, ^cut}, 1000

      # The chunk on disk is named for the last commit it contains (1050)
      chunk_key = Keys.chunk_path("2r", 1050)
      assert {:ok, _} = ObjectStorage.get(backend, chunk_key)
    end

    test "confirms the cut immediately when the buffer is empty (idle shard)", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 600

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      cut = Version.from_integer(5_000)
      ShardServer.flush(server, cut)

      assert_receive {:durable, ^shard_id, ^cut}, 1000
      assert ShardServer.durable_version(server) == cut
    end

    test "retains buffered data above the cut", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 650

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1200 = Version.from_integer(1200)
      cut = Version.from_integer(1099)

      ShardServer.push(server, v1000, slice)
      ShardServer.push(server, v1200, slice)
      ShardServer.flush(server, cut)

      assert_receive {:durable, ^shard_id, ^cut}, 1000

      # v1200 is above the cut: still buffered, still pullable
      {:ok, transactions, _} = ShardServer.pull(server, v1200, timeout: 100, limit: 10)
      assert [{^v1200, _}] = transactions
    end

    test "queues cuts arriving while a flush is in flight", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(DelayedLocalFilesystem, root: test_dir, delay_ms: 100)
      shard_id = :erlang.unique_integer([:positive]) + 700

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      cut1 = Version.from_integer(1099)
      v1200 = Version.from_integer(1200)
      cut2 = Version.from_integer(1299)

      ShardServer.push(server, v1000, slice)
      ShardServer.flush(server, cut1)
      ShardServer.push(server, v1200, slice)
      ShardServer.flush(server, cut2)

      assert_receive {:durable, ^shard_id, ^cut1}, 1_500
      assert_receive {:durable, ^shard_id, ^cut2}, 1_500
      assert ShardServer.durable_version(server) == cut2
    end

    test "push path remains responsive while persistence is in-flight", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(DelayedLocalFilesystem, root: test_dir, delay_ms: 200)
      shard_id = :erlang.unique_integer([:positive]) + 200

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1200 = Version.from_integer(1200)

      ShardServer.push(server, v1000, slice)
      ShardServer.flush(server, Version.from_integer(1100))
      ShardServer.push(server, v1200, slice)

      # With async persistence, latest_version remains callable even while flush
      # work is still running in the background worker.
      assert v1200 == GenServer.call(server, :latest_version, 50)
    end

    test "durable watermark advances only after confirmed persistence", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(DelayedLocalFilesystem, root: test_dir, delay_ms: 150)
      shard_id = :erlang.unique_integer([:positive]) + 300

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      cut = Version.from_integer(1100)

      ShardServer.push(server, v1000, slice)
      ShardServer.flush(server, cut)

      Process.sleep(25)
      assert ShardServer.durable_version(server) == nil
      refute_received {:durable, ^shard_id, _}

      assert_receive {:durable, ^shard_id, ^cut}, 1_500
      assert ShardServer.durable_version(server) == cut
    end
  end

  describe "persistence telemetry" do
    test "emits write success and watermark advancement telemetry", %{test_dir: test_dir, registry: registry} do
      test_pid = self()
      shard_id = :erlang.unique_integer([:positive]) + 400
      handler_id = "shard-persistence-success-#{shard_id}"

      :ok =
        :telemetry.attach_many(
          handler_id,
          [
            [:bedrock, :demux, :persistence, :write, :ok],
            [:bedrock, :demux, :persistence, :watermark, :advanced]
          ],
          &__MODULE__.handle_telemetry_event/4,
          test_pid
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1200 = Version.from_integer(1200)

      ShardServer.push(server, v1000, slice)
      ShardServer.push(server, v1200, slice)
      ShardServer.flush(server, Version.from_integer(1300))

      assert_receive {:telemetry, [:bedrock, :demux, :persistence, :write, :ok], write_meas, write_meta}, 1_500
      assert write_meta.shard_id == shard_id
      assert write_meas.batch_size >= 1
      assert write_meas.duration_us >= 0

      assert_receive {:telemetry, [:bedrock, :demux, :persistence, :watermark, :advanced], wm_meas, wm_meta}, 1_500
      assert wm_meta.shard_id == shard_id
      assert wm_meta.durable_version == Version.from_integer(1300)
      assert wm_meas.buffered_transactions >= 0
    end

    test "emits retry telemetry for transient object write failures", %{test_dir: test_dir, registry: registry} do
      test_pid = self()
      shard_id = :erlang.unique_integer([:positive]) + 500
      handler_id = "shard-persistence-retry-#{shard_id}"
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      :ok =
        :telemetry.attach_many(
          handler_id,
          [
            [:bedrock, :demux, :persistence, :write, :error],
            [:bedrock, :demux, :persistence_queue, :retry_scheduled],
            [:bedrock, :demux, :persistence, :write, :ok]
          ],
          &__MODULE__.handle_telemetry_event/4,
          test_pid
        )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        if Process.alive?(attempts), do: Agent.stop(attempts)
      end)

      backend = ObjectStorage.backend(FlakyLocalFilesystem, root: test_dir, attempts: attempts)

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry,
          persistence_retry_backoff_ms: 1,
          persistence_retry_tick_ms: 1
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1200 = Version.from_integer(1200)

      ShardServer.push(server, v1000, slice)
      ShardServer.push(server, v1200, slice)
      ShardServer.flush(server, Version.from_integer(1300))

      assert_receive {:telemetry, [:bedrock, :demux, :persistence, :write, :error], _meas, error_meta}, 1_500
      assert error_meta.shard_id == shard_id
      assert error_meta.reason == {:write_failed, :transient_write_failure}

      assert_receive {:telemetry, [:bedrock, :demux, :persistence_queue, :retry_scheduled], retry_meas, retry_meta},
                     1_500

      assert retry_meas.attempt == 1
      assert retry_meas.delay_ms >= 1
      assert retry_meta.reason == {:write_failed, :transient_write_failure}

      assert_receive {:telemetry, [:bedrock, :demux, :persistence, :write, :ok], _ok_meas, ok_meta}, 1_500
      assert ok_meta.shard_id == shard_id
    end
  end

  describe "permanent flush failure" do
    defmodule BrokenLocalFilesystem do
      @moduledoc false
      @behaviour ObjectStorage

      @impl true
      def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)

      @impl true
      def get(config, key), do: LocalFilesystem.get(config, key)

      @impl true
      def delete(config, key), do: LocalFilesystem.delete(config, key)

      @impl true
      def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)

      @impl true
      def put_if_not_exists(_config, _key, _data, _opts \\ []), do: {:error, :permanently_broken}

      @impl true
      def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)

      @impl true
      def put_if_version_matches(config, key, version_token, data, opts \\ []) do
        LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
      end
    end

    test "crashes the ShardServer when a flush exhausts its retries", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(BrokenLocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 800

      Process.flag(:trap_exit, true)

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry,
          persistence_max_retries: 1,
          persistence_retry_backoff_ms: 1,
          persistence_retry_tick_ms: 1
        )

      slice = make_slice([{:set, "key", "value"}])
      ShardServer.push(server, Version.from_integer(1000), slice)
      ShardServer.flush(server, Version.from_integer(1100))

      # A permanently failing flush must not wedge silently: the server
      # crashes so the linked Demux -> Log chain converts it into recovery.
      assert_receive {:EXIT, ^server, {:flush_permanently_failed, meta}}, 2_000
      assert meta.shard_id == shard_id
      refute_received {:durable, ^shard_id, _}
    end
  end

  describe "stream continuity" do
    test "a pull from an old position serves the chunk range before the buffer", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 900

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v1200 = Version.from_integer(1200)
      v1400 = Version.from_integer(1400)
      cut = Version.from_integer(1300)

      ShardServer.push(server, v1000, slice)
      ShardServer.push(server, v1200, slice)
      ShardServer.flush(server, cut)
      assert_receive {:durable, ^shard_id, ^cut}, 1_500

      # Recent data lands in the buffer after the cut confirmed
      ShardServer.push(server, v1400, slice)

      # From an old position the stream must begin with the chunk range —
      # never skip ahead to the buffer.
      {:ok, transactions, _} = ShardServer.pull(server, Version.from_integer(900), timeout: 100, limit: 10)
      assert [{^v1000, _}, {^v1200, _}] = transactions

      # And the next pull picks up seamlessly in the buffer.
      {:ok, transactions, _} = ShardServer.pull(server, Version.from_integer(1201), timeout: 100, limit: 10)
      assert [{^v1400, _}] = transactions
    end
  end

  describe "version currency" do
    test "pull replies carry the high-water and known-committed versions", %{server: server} do
      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      kcv = Version.from_integer(950)

      ShardServer.push(server, v1000, slice, kcv)

      assert {:ok, [{^v1000, _}], %{high_water: ^v1000, kcv: ^kcv}} =
               ShardServer.pull(server, v1000, timeout: 100, limit: 10)
    end

    test "a currency advance wakes waiters with an empty reply through the high-water", %{server: server} do
      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      hw = Version.from_integer(3000)
      kcv = Version.from_integer(2900)
      test_pid = self()

      ShardServer.push(server, v1000, slice)

      spawn(fn ->
        result = ShardServer.pull(server, Version.from_integer(2000), timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, result})
      end)

      :timer.sleep(50)

      # The demux answers a currency request: no data for this shard, but the
      # cluster is current through 3000.
      send(server, {:currency, hw, kcv})

      assert_receive {:pull_result, {:ok, [], %{high_water: ^hw, kcv: ^kcv}}}, 1_000
    end

    test "parking a puller requests currency from the demux immediately — no timers", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 950
      test_pid = self()

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      spawn(fn ->
        result = ShardServer.pull(server, Version.from_integer(2000), timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, result})
      end)

      # The park itself triggers the request, carrying what this shard has
      # already seen so the demux can hold the reply until it knows more.
      assert_receive {:currency_request, from_pid, nil}, 1_000

      hw = Version.from_integer(2500)
      send(from_pid, {:currency, hw, hw})

      assert_receive {:pull_result, {:ok, [], %{high_water: ^hw}}}, 1_000
    end

    test "a shard server with still-parked pullers re-registers after each advance", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 960
      test_pid = self()

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test-cluster",
          object_storage: backend,
          registry: registry
        )

      # Two pullers at different positions
      spawn(fn ->
        result = ShardServer.pull(server, Version.from_integer(2000), timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, :near, result})
      end)

      spawn(fn ->
        result = ShardServer.pull(server, Version.from_integer(5000), timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, :far, result})
      end)

      assert_receive {:currency_request, from_pid, _seen}, 1_000

      # An advance to 3000 satisfies only the near puller...
      hw = Version.from_integer(3000)
      send(from_pid, {:currency, hw, hw})
      assert_receive {:pull_result, :near, {:ok, [], %{high_water: ^hw}}}, 1_000

      # ...so the shard server re-registers for the next advance, telling
      # the demux what it has now seen.
      assert_receive {:currency_request, ^from_pid, ^hw}, 1_000

      hw2 = Version.from_integer(6000)
      send(from_pid, {:currency, hw2, hw2})
      assert_receive {:pull_result, :far, {:ok, [], %{high_water: ^hw2}}}, 1_000
    end
  end

  describe "waiting/notification" do
    test "notifies waiting pullers when data arrives", %{test_dir: test_dir, registry: registry} do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)

      {:ok, server} =
        ShardServer.start_link(
          shard_id: 1,
          demux: self(),
          cluster: "test",
          object_storage: backend,
          registry: registry
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
      assert_receive {:pull_result, {:ok, transactions, _}}, 1_000
      assert length(transactions) >= 1
    end

    test "a woken waiter receives data from its own start position, not from zero", %{
      test_dir: test_dir,
      registry: registry
    } do
      backend = ObjectStorage.backend(LocalFilesystem, root: test_dir)
      shard_id = :erlang.unique_integer([:positive]) + 970

      {:ok, server} =
        ShardServer.start_link(
          shard_id: shard_id,
          demux: self(),
          cluster: "test",
          object_storage: backend,
          registry: registry
        )

      test_pid = self()
      slice = make_slice([{:set, "key", "value"}])
      v1000 = Version.from_integer(1000)
      v2500 = Version.from_integer(2500)

      # Older data already sits in the buffer
      ShardServer.push(server, v1000, slice)

      spawn(fn ->
        result = ShardServer.pull(server, Version.from_integer(2000), timeout: 5_000, limit: 10)
        send(test_pid, {:pull_result, result})
      end)

      :timer.sleep(50)
      ShardServer.push(server, v2500, slice)

      # The waiter asked for 2000 onward: v1000 must not be replayed to it.
      assert_receive {:pull_result, {:ok, [{^v2500, _}], _}}, 1_000
    end
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end
end
