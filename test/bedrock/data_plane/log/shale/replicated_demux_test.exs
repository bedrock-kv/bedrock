defmodule Bedrock.DataPlane.Log.Shale.ReplicatedDemuxTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Demux.Server, as: Demux
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Server, as: Shale
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir

  defmodule GatedObjectStorage do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def put(config, key, data, opts), do: ObjectStorage.put(delegate(config), key, data, opts)

    @impl true
    def get(config, key), do: ObjectStorage.get(delegate(config), key)

    @impl true
    def delete(config, key), do: ObjectStorage.delete(delegate(config), key)

    @impl true
    def list(config, prefix, opts), do: ObjectStorage.list(delegate(config), prefix, opts)

    @impl true
    def put_if_not_exists(config, key, data, opts) do
      ref = make_ref()
      data = IO.iodata_to_binary(data)
      send(Keyword.fetch!(config, :controller), {:conditional_create_waiting, self(), ref, key, data})

      receive do
        {:release_conditional_create, ^ref} ->
          result = ObjectStorage.put_if_not_exists(delegate(config), key, data, opts)
          send(Keyword.fetch!(config, :controller), {:conditional_create_finished, ref, result})
          result
      end
    end

    @impl true
    def get_with_version(config, key), do: ObjectStorage.get_with_version(delegate(config), key)

    @impl true
    def put_if_version_matches(config, key, version, data, opts) do
      ObjectStorage.put_if_version_matches(delegate(config), key, version, data, opts)
    end

    defp delegate(config), do: Keyword.fetch!(config, :delegate)
  end

  test "replica-local confirmation alone advances and trims each log", %{tmp_dir: tmp_dir} do
    object_root = Path.join(tmp_dir, "objects")
    first_wal_root = Path.join(tmp_dir, "first-wal")
    second_wal_root = Path.join(tmp_dir, "second-wal")
    File.mkdir_p!(object_root)
    File.mkdir_p!(first_wal_root)
    File.mkdir_p!(second_wal_root)

    shared_backend = ObjectStorage.backend(LocalFilesystem, root: object_root)

    gated_backend =
      ObjectStorage.backend(GatedObjectStorage,
        delegate: shared_backend,
        controller: self()
      )

    first_log = start_log("first", first_wal_root, shared_backend)
    second_log = start_log("second", second_wal_root, gated_backend)

    shard_id = 17
    first_version = Version.from_integer(1_000)
    crossing_version = Version.from_integer(Demux.default_cut_interval_us())
    cut_version = Version.from_integer(Demux.default_cut_interval_us() - 1)

    first_transaction =
      TransactionTestSupport.new_log_transaction(first_version, %{"key" => "first"}, shard_id: shard_id)

    crossing_transaction =
      TransactionTestSupport.new_log_transaction(crossing_version, %{"key" => "second"}, shard_id: shard_id)

    :ok = Log.push(first_log, first_transaction, Version.zero(), known_committed_version: first_version)
    :ok = Log.push(second_log, first_transaction, Version.zero(), known_committed_version: first_version)

    assert {:ok, first_shard_server} = Log.get_shard_server(first_log, shard_id)
    assert {:ok, second_shard_server} = Log.get_shard_server(second_log, shard_id)
    assert first_shard_server != second_shard_server

    first_demux = :sys.get_state(first_log).demux
    second_demux = :sys.get_state(second_log).demux
    assert %{demux: ^first_demux} = :sys.get_state(first_shard_server)
    assert %{demux: ^second_demux} = :sys.get_state(second_shard_server)

    assert {:ok, first_slices, _currency} =
             ShardServer.pull(first_shard_server, first_version, timeout: 100, limit: 10)

    assert {:ok, ^first_slices, _currency} =
             ShardServer.pull(second_shard_server, first_version, timeout: 100, limit: 10)

    :ok = Log.push(first_log, crossing_transaction, first_version, known_committed_version: crossing_version)
    :ok = Log.push(second_log, crossing_transaction, first_version, known_committed_version: crossing_version)

    assert_receive {:conditional_create_waiting, persistence_worker, gate_ref, chunk_key, proposed_chunk}, 2_000

    eventually(fn ->
      assert %{min_durable_version: ^cut_version, segments: []} = :sys.get_state(first_log)
    end)

    second_state = :sys.get_state(second_log)
    assert second_state.min_durable_version == nil
    assert [_ | _] = retained_segments = second_state.segments
    assert Enum.all?(retained_segments, &File.exists?(&1.path))

    assert {:ok, stored_chunk} = ObjectStorage.get(shared_backend, chunk_key)
    assert proposed_chunk == stored_chunk

    first_state = :sys.get_state(first_log)
    assert first_state.demux == first_demux
    first_log_ref = Process.monitor(first_log)
    first_demux_ref = Process.monitor(first_demux)
    Process.exit(first_log, :kill)

    assert_receive {:DOWN, ^first_log_ref, :process, ^first_log, :killed}, 2_000
    assert_receive {:DOWN, ^first_demux_ref, :process, ^first_demux, _reason}, 2_000

    assert Process.alive?(second_log)
    assert Process.alive?(second_shard_server)
    assert %{min_durable_version: nil} = :sys.get_state(second_log)
    assert Enum.all?(retained_segments, &File.exists?(&1.path))

    send(persistence_worker, {:release_conditional_create, gate_ref})
    assert_receive {:conditional_create_finished, ^gate_ref, {:error, :already_exists}}, 2_000

    eventually(fn ->
      assert %{min_durable_version: ^cut_version, segments: []} = :sys.get_state(second_log)
      refute Enum.any?(retained_segments, &File.exists?(&1.path))
    end)
  end

  test "replicas produce identical ordered slices and chunks from different arrival orders", %{tmp_dir: tmp_dir} do
    object_root = Path.join(tmp_dir, "deterministic-objects")
    first_wal_root = Path.join(tmp_dir, "deterministic-first-wal")
    second_wal_root = Path.join(tmp_dir, "deterministic-second-wal")
    File.mkdir_p!(object_root)
    File.mkdir_p!(first_wal_root)
    File.mkdir_p!(second_wal_root)

    shared_backend = ObjectStorage.backend(LocalFilesystem, root: object_root)

    gated_backend =
      ObjectStorage.backend(GatedObjectStorage,
        delegate: shared_backend,
        controller: self()
      )

    first_log = start_log("ordered-first", first_wal_root, gated_backend)
    second_log = start_log("ordered-second", second_wal_root, gated_backend)

    shard_id = 23
    first_version = Version.from_integer(1_000)
    second_version = Version.from_integer(2_000)
    crossing_version = Version.from_integer(Demux.default_cut_interval_us())
    heartbeat_version = Version.increment(crossing_version)

    first =
      TransactionTestSupport.new_log_transaction(first_version, %{"first" => "1"}, shard_id: shard_id)

    second =
      TransactionTestSupport.new_log_transaction(second_version, %{"second" => "2"}, shard_id: shard_id)

    crossing =
      TransactionTestSupport.new_log_transaction(crossing_version, %{"third" => "3"}, shard_id: shard_id)

    heartbeat = TransactionTestSupport.new_log_transaction(heartbeat_version, %{})

    first_crossing = queue_push(first_log, crossing, second_version)
    first_second = queue_push(first_log, second, first_version)
    assert :ok = Log.push(first_log, first, Version.zero(), known_committed_version: Version.zero())
    assert :ok = Task.await(first_second, 2_000)
    assert :ok = Task.await(first_crossing, 2_000)

    second_second = queue_push(second_log, second, first_version)
    second_crossing = queue_push(second_log, crossing, second_version)
    assert :ok = Log.push(second_log, first, Version.zero(), known_committed_version: Version.zero())
    assert :ok = Task.await(second_second, 2_000)
    assert :ok = Task.await(second_crossing, 2_000)

    assert {:ok, first_shard_server} = Log.get_shard_server(first_log, shard_id)
    assert {:ok, second_shard_server} = Log.get_shard_server(second_log, shard_id)

    expected_versions = [first_version, second_version, crossing_version]

    assert {:ok, first_slices, _currency} =
             ShardServer.pull(first_shard_server, first_version, timeout: 100, limit: 10)

    assert {:ok, second_slices, _currency} =
             ShardServer.pull(second_shard_server, first_version, timeout: 100, limit: 10)

    assert Enum.map(first_slices, &elem(&1, 0)) == expected_versions
    assert second_slices == first_slices

    assert :ok =
             Log.push(first_log, heartbeat, crossing_version, known_committed_version: crossing_version)

    assert :ok =
             Log.push(second_log, heartbeat, crossing_version, known_committed_version: crossing_version)

    assert_receive {:conditional_create_waiting, first_worker, first_ref, chunk_key, proposed_chunk}, 2_000

    assert_receive {:conditional_create_waiting, second_worker, second_ref, ^chunk_key, ^proposed_chunk}, 2_000

    send(first_worker, {:release_conditional_create, first_ref})
    assert_receive {:conditional_create_finished, ^first_ref, :ok}, 2_000

    send(second_worker, {:release_conditional_create, second_ref})
    assert_receive {:conditional_create_finished, ^second_ref, {:error, :already_exists}}, 2_000
  end

  defp start_log(label, wal_root, object_storage) do
    suffix = :erlang.unique_integer([:positive])

    spec =
      [
        cluster: Cluster,
        otp_name: String.to_atom("replicated_demux_#{label}_#{suffix}"),
        id: "replicated-demux-#{label}-#{suffix}",
        foreman: self(),
        path: wal_root,
        object_storage: object_storage,
        start_unlocked: true
      ]
      |> Shale.child_spec()
      |> Supervisor.child_spec(restart: :temporary)

    pid = start_supervised!(spec)

    eventually(fn ->
      state = :sys.get_state(pid)
      assert state.init_state == :initialized
      assert is_pid(state.demux)
      assert is_pid(state.segment_recycler)
    end)

    pid
  end

  defp queue_push(log, transaction, expected_version) do
    task =
      Task.async(fn ->
        Log.push(log, transaction, expected_version, known_committed_version: Version.zero())
      end)

    eventually(fn ->
      assert Map.has_key?(:sys.get_state(log).pending_pushes, expected_version)
    end)

    task
  end

  defp eventually(assertion, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually(assertion, deadline, nil)
  end

  defp eventually(assertion, deadline, last_error) do
    assertion.()
  rescue
    error in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        eventually(assertion, deadline, error)
      else
        reraise(last_error || error, __STACKTRACE__)
      end
  end
end
