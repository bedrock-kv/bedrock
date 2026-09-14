defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotLoadingTest do
  @moduledoc """
  Tests for snapshot loading during Materializer cold start.

  Scenarios covered:
  1. Cold start, snapshot exists - discovers, downloads, restores from ObjectStorage
  2. Cold start, no snapshot - starts fresh with empty state
  3. Warm start - uses local files, skips discovery
  4. Download failure - returns error, startup fails
  5. Split failure - returns error, startup fails
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  setup do
    test_id = :erlang.unique_integer([:positive])
    test_dir = Path.join(System.tmp_dir!(), "olivine_snapshot_loading_test_#{test_id}")
    object_storage_root = Path.join(System.tmp_dir!(), "object_storage_#{test_id}")

    File.rm_rf!(test_dir)
    File.rm_rf!(object_storage_root)
    File.mkdir_p!(test_dir)
    File.mkdir_p!(object_storage_root)

    # The backend a foreman hands its workers
    backend = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)

    on_exit(fn ->
      # Cleanup
      Enum.reduce_while(1..5, :error, fn attempt, _acc ->
        case File.rm_rf(test_dir) do
          {:ok, _} -> {:halt, :ok}
          _error when attempt < 5 -> {:cont, :error}
          _error -> {:halt, :error}
        end
      end)

      File.rm_rf(object_storage_root)
    end)

    {:ok, test_dir: test_dir, backend: backend, shard_id: "test-shard-#{test_id}"}
  end

  describe "maybe_load_snapshot/2" do
    test "returns :ok when snapshot handle is nil", %{test_dir: test_dir} do
      assert :ok = Logic.maybe_load_snapshot(test_dir, nil)
    end

    test "returns :ok when local files exist (warm start)", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      # Create local files (Database.open uses path/data and path/idx)
      File.write!(Path.join(test_dir, "data"), "existing data")
      File.write!(Path.join(test_dir, "idx"), "existing idx")

      snapshot = Snapshot.new(backend, shard_id)

      {result, logs} =
        with_log(fn ->
          Logic.maybe_load_snapshot(test_dir, snapshot)
        end)

      assert result == :ok
      # Should not log about discovering snapshots
      refute logs =~ "Discovered"
    end

    test "loads snapshot from ObjectStorage on cold start", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      snapshot = Snapshot.new(backend, shard_id)

      # Create a valid snapshot in ObjectStorage
      data_content = "snapshot data content"
      version = <<0, 0, 0, 0, 0, 0, 0, 42>>
      pages_map = %{0 => {Page.new(0, []), 0}}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_content = data_content <> index_binary
      :ok = Snapshot.write(snapshot, 42, bundle_content)

      {result, logs} =
        with_log(fn ->
          Logic.maybe_load_snapshot(test_dir, snapshot)
        end)

      assert result == :ok
      assert logs =~ "Discovered and loaded snapshot from ObjectStorage"

      # Verify files were created
      data_path = Path.join(test_dir, "data")
      idx_path = Path.join(test_dir, "idx")

      assert File.exists?(data_path)
      assert File.exists?(idx_path)
      assert File.read!(data_path) == data_content
      assert File.read!(idx_path) == index_binary
    end

    test "starts fresh when no snapshot exists in ObjectStorage", %{
      test_dir: test_dir,
      backend: backend,
      shard_id: shard_id
    } do
      snapshot = Snapshot.new(backend, shard_id)

      {result, logs} =
        with_log(fn ->
          Logic.maybe_load_snapshot(test_dir, snapshot)
        end)

      assert result == :ok
      assert logs =~ "No snapshot discovered in ObjectStorage, starting fresh"

      # Verify files were NOT created
      data_path = Path.join(test_dir, "data")
      idx_path = Path.join(test_dir, "idx")

      refute File.exists?(data_path)
      refute File.exists?(idx_path)
    end

    test "returns error when snapshot bundle is invalid", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      snapshot = Snapshot.new(backend, shard_id)

      # Write invalid bundle (no valid index record)
      :ok = Snapshot.write(snapshot, 100, "invalid bundle content without index")

      result = Logic.maybe_load_snapshot(test_dir, snapshot)

      assert {:error, {:snapshot_load_failed, _reason}} = result
    end
  end

  describe "startup/5 with snapshot loading" do
    test "cold start with snapshot loads from ObjectStorage", %{
      test_dir: test_dir,
      backend: backend,
      shard_id: shard_id
    } do
      snapshot = Snapshot.new(backend, shard_id)

      # Create a valid snapshot with proper index structure
      data_content = ""
      version = <<0, 0, 0, 0, 0, 0, 0, 100>>
      pages_map = %{0 => {Page.new(0, []), 0}}

      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      index_binary = IO.iodata_to_binary(index_record)

      bundle_content = data_content <> index_binary
      :ok = Snapshot.write(snapshot, 100, bundle_content)

      {result, logs} =
        with_log(fn ->
          Logic.startup(
            :test_snapshot_loading,
            self(),
            "test-id",
            test_dir,
            object_storage: backend,
            shard_id: shard_id,
            pool_size: 1
          )
        end)

      assert {:ok, %State{shard_id: ^shard_id, snapshot: %Snapshot{}} = state} = result
      assert logs =~ "Discovered and loaded snapshot from ObjectStorage"

      Logic.shutdown(state)
    end

    test "cold start without snapshot starts fresh", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      {result, logs} =
        with_log(fn ->
          Logic.startup(
            :test_fresh_start,
            self(),
            "test-id",
            test_dir,
            object_storage: backend,
            shard_id: shard_id,
            pool_size: 1
          )
        end)

      assert {:ok, %State{shard_id: ^shard_id} = state} = result
      assert logs =~ "No snapshot discovered in ObjectStorage, starting fresh"

      Logic.shutdown(state)
    end

    test "warm start uses local files", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      snapshot = Snapshot.new(backend, shard_id)

      # Write a snapshot to ObjectStorage (should not be used)
      version = <<0, 0, 0, 0, 0, 0, 0, 999>>
      pages_map = %{0 => {Page.new(0, []), 0}}
      index_record = IndexDatabase.build_snapshot_record(version, pages_map)
      :ok = Snapshot.write(snapshot, 999, IO.iodata_to_binary(index_record))

      # First, start fresh to create local files
      {fresh_result, _logs} =
        with_log(fn ->
          Logic.startup(
            :test_warm_first,
            self(),
            "test-id",
            test_dir,
            object_storage: backend,
            shard_id: shard_id,
            pool_size: 1
          )
        end)

      {:ok, state1} = fresh_result
      Logic.shutdown(state1)

      # Now restart - should use local files, not ObjectStorage snapshot
      {result, logs} =
        with_log(fn ->
          Logic.startup(
            :test_warm_second,
            self(),
            "test-id",
            test_dir,
            object_storage: backend,
            shard_id: shard_id,
            pool_size: 1
          )
        end)

      assert {:ok, %State{} = state2} = result
      # Should not see snapshot discovery logs
      refute logs =~ "Discovered and loaded snapshot"
      refute logs =~ "No snapshot discovered"

      Logic.shutdown(state2)
    end

    test "startup without object_storage/shard_id works (no snapshot loading)", %{test_dir: test_dir} do
      {result, logs} =
        with_log(fn ->
          Logic.startup(
            :test_no_snapshot,
            self(),
            "test-id",
            test_dir,
            pool_size: 1
          )
        end)

      assert {:ok, %State{shard_id: nil, snapshot: nil} = state} = result
      # Should not attempt any snapshot loading
      refute logs =~ "snapshot"

      Logic.shutdown(state)
    end
  end

  describe "startup/5 error handling" do
    test "returns error when snapshot bundle is corrupted", %{test_dir: test_dir, backend: backend, shard_id: shard_id} do
      snapshot = Snapshot.new(backend, shard_id)

      # Write corrupted bundle
      :ok = Snapshot.write(snapshot, 50, "corrupted data without valid index")

      result =
        Logic.startup(
          :test_corrupted,
          self(),
          "test-id",
          test_dir,
          object_storage: backend,
          shard_id: shard_id,
          pool_size: 1
        )

      assert {:error, {:snapshot_load_failed, _reason}} = result
    end
  end

  describe "build_snapshot_handle/2" do
    test "returns nil when object_storage is nil" do
      # Access the private function through the public API behavior
      # When object_storage is nil, startup should work but snapshot should be nil

      test_dir = Path.join(System.tmp_dir!(), "build_handle_test_#{:rand.uniform(100_000)}")
      File.mkdir_p!(test_dir)

      on_exit(fn -> File.rm_rf(test_dir) end)

      {result, _logs} =
        with_log(fn ->
          Logic.startup(:test_nil_cluster, self(), "id", test_dir, shard_id: "shard", pool_size: 1)
        end)

      {:ok, state} = result
      assert state.snapshot == nil

      Logic.shutdown(state)
    end

    test "returns nil when shard_id is nil", %{backend: backend} do
      test_dir = Path.join(System.tmp_dir!(), "build_handle_test2_#{:rand.uniform(100_000)}")
      File.mkdir_p!(test_dir)

      on_exit(fn -> File.rm_rf(test_dir) end)

      {result, _logs} =
        with_log(fn ->
          Logic.startup(:test_nil_shard, self(), "id", test_dir, object_storage: backend, pool_size: 1)
        end)

      {:ok, state} = result
      assert state.snapshot == nil

      Logic.shutdown(state)
    end
  end
end
