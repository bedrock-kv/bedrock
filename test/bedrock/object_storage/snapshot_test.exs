defmodule Bedrock.ObjectStorage.SnapshotTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  setup do
    root = Path.join(System.tmp_dir!(), "snapshot_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, backend: backend, root: root}
  end

  describe "new/3" do
    test "creates snapshot handler", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert snapshot.cluster == "cluster"
      assert snapshot.shard_tag == "shard"
    end
  end

  describe "write/3" do
    test "writes snapshot data", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert :ok = Snapshot.write(snapshot, 1000, "snapshot data")
    end

    test "is idempotent - duplicate writes succeed", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      assert :ok = Snapshot.write(snapshot, 1000, "snapshot data")
      assert :ok = Snapshot.write(snapshot, 1000, "snapshot data")
    end

    test "handles binary data", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      data = <<0, 1, 2, 255, 254, 253>>

      assert :ok = Snapshot.write(snapshot, 1000, data)
      assert {:ok, ^data} = Snapshot.read(snapshot, 1000)
    end
  end

  describe "read/2" do
    test "reads written snapshot", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 1000, "my state data")
      assert {:ok, "my state data"} = Snapshot.read(snapshot, 1000)
    end

    test "returns not_found for missing snapshot", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert {:error, :not_found} = Snapshot.read(snapshot, 9999)
    end
  end

  describe "read_latest/1" do
    test "returns latest snapshot", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 100, "state at 100")
      :ok = Snapshot.write(snapshot, 200, "state at 200")
      :ok = Snapshot.write(snapshot, 300, "state at 300")

      assert {:ok, 300, "state at 300"} = Snapshot.read_latest(snapshot)
    end

    test "returns not_found when no snapshots exist", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert {:error, :not_found} = Snapshot.read_latest(snapshot)
    end

    test "works with single snapshot", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 500, "only snapshot")
      assert {:ok, 500, "only snapshot"} = Snapshot.read_latest(snapshot)
    end
  end

  describe "list/2" do
    test "lists snapshots in newest-first order", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 100, "a")
      :ok = Snapshot.write(snapshot, 200, "b")
      :ok = Snapshot.write(snapshot, 300, "c")

      versions = snapshot |> Snapshot.list() |> Enum.map(fn {v, _} -> v end)
      assert versions == [300, 200, 100]
    end

    test "respects limit option", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      for v <- 1..10 do
        :ok = Snapshot.write(snapshot, v * 100, "data")
      end

      versions = snapshot |> Snapshot.list(limit: 3) |> Enum.map(fn {v, _} -> v end)
      assert length(versions) == 3
    end

    test "returns empty for no snapshots", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert [] == snapshot |> Snapshot.list() |> Enum.to_list()
    end
  end

  describe "latest_version/1" do
    test "returns latest version", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 100, "a")
      :ok = Snapshot.write(snapshot, 500, "b")
      :ok = Snapshot.write(snapshot, 300, "c")

      assert {:ok, 500} = Snapshot.latest_version(snapshot)
    end

    test "returns not_found for empty shard", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert {:error, :not_found} = Snapshot.latest_version(snapshot)
    end
  end

  describe "delete/2" do
    test "deletes snapshot", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 100, "data")
      assert {:ok, "data"} = Snapshot.read(snapshot, 100)

      assert :ok = Snapshot.delete(snapshot, 100)
      assert {:error, :not_found} = Snapshot.read(snapshot, 100)
    end

    test "is idempotent - deleting non-existent succeeds", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      assert :ok = Snapshot.delete(snapshot, 9999)
    end
  end

  describe "delete_older_than/2" do
    test "deletes older snapshots", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 100, "a")
      :ok = Snapshot.write(snapshot, 200, "b")
      :ok = Snapshot.write(snapshot, 300, "c")
      :ok = Snapshot.write(snapshot, 400, "d")

      assert {:ok, 2} = Snapshot.delete_older_than(snapshot, 300)

      # 300 and 400 should remain
      versions = snapshot |> Snapshot.list() |> Enum.map(fn {v, _} -> v end)
      assert versions == [400, 300]
    end

    test "returns 0 when nothing to delete", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      :ok = Snapshot.write(snapshot, 500, "data")
      assert {:ok, 0} = Snapshot.delete_older_than(snapshot, 100)
    end
  end

  describe "exists?/1" do
    test "returns false for empty shard", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      refute Snapshot.exists?(snapshot)
    end

    test "returns true after write", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      refute Snapshot.exists?(snapshot)
      :ok = Snapshot.write(snapshot, 100, "data")
      assert Snapshot.exists?(snapshot)
    end
  end

  describe "count/1" do
    test "returns snapshot count", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      assert 0 == Snapshot.count(snapshot)

      :ok = Snapshot.write(snapshot, 100, "a")
      assert 1 == Snapshot.count(snapshot)

      :ok = Snapshot.write(snapshot, 200, "b")
      assert 2 == Snapshot.count(snapshot)
    end
  end

  describe "concurrent writes" do
    test "only one write wins, others get already_exists", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")
      version = 12_345

      results =
        1..10
        |> Enum.map(fn i ->
          Task.async(fn ->
            # Each writer tries to write slightly different data
            # (in real usage, data would be identical since version determines state)
            Snapshot.write(snapshot, version, "data_#{i}")
          end)
        end)
        |> Enum.map(&Task.await/1)

      # All should succeed (either wrote or already_exists, both return :ok)
      assert Enum.all?(results, &(&1 == :ok))

      # Only one snapshot should exist
      assert 1 == Snapshot.count(snapshot)
    end
  end

  describe "isolation between shards" do
    test "snapshots are isolated by shard", %{backend: backend} do
      snapshot1 = Snapshot.new(backend, "cluster", "shard-01")
      snapshot2 = Snapshot.new(backend, "cluster", "shard-02")

      :ok = Snapshot.write(snapshot1, 100, "shard1 data")
      :ok = Snapshot.write(snapshot2, 100, "shard2 data")

      assert {:ok, "shard1 data"} = Snapshot.read(snapshot1, 100)
      assert {:ok, "shard2 data"} = Snapshot.read(snapshot2, 100)
    end
  end

  describe "round-trip workflow" do
    test "materializer cold start scenario", %{backend: backend} do
      snapshot = Snapshot.new(backend, "cluster", "shard")

      # Materializer writes periodic snapshots
      :ok = Snapshot.write(snapshot, 1000, :erlang.term_to_binary(%{key1: "value1"}))
      :ok = Snapshot.write(snapshot, 2000, :erlang.term_to_binary(%{key1: "value1", key2: "value2"}))
      :ok = Snapshot.write(snapshot, 3000, :erlang.term_to_binary(%{key1: "v1", key2: "v2", key3: "v3"}))

      # Materializer cold starts - load latest snapshot
      {:ok, version, data} = Snapshot.read_latest(snapshot)
      state = :erlang.binary_to_term(data)

      assert version == 3000
      assert state == %{key1: "v1", key2: "v2", key3: "v3"}
    end
  end
end
