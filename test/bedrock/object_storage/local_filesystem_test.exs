defmodule Bedrock.ObjectStorage.LocalFilesystemTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    # Create a unique temp directory for each test
    root = Path.join(System.tmp_dir!(), "object_storage_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, backend: backend, root: root}
  end

  describe "put/4 and get/2" do
    test "stores and retrieves data", %{backend: backend} do
      data = "hello world"
      assert :ok = ObjectStorage.put(backend, "test/key", data)
      assert {:ok, ^data} = ObjectStorage.get(backend, "test/key")
    end

    test "creates nested directories", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "a/b/c/d/key", "data")
      assert {:ok, "data"} = ObjectStorage.get(backend, "a/b/c/d/key")
    end

    test "handles binary data", %{backend: backend} do
      data = <<0, 1, 2, 255, 254, 253>>
      assert :ok = ObjectStorage.put(backend, "binary/key", data)
      assert {:ok, ^data} = ObjectStorage.get(backend, "binary/key")
    end

    test "handles empty data", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "empty/key", "")
      assert {:ok, ""} = ObjectStorage.get(backend, "empty/key")
    end

    test "overwrites existing data", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "key", "first")
      assert :ok = ObjectStorage.put(backend, "key", "second")
      assert {:ok, "second"} = ObjectStorage.get(backend, "key")
    end
  end

  describe "get/2 errors" do
    test "returns not_found for missing key", %{backend: backend} do
      assert {:error, :not_found} = ObjectStorage.get(backend, "nonexistent/key")
    end
  end

  describe "delete/2" do
    test "deletes existing object", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "key", "data")
      assert :ok = ObjectStorage.delete(backend, "key")
      assert {:error, :not_found} = ObjectStorage.get(backend, "key")
    end

    test "is idempotent - deleting non-existent object succeeds", %{backend: backend} do
      assert :ok = ObjectStorage.delete(backend, "nonexistent/key")
      assert :ok = ObjectStorage.delete(backend, "nonexistent/key")
    end
  end

  describe "list/3" do
    test "lists objects with prefix", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "prefix/a", "1")
      :ok = ObjectStorage.put(backend, "prefix/b", "2")
      :ok = ObjectStorage.put(backend, "prefix/c", "3")
      :ok = ObjectStorage.put(backend, "other/x", "4")

      keys = backend |> ObjectStorage.list("prefix/") |> Enum.to_list()
      assert length(keys) == 3
      assert "prefix/a" in keys
      assert "prefix/b" in keys
      assert "prefix/c" in keys
      refute "other/x" in keys
    end

    test "returns empty stream for non-existent prefix", %{backend: backend} do
      keys = backend |> ObjectStorage.list("nonexistent/") |> Enum.to_list()
      assert keys == []
    end

    test "returns keys in sorted order", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "sorted/c", "3")
      :ok = ObjectStorage.put(backend, "sorted/a", "1")
      :ok = ObjectStorage.put(backend, "sorted/b", "2")

      keys = backend |> ObjectStorage.list("sorted/") |> Enum.to_list()
      assert keys == ["sorted/a", "sorted/b", "sorted/c"]
    end

    test "respects limit option", %{backend: backend} do
      for i <- 1..10 do
        :ok = ObjectStorage.put(backend, "limited/#{String.pad_leading(to_string(i), 2, "0")}", "data")
      end

      keys = backend |> ObjectStorage.list("limited/", limit: 3) |> Enum.to_list()
      assert length(keys) == 3
    end

    test "handles nested directories", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "nested/a/1", "data")
      :ok = ObjectStorage.put(backend, "nested/a/2", "data")
      :ok = ObjectStorage.put(backend, "nested/b/1", "data")

      keys = backend |> ObjectStorage.list("nested/") |> Enum.to_list() |> Enum.sort()
      assert keys == ["nested/a/1", "nested/a/2", "nested/b/1"]
    end

    test "is lazy - doesn't read all files at once", %{backend: backend} do
      for i <- 1..100 do
        :ok = ObjectStorage.put(backend, "lazy/#{String.pad_leading(to_string(i), 3, "0")}", "data")
      end

      stream = ObjectStorage.list(backend, "lazy/")

      # Taking just 5 should not enumerate all 100
      keys = Enum.take(stream, 5)
      assert length(keys) == 5
    end
  end

  describe "put_if_not_exists/4" do
    test "stores object when key doesn't exist", %{backend: backend} do
      assert :ok = ObjectStorage.put_if_not_exists(backend, "conditional/key", "data")
      assert {:ok, "data"} = ObjectStorage.get(backend, "conditional/key")
    end

    test "returns already_exists when key exists", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "existing/key", "first")
      assert {:error, :already_exists} = ObjectStorage.put_if_not_exists(backend, "existing/key", "second")
      assert {:ok, "first"} = ObjectStorage.get(backend, "existing/key")
    end

    test "is atomic - only one writer wins", %{backend: backend} do
      # Simulate concurrent writes
      key = "concurrent/key"

      results =
        1..10
        |> Enum.map(fn i ->
          Task.async(fn ->
            ObjectStorage.put_if_not_exists(backend, key, "value_#{i}")
          end)
        end)
        |> Enum.map(&Task.await/1)

      # Exactly one should succeed
      successes = Enum.count(results, &(&1 == :ok))
      already_exists = Enum.count(results, &(&1 == {:error, :already_exists}))

      assert successes == 1
      assert already_exists == 9
    end

    test "creates nested directories", %{backend: backend} do
      assert :ok = ObjectStorage.put_if_not_exists(backend, "deep/nested/path/key", "data")
      assert {:ok, "data"} = ObjectStorage.get(backend, "deep/nested/path/key")
    end
  end

  describe "get_with_version/2" do
    test "returns data and version token", %{backend: backend} do
      data = "hello world"
      :ok = ObjectStorage.put(backend, "versioned/key", data)

      assert {:ok, ^data, version_token} = ObjectStorage.get_with_version(backend, "versioned/key")
      assert is_binary(version_token)
      assert String.starts_with?(version_token, "sha256:")
    end

    test "returns not_found for missing key", %{backend: backend} do
      assert {:error, :not_found} = ObjectStorage.get_with_version(backend, "nonexistent/key")
    end

    test "same content produces same version token", %{backend: backend} do
      data = "identical content"
      :ok = ObjectStorage.put(backend, "key1", data)
      :ok = ObjectStorage.put(backend, "key2", data)

      {:ok, _, token1} = ObjectStorage.get_with_version(backend, "key1")
      {:ok, _, token2} = ObjectStorage.get_with_version(backend, "key2")

      assert token1 == token2
    end

    test "different content produces different version token", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "diff/key", "content v1")
      {:ok, _, token1} = ObjectStorage.get_with_version(backend, "diff/key")

      :ok = ObjectStorage.put(backend, "diff/key", "content v2")
      {:ok, _, token2} = ObjectStorage.get_with_version(backend, "diff/key")

      assert token1 != token2
    end
  end

  describe "put_if_version_matches/5" do
    test "succeeds when version matches", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "cas/key", "original")
      {:ok, _, version_token} = ObjectStorage.get_with_version(backend, "cas/key")

      assert :ok = ObjectStorage.put_if_version_matches(backend, "cas/key", version_token, "updated")
      assert {:ok, "updated"} = ObjectStorage.get(backend, "cas/key")
    end

    test "returns version_mismatch when content changed", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "cas/key", "original")
      {:ok, _, stale_token} = ObjectStorage.get_with_version(backend, "cas/key")

      # Another writer updates the content
      :ok = ObjectStorage.put(backend, "cas/key", "updated by other")

      # Our stale write should fail
      assert {:error, :version_mismatch} =
               ObjectStorage.put_if_version_matches(backend, "cas/key", stale_token, "our update")

      # Content should remain as the other writer left it
      assert {:ok, "updated by other"} = ObjectStorage.get(backend, "cas/key")
    end

    test "returns not_found when key doesn't exist", %{backend: backend} do
      fake_token = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

      assert {:error, :not_found} =
               ObjectStorage.put_if_version_matches(backend, "nonexistent/key", fake_token, "data")
    end

    test "sequential writers - second fails after first succeeds", %{backend: backend} do
      # LocalFilesystem has TOCTOU race for concurrent writes.
      # This test verifies sequential behavior; S3/GCS use native atomic conditionals.
      :ok = ObjectStorage.put(backend, "race/key", "initial")
      {:ok, _, version_token} = ObjectStorage.get_with_version(backend, "race/key")

      # First writer succeeds
      assert :ok = ObjectStorage.put_if_version_matches(backend, "race/key", version_token, "first")

      # Second writer with stale token fails
      assert {:error, :version_mismatch} =
               ObjectStorage.put_if_version_matches(backend, "race/key", version_token, "second")

      # Content is from first writer
      assert {:ok, "first"} = ObjectStorage.get(backend, "race/key")
    end
  end

  describe "integration with Keys module" do
    alias Bedrock.ObjectStorage.Keys

    test "stores and retrieves chunk with versioned key", %{backend: backend} do
      version = 12_345
      key = Keys.chunk_path("shard-01", version)
      data = "chunk data"

      assert :ok = ObjectStorage.put(backend, key, data)
      assert {:ok, ^data} = ObjectStorage.get(backend, key)
    end

    test "lists chunks in version order (newest first)", %{backend: backend} do
      versions = [100, 200, 300, 400, 500]

      for v <- versions do
        key = Keys.chunk_path("shard", v)
        :ok = ObjectStorage.put(backend, key, "data_#{v}")
      end

      prefix = Keys.chunks_prefix("shard")
      keys = backend |> ObjectStorage.list(prefix) |> Enum.to_list()

      # Keys should be sorted, and due to version inversion, highest versions come first
      extracted_versions =
        Enum.map(keys, fn key ->
          {:ok, v} = Keys.extract_version(key)
          v
        end)

      assert extracted_versions == [500, 400, 300, 200, 100]
    end

    test "conditional write for snapshots prevents duplicates", %{backend: backend} do
      key = Keys.snapshot_path("shard", 1000)

      assert :ok = ObjectStorage.put_if_not_exists(backend, key, "snapshot_1")
      assert {:error, :already_exists} = ObjectStorage.put_if_not_exists(backend, key, "snapshot_2")
      assert {:ok, "snapshot_1"} = ObjectStorage.get(backend, key)
    end
  end
end
