defmodule Bedrock.ObjectStorage.S3Test do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.Test.Minio

  @moduletag :s3
  @bucket "bedrock-test"

  setup do
    :ok = Minio.initialize_bucket(@bucket)
    :ok = Minio.clean_bucket(@bucket)

    backend =
      ObjectStorage.backend(S3,
        bucket: @bucket,
        config: Minio.config()
      )

    {:ok, backend: backend}
  end

  describe "put/4 and get/2" do
    test "stores and retrieves object data", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "objects/a.txt", "hello")
      assert {:ok, "hello"} = ObjectStorage.get(backend, "objects/a.txt")
    end

    test "returns not_found for missing key", %{backend: backend} do
      assert {:error, :not_found} = ObjectStorage.get(backend, "objects/missing.txt")
    end
  end

  describe "delete/2" do
    test "deletes existing object", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "objects/delete-me.txt", "value")
      assert :ok = ObjectStorage.delete(backend, "objects/delete-me.txt")
      assert {:error, :not_found} = ObjectStorage.get(backend, "objects/delete-me.txt")
    end

    test "is idempotent for missing keys", %{backend: backend} do
      assert :ok = ObjectStorage.delete(backend, "objects/never-there.txt")
      assert :ok = ObjectStorage.delete(backend, "objects/never-there.txt")
    end
  end

  describe "list/3" do
    test "lists keys by prefix", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "logs/a", "1")
      :ok = ObjectStorage.put(backend, "logs/b", "2")
      :ok = ObjectStorage.put(backend, "other/c", "3")

      keys = backend |> ObjectStorage.list("logs/") |> Enum.to_list() |> Enum.sort()

      assert keys == ["logs/a", "logs/b"]
    end

    test "respects limit option", %{backend: backend} do
      :ok = ObjectStorage.put(backend, "limited/1", "1")
      :ok = ObjectStorage.put(backend, "limited/2", "2")
      :ok = ObjectStorage.put(backend, "limited/3", "3")

      keys = backend |> ObjectStorage.list("limited/", limit: 2) |> Enum.to_list()
      assert length(keys) == 2
    end
  end

  describe "put_if_not_exists/4" do
    test "stores object when key is missing", %{backend: backend} do
      assert :ok = ObjectStorage.put_if_not_exists(backend, "conditional/new.txt", "v1")
      assert {:ok, "v1"} = ObjectStorage.get(backend, "conditional/new.txt")
    end

    test "returns already_exists when key exists", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "conditional/existing.txt", "first")

      assert {:error, :already_exists} =
               ObjectStorage.put_if_not_exists(backend, "conditional/existing.txt", "second")

      assert {:ok, "first"} = ObjectStorage.get(backend, "conditional/existing.txt")
    end

    test "is atomic under concurrent writers", %{backend: backend} do
      key = "conditional/concurrent.txt"

      results =
        1..8
        |> Enum.map(fn i ->
          Task.async(fn ->
            ObjectStorage.put_if_not_exists(backend, key, "value-#{i}")
          end)
        end)
        |> Enum.map(&Task.await/1)

      assert Enum.count(results, &(&1 == :ok)) == 1
      assert Enum.count(results, &(&1 == {:error, :already_exists})) == 7
    end
  end

  describe "get_with_version/2 and put_if_version_matches/5" do
    test "returns data with opaque version token", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "versioned/key.txt", "hello")
      assert {:ok, "hello", version_token} = ObjectStorage.get_with_version(backend, "versioned/key.txt")
      assert is_binary(version_token)
      refute version_token == ""
    end

    test "supports optimistic CAS update", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "cas/key.txt", "original")
      assert {:ok, "original", version_token} = ObjectStorage.get_with_version(backend, "cas/key.txt")

      assert :ok = ObjectStorage.put_if_version_matches(backend, "cas/key.txt", version_token, "updated")
      assert {:ok, "updated"} = ObjectStorage.get(backend, "cas/key.txt")
    end

    test "returns version_mismatch with stale token", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "cas/stale.txt", "v1")
      assert {:ok, "v1", stale_token} = ObjectStorage.get_with_version(backend, "cas/stale.txt")
      assert :ok = ObjectStorage.put(backend, "cas/stale.txt", "v2")

      assert {:error, :version_mismatch} =
               ObjectStorage.put_if_version_matches(backend, "cas/stale.txt", stale_token, "our-write")

      assert {:ok, "v2"} = ObjectStorage.get(backend, "cas/stale.txt")
    end

    test "returns not_found when CAS target does not exist", %{backend: backend} do
      assert {:error, :not_found} =
               ObjectStorage.put_if_version_matches(backend, "cas/missing.txt", "\"etag-token\"", "value")
    end

    test "only one writer wins with same version token", %{backend: backend} do
      assert :ok = ObjectStorage.put(backend, "cas/race.txt", "initial")
      assert {:ok, "initial", token} = ObjectStorage.get_with_version(backend, "cas/race.txt")

      results =
        1..8
        |> Enum.map(fn i ->
          Task.async(fn ->
            ObjectStorage.put_if_version_matches(backend, "cas/race.txt", token, "writer-#{i}")
          end)
        end)
        |> Enum.map(&Task.await/1)

      assert Enum.count(results, &(&1 == :ok)) == 1
      assert Enum.count(results, &(&1 == {:error, :version_mismatch})) == 7
    end
  end
end
