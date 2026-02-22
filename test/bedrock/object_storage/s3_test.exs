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
end
