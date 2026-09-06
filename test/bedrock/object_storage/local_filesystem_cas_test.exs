defmodule Bedrock.ObjectStorage.LocalFilesystemCASTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    root = Path.join(System.tmp_dir!(), "bedrock-cas-#{System.pid()}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, backend: ObjectStorage.backend(LocalFilesystem, root: root), root: root}
  end

  test "mutation APIs protect permanent locking metadata", %{backend: backend, root: root} do
    for key <- [".bedrock-lock", "nested/.BEDROCK-LOCK", "../outside", "a/../.bedrock-lock"] do
      assert {:error, :invalid_key} = ObjectStorage.put(backend, key, "bad")
      assert {:error, :invalid_key} = ObjectStorage.put_if_not_exists(backend, key, "bad")
      assert {:error, :invalid_key} = ObjectStorage.put_if_version_matches(backend, key, "sha256:0", "bad")
      assert {:error, :invalid_key} = ObjectStorage.delete(backend, key)
    end

    refute File.exists?(Path.join(root, ".bedrock-lock"))
  end

  test "concurrent distinct replacements have one winner matching the stored value", %{backend: backend} do
    assert :ok = ObjectStorage.put(backend, "key", "original")
    assert {:ok, "original", token} = ObjectStorage.get_with_version(backend, "key")
    parent = self()

    writers =
      for id <- 1..8 do
        Task.async(fn ->
          data = :binary.copy(<<id>>, 4 * 1024 * 1024)
          send(parent, {:ready, self()})
          receive do: (:go -> :ok)
          {data, ObjectStorage.put_if_version_matches(backend, "key", token, data)}
        end)
      end

    for _ <- writers, do: assert_receive({:ready, _}, 5_000)
    for writer <- writers, do: send(writer.pid, :go)
    results = Enum.map(writers, &Task.await(&1, 15_000))
    winners = for {data, :ok} <- results, do: data
    assert length(winners) == 1, "one token produced #{length(winners)} successful replacements"
    assert {:ok, hd(winners)} == ObjectStorage.get(backend, "key")
    assert Enum.count(results, &(elem(&1, 1) == {:error, :version_mismatch})) == 7
    assert {:error, :version_mismatch} = ObjectStorage.put_if_version_matches(backend, "key", token, "stale")
    assert {:ok, _, fresh_token} = ObjectStorage.get_with_version(backend, "key")
    assert :ok = ObjectStorage.put_if_version_matches(backend, "key", fresh_token, "retry")
    assert {:ok, "retry"} = ObjectStorage.get(backend, "key")
  end

  test "failed scratch, native publication and token checks preserve value and permit retry", %{
    backend: backend,
    root: root
  } do
    assert :ok = ObjectStorage.put(backend, "key", "original")
    {:ok, _, token} = ObjectStorage.get_with_version(backend, "key")
    assert {:error, _} = ObjectStorage.put_if_version_matches(backend, "key", token, [self()])
    assert {:error, :version_mismatch} = ObjectStorage.put_if_version_matches(backend, "key", "malformed", "bad")
    assert {:ok, "original"} = ObjectStorage.get(backend, "key")
    assert Enum.sort(File.ls!(root)) == [".bedrock-lock", "key"]
    assert :ok = ObjectStorage.put_if_version_matches(backend, "key", token, ["re", "try"])
    assert {:ok, "retry"} = ObjectStorage.get(backend, "key")
    assert {:ok, _, retry_token} = ObjectStorage.get_with_version(backend, "key")
    assert :ok = ObjectStorage.put_if_version_matches(backend, "key", retry_token, "retry")
    assert {:ok, "retry", ^retry_token} = ObjectStorage.get_with_version(backend, "key")
  end

  test "object symlinks and lock aliases cannot replace metadata", %{backend: backend, root: root} do
    assert :ok = ObjectStorage.put(backend, "key", "original")
    lock = Path.join(root, ".bedrock-lock")
    {:ok, before} = File.stat(lock)
    File.ln_s!(lock, Path.join(root, "alias"))
    assert {:error, :eloop} = ObjectStorage.put(backend, "alias", "bad")
    assert {:error, :eloop} = ObjectStorage.delete(backend, "alias")
    File.ln!(lock, Path.join(root, "hard-alias"))
    assert {:error, :einval} = ObjectStorage.put(backend, "hard-alias", "bad")
    assert {:error, :einval} = ObjectStorage.delete(backend, "hard-alias")
    {:ok, after_stat} = File.stat(lock)
    assert before.inode == after_stat.inode
    assert {:error, :not_found} = ObjectStorage.get(backend, ".bedrock-lock")
    assert {:error, :not_found} = ObjectStorage.get_with_version(backend, ".bedrock-lock")
    assert :ok = ObjectStorage.put(backend, "nested/key", "value")
    assert File.regular?(Path.join(root, "nested/.bedrock-lock"))
    assert {:error, :not_found} = ObjectStorage.get(backend, "nested/.BEDROCK-LOCK")
    assert {:error, :not_found} = ObjectStorage.get_with_version(backend, "nested/.bedrock-lock")
  end
end
