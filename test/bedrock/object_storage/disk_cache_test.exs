defmodule Bedrock.ObjectStorage.DiskCacheTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.TelemetryTestHelper

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.DiskCache
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  @eviction_event [:bedrock, :object_storage, :disk_cache, :evicted]

  # A LocalFilesystem that remembers what was asked of it, so a test can
  # count how many times an object actually left the inner store.
  defmodule RecordingBackend do
    @moduledoc false
    @behaviour ObjectStorage

    @spec keys_for(keyword(), atom()) :: [String.t()]
    def keys_for(config, op) do
      config
      |> Keyword.fetch!(:calls)
      |> Agent.get(&Enum.reverse/1)
      |> Enum.flat_map(fn
        {^op, key} -> [key]
        _other_op -> []
      end)
    end

    @impl true
    def get(config, key) do
      record(config, {:get, key})
      LocalFilesystem.get(config, key)
    end

    @impl true
    def put(config, key, data, opts \\ []) do
      record(config, {:put, key})
      LocalFilesystem.put(config, key, data, opts)
    end

    @impl true
    def delete(config, key) do
      record(config, {:delete, key})
      LocalFilesystem.delete(config, key)
    end

    @impl true
    def list(config, prefix, opts \\ []) do
      record(config, {:list, prefix})
      LocalFilesystem.list(config, prefix, opts)
    end

    @impl true
    def put_if_not_exists(config, key, data, opts \\ []) do
      record(config, {:put_if_not_exists, key})
      LocalFilesystem.put_if_not_exists(config, key, data, opts)
    end

    @impl true
    def get_with_version(config, key) do
      record(config, {:get_with_version, key})
      LocalFilesystem.get_with_version(config, key)
    end

    @impl true
    def put_if_version_matches(config, key, version_token, data, opts \\ []) do
      record(config, {:put_if_version_matches, key})
      LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
    end

    defp record(config, call), do: config |> Keyword.fetch!(:calls) |> Agent.update(&[call | &1])
  end

  setup do
    tmp = Path.join(System.tmp_dir!(), "disk_cache_test_#{:erlang.unique_integer([:positive])}")
    store_root = Path.join(tmp, "store")
    cache_root = Path.join(tmp, "cache")
    File.mkdir_p!(store_root)
    File.mkdir_p!(cache_root)
    on_exit(fn -> File.rm_rf!(tmp) end)

    {:ok, calls} = Agent.start_link(fn -> [] end)

    inner_config = [root: store_root, calls: calls]
    inner = ObjectStorage.backend(RecordingBackend, inner_config)
    cached = ObjectStorage.backend(DiskCache, inner: inner, root: cache_root)

    {:ok, inner: inner, inner_config: inner_config, cached: cached, cache_root: cache_root}
  end

  defp gets(inner_config), do: RecordingBackend.keys_for(inner_config, :get)

  defp entry_path(cache_root, key), do: Path.join(cache_root, key)

  defp write_chunk(backend, shard_tag, transactions) do
    {:ok, binary} = Chunk.encode(transactions)
    {max_version, _data} = List.last(transactions)
    key = Keys.chunk_path(shard_tag, max_version)
    :ok = ObjectStorage.put(backend, key, binary)
    key
  end

  # What a materializer does on a cold start: build a fresh reader and
  # replay the shard's chunks from the beginning.
  defp replay(backend, shard_tag) do
    backend
    |> ChunkReader.new(shard_tag)
    |> ChunkReader.read_from_version(0)
    |> Enum.to_list()
  end

  describe "cold restart on the same node" do
    test "the bare backend re-fetches the same chunk on every cold start", %{
      inner: inner,
      inner_config: inner_config
    } do
      key = write_chunk(inner, "a", [{100, "txn"}])

      assert replay(inner, "a") == [{100, "txn"}]
      assert replay(inner, "a") == [{100, "txn"}]

      assert gets(inner_config) == [key, key]
    end

    test "a disk cache serves the second cold start without reading the chunk again", %{
      inner: inner,
      inner_config: inner_config,
      cached: cached
    } do
      key = write_chunk(inner, "a", [{100, "txn"}])

      assert replay(cached, "a") == [{100, "txn"}]
      assert replay(cached, "a") == [{100, "txn"}]

      assert gets(inner_config) == [key]
    end

    test "a disk cache downloads a shard's snapshot once", %{
      inner: inner,
      inner_config: inner_config,
      cached: cached
    } do
      key = Keys.snapshot_path("a", 700)
      :ok = ObjectStorage.put(inner, key, "bundle-bytes")

      snapshot = Snapshot.new(cached, "a")
      assert {:ok, 700, "bundle-bytes"} = Snapshot.read_latest(snapshot)
      assert {:ok, 700, "bundle-bytes"} = Snapshot.read_latest(snapshot)

      assert gets(inner_config) == [key]
    end
  end

  describe "writes" do
    test "a write populates the cache, so reading it back never touches the inner backend", %{
      inner_config: inner_config,
      cached: cached,
      cache_root: cache_root
    } do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")

      assert File.exists?(entry_path(cache_root, key))
      assert {:ok, "v1"} = ObjectStorage.get(cached, key)
      assert gets(inner_config) == []
    end

    test "a create-only write populates the cache when it wins", %{
      inner_config: inner_config,
      cached: cached
    } do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put_if_not_exists(cached, key, "v1")

      assert {:ok, "v1"} = ObjectStorage.get(cached, key)
      assert gets(inner_config) == []
    end

    test "a rewrite replaces the cached copy", %{cached: cached} do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")
      :ok = ObjectStorage.put(cached, key, "v2")

      assert {:ok, "v2"} = ObjectStorage.get(cached, key)
    end

    test "a failed write leaves nothing cached", %{
      inner_config: inner_config,
      cached: cached,
      cache_root: cache_root
    } do
      key = Keys.chunk_path("a", 100)
      # A directory under the object's key makes the inner write fail.
      File.mkdir_p!(Path.join(Keyword.fetch!(inner_config, :root), key))

      assert {:error, _reason} = ObjectStorage.put(cached, key, "v1")
      refute File.exists?(entry_path(cache_root, key))
    end
  end

  describe "the inner backend is the authority" do
    test "a cached entry cannot make a create-only write report :already_exists", %{
      inner: inner,
      cached: cached,
      cache_root: cache_root
    } do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")
      # Removed behind the cache's back, the way another node's
      # consolidation or retention sweep would remove it.
      :ok = ObjectStorage.delete(inner, key)
      assert File.exists?(entry_path(cache_root, key))

      assert :ok = ObjectStorage.put_if_not_exists(cached, key, "v1")
    end

    test "a create-only write still loses to an object it has never read", %{inner: inner, cached: cached} do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(inner, key, "v1")

      assert {:error, :already_exists} = ObjectStorage.put_if_not_exists(cached, key, "v2")
    end

    test "listing is answered by the inner backend, never by the cache", %{
      inner: inner,
      cached: cached,
      cache_root: cache_root
    } do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")
      :ok = ObjectStorage.delete(inner, key)

      assert File.exists?(entry_path(cache_root, key))
      assert cached |> ObjectStorage.list("c/a/") |> Enum.to_list() == []
    end

    test "an unreadable cache entry falls through instead of failing the read", %{
      inner: inner,
      cached: cached,
      cache_root: cache_root
    } do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(inner, key, "v1")
      File.mkdir_p!(entry_path(cache_root, key))

      assert {:ok, "v1"} = ObjectStorage.get(cached, key)
    end

    test "a missing object is still missing", %{cached: cached} do
      assert {:error, :not_found} = ObjectStorage.get(cached, Keys.chunk_path("a", 100))
    end
  end

  describe "entries never outlive their object" do
    test "delete drops the cached copy", %{inner_config: inner_config, cached: cached, cache_root: cache_root} do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")
      assert {:ok, "v1"} = ObjectStorage.get(cached, key)
      assert gets(inner_config) == []

      :ok = ObjectStorage.delete(cached, key)

      refute File.exists?(entry_path(cache_root, key))
      assert {:error, :not_found} = ObjectStorage.get(cached, key)
      assert gets(inner_config) == [key]
    end

    test "a conditional update drops the cached copy", %{cached: cached} do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(cached, key, "v1")
      {:ok, "v1", token} = ObjectStorage.get_with_version(cached, key)

      :ok = ObjectStorage.put_if_version_matches(cached, key, token, "v2")

      assert {:ok, "v2"} = ObjectStorage.get(cached, key)
    end

    test "keys outside the chunk and snapshot namespaces are never cached", %{
      inner: inner,
      inner_config: inner_config,
      cached: cached,
      cache_root: cache_root
    } do
      :ok = ObjectStorage.put(inner, "bootstrap", "v1")
      assert {:ok, "v1"} = ObjectStorage.get(cached, "bootstrap")

      :ok = ObjectStorage.put(inner, "bootstrap", "v2")
      assert {:ok, "v2"} = ObjectStorage.get(cached, "bootstrap")

      refute File.exists?(entry_path(cache_root, "bootstrap"))
      assert gets(inner_config) == ["bootstrap", "bootstrap"]
    end
  end

  describe "bounded size" do
    test "the cache evicts least-recently-used entries to stay under its cap and reports what it dropped", %{
      inner: inner,
      inner_config: inner_config,
      cache_root: cache_root
    } do
      cached = ObjectStorage.backend(DiskCache, inner: inner, root: cache_root, max_bytes: 200)

      attach_telemetry_reflector(
        self(),
        [@eviction_event],
        "disk-cache-eviction-#{:erlang.unique_integer([:positive])}"
      )

      [oldest, middle, newest] = for version <- [100, 200, 300], do: Keys.chunk_path("a", version)

      for {key, mtime} <- [{oldest, 1_000}, {middle, 2_000}] do
        :ok = ObjectStorage.put(inner, key, String.duplicate("x", 100))
        {:ok, _data} = ObjectStorage.get(cached, key)
        File.touch!(entry_path(cache_root, key), mtime)
      end

      :ok = ObjectStorage.put(inner, newest, String.duplicate("x", 100))
      {:ok, _data} = ObjectStorage.get(cached, newest)

      refute File.exists?(entry_path(cache_root, oldest))
      assert File.exists?(entry_path(cache_root, middle))
      assert File.exists?(entry_path(cache_root, newest))

      {measurements, metadata} = expect_telemetry(@eviction_event)
      assert measurements.objects == 1
      assert measurements.bytes == 100
      assert measurements.retained_bytes == 200
      assert metadata.root == cache_root

      # The evicted object is still an object; it just costs a fetch again.
      assert {:ok, _data} = ObjectStorage.get(cached, oldest)
      assert gets(inner_config) == [oldest, middle, newest, oldest]
    end

    test "a hit refreshes an entry's recency", %{inner: inner, cached: cached, cache_root: cache_root} do
      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(inner, key, "v1")
      {:ok, "v1"} = ObjectStorage.get(cached, key)

      path = entry_path(cache_root, key)
      File.touch!(path, 1_000)
      assert {:ok, "v1"} = ObjectStorage.get(cached, key)

      {:ok, %File.Stat{mtime: mtime}} = File.stat(path, time: :posix)
      assert mtime > 1_000
    end

    test "a writer's scratch file is neither counted nor evicted", %{
      inner: inner,
      cache_root: cache_root
    } do
      cached = ObjectStorage.backend(DiskCache, inner: inner, root: cache_root, max_bytes: 200)
      scratch = Path.join(cache_root, ".bedrock-tmp.in-flight")
      File.write!(scratch, String.duplicate("x", 1_000))
      File.touch!(scratch, 1_000)

      key = Keys.chunk_path("a", 100)
      :ok = ObjectStorage.put(inner, key, String.duplicate("x", 100))
      {:ok, _data} = ObjectStorage.get(cached, key)

      assert File.exists?(scratch)
      assert File.exists?(entry_path(cache_root, key))
    end
  end
end
