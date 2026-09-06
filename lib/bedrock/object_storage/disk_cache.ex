defmodule Bedrock.ObjectStorage.DiskCache do
  @moduledoc """
  A local disk cache in front of another ObjectStorage backend.

  A materializer that restarts on a node it has already run on pays for
  the same bytes twice: `Snapshot.read_latest/1` downloads the shard's
  baseline again, and `ChunkReader.read_from_version/3` re-fetches every
  chunk it replays. Neither object ever changed. This backend keeps a
  copy in a local directory so the second cold start reads it from disk.

  It is a decorator: it holds another backend as `:inner` and forwards
  every operation to it. The inner backend remains the authority for what
  exists — the cache only ever answers a `get/2` that it can satisfy from
  a copy of something the inner backend already gave it.

  ## What is cached, and why nothing needs invalidating

  Only chunks (`c/`) and snapshots (`s/`). Both are written once under a
  key naming the version they end at, and are never rewritten, so a copy
  cannot go stale.

  The other keys in the store are not like that. The bootstrap record is
  read with `get/2` (`Coordinator.Server`, recovery's `PersistencePhase`)
  and REWRITTEN in place by `put_if_version_matches/5`; serving a cached
  copy would hand a coordinator a cluster layout another node has already
  replaced. Those keys pass straight through.

  Immutable is not eternal, though — consolidation reclaims chunks
  (bedrock-wxf.6) and retention drops old snapshots. Every operation that
  removes or replaces an object at a cached key drops the entry with it,
  so an entry never outlives the object it copies. A removal performed
  from ANOTHER node is invisible here, but so is the key: `list/3` is
  never served from the cache, so nothing names an object the store no
  longer lists, and the entry ages out.

  ## Bound

  `:max_bytes` caps what the directory holds. A populate that carries the
  total over the cap deletes entries oldest-mtime-first until it fits, and
  emits `[:bedrock, :object_storage, :disk_cache, :evicted]` with the
  count and bytes dropped. mtime IS the recency clock: a hit touches its
  entry, so eviction is least-recently-USED rather than
  least-recently-written.

  The sweep walks the directory, which is O(entries). It runs only on a
  populate — the path that just paid for a fetch from the inner backend,
  which dwarfs a directory walk — and staying stateless is what lets a
  backend remain a plain `{module, config}` term that any process can
  hold without registering anything.

  ## Configuration

  - `:inner` - Required. The backend to wrap, as `{module, config}`.
  - `:root` - Required. Local directory holding the cached copies.
  - `:max_bytes` - Cap on the bytes held (default: 1 GiB).

  Not wired in by default: `ObjectStorage.Config` reaches this backend
  only when it is asked for.

      config :bedrock, Bedrock.ObjectStorage,
        backend: {:disk_cache, root: "/var/cache/bedrock", inner: :s3},
        s3: [bucket: "bedrock"]
  """

  @behaviour Bedrock.ObjectStorage

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Telemetry

  @default_max_bytes 1024 * 1024 * 1024

  # `Keys` builds every chunk key under `c/` and every snapshot key under
  # `s/`. Nothing else in the store is write-once.
  @immutable_prefixes ["c/", "s/"]

  @impl true
  def get(config, key) do
    if cacheable?(key) do
      read_through(config, key)
    else
      ObjectStorage.get(inner(config), key)
    end
  end

  @impl true
  def put(config, key, data, opts \\ []) do
    with :ok <- ObjectStorage.put(inner(config), key, data, opts) do
      populate(config, key, data)
    end
  end

  @impl true
  def put_if_not_exists(config, key, data, opts \\ []) do
    # The inner backend decides. A cache entry proves only that this node
    # once read the object; answering `:already_exists` from it would let
    # a local file veto a write the store would have accepted, and the
    # callers that read `:already_exists` as success
    # (`Demux.ShardServer`, `Snapshot.write/3`) would never learn the
    # object is not there.
    with :ok <- ObjectStorage.put_if_not_exists(inner(config), key, data, opts) do
      populate(config, key, data)
    end
  end

  @impl true
  def delete(config, key) do
    result = ObjectStorage.delete(inner(config), key)
    discard(config, key)
    result
  end

  @impl true
  def list(config, prefix, opts \\ []) do
    # Never cached. A listing is the answer to "what exists", which is the
    # one question a copy of past bytes cannot speak to.
    ObjectStorage.list(inner(config), prefix, opts)
  end

  @impl true
  def get_with_version(config, key) do
    # Passed through: the version token is the inner backend's (an ETag,
    # a content hash), and it must describe the object in the store
    # rather than the copy on this disk.
    ObjectStorage.get_with_version(inner(config), key)
  end

  @impl true
  def put_if_version_matches(config, key, version_token, data, opts \\ []) do
    result = ObjectStorage.put_if_version_matches(inner(config), key, version_token, data, opts)
    discard(config, key)
    result
  end

  defp inner(config), do: Keyword.fetch!(config, :inner)
  defp root(config), do: Keyword.fetch!(config, :root)
  defp max_bytes(config), do: Keyword.get(config, :max_bytes, @default_max_bytes)

  defp cacheable?(key), do: String.starts_with?(key, @immutable_prefixes)

  defp entry_path(config, key), do: Path.join(root(config), key)

  @spec read_through(keyword(), ObjectStorage.key()) :: {:ok, ObjectStorage.data()} | ObjectStorage.error()
  defp read_through(config, key) do
    case read_entry(config, key) do
      {:ok, data} ->
        {:ok, data}

      :miss ->
        with {:ok, data} <- ObjectStorage.get(inner(config), key) do
          populate(config, key, data)
          {:ok, data}
        end
    end
  end

  # A cache read that fails for ANY reason is a miss, never an error: the
  # inner backend is still there to answer. An entry can be swept away by
  # another process between one call and the next, and a local disk fault
  # must not turn an object that IS in the store into `:not_found`.
  @spec read_entry(keyword(), ObjectStorage.key()) :: {:ok, binary()} | :miss
  defp read_entry(config, key) do
    path = entry_path(config, key)

    case File.read(path) do
      {:ok, data} ->
        _ = File.touch(path)
        {:ok, data}

      {:error, _reason} ->
        :miss
    end
  end

  # Populating is best effort. A cache that cannot write is a cache that
  # misses; it is never a reason to fail an operation the store completed.
  @spec populate(keyword(), ObjectStorage.key(), ObjectStorage.data()) :: :ok
  defp populate(config, key, data) do
    if cacheable?(key) do
      # LocalFilesystem writes to a scratch file in the target's own
      # directory, fsyncs it and renames, so an entry is whole or absent.
      # A short entry would be served as if it were the object.
      case LocalFilesystem.put([root: root(config)], key, data) do
        :ok -> evict_to_fit(config)
        {:error, _reason} -> :ok
      end
    else
      :ok
    end
  end

  @spec discard(keyword(), ObjectStorage.key()) :: :ok
  defp discard(config, key) do
    if cacheable?(key) do
      _ = File.rm(entry_path(config, key))
    end

    :ok
  end

  @spec evict_to_fit(keyword()) :: :ok
  defp evict_to_fit(config) do
    root = root(config)
    max_bytes = max_bytes(config)
    entries = entries(root)
    total = Enum.reduce(entries, 0, fn {_path, size, _mtime}, sum -> sum + size end)

    if total > max_bytes do
      evict(root, entries, total, max_bytes)
    else
      :ok
    end
  end

  @spec evict(Path.t(), [{Path.t(), non_neg_integer(), integer()}], non_neg_integer(), non_neg_integer()) :: :ok
  defp evict(root, entries, total, max_bytes) do
    {victims, freed} =
      entries
      |> Enum.sort_by(fn {_path, _size, mtime} -> mtime end)
      |> Enum.reduce_while({[], 0}, fn {path, size, _mtime}, {victims, freed} ->
        if total - freed > max_bytes do
          {:cont, {[path | victims], freed + size}}
        else
          {:halt, {victims, freed}}
        end
      end)

    Enum.each(victims, &File.rm/1)

    Telemetry.execute(
      [:bedrock, :object_storage, :disk_cache, :evicted],
      %{objects: length(victims), bytes: freed, retained_bytes: total - freed},
      %{root: root}
    )
  end

  # `Path.wildcard/1` skips dot-prefixed names, which is exactly right
  # here: a LocalFilesystem scratch file is a write in progress, not an
  # entry, and evicting one would corrupt the writer that owns it.
  @spec entries(Path.t()) :: [{Path.t(), non_neg_integer(), integer()}]
  defp entries(root) do
    root
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.flat_map(fn path ->
      case File.stat(path, time: :posix) do
        {:ok, %File.Stat{type: :regular, size: size, mtime: mtime}} -> [{path, size, mtime}]
        # Directories, and entries a concurrent sweep already removed.
        _not_an_entry -> []
      end
    end)
  end
end
