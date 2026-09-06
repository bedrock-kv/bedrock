defmodule Bedrock.ObjectStorage.DiskCache do
  @moduledoc """
  A local disk cache in front of another ObjectStorage backend.

  A node that restarts pays for the same bytes twice. A materializer's
  cold start downloads the shard's baseline again
  (`Snapshot.read_latest/1`, reached from
  `Olivine.Logic.maybe_load_snapshot/2`), and a restarted
  `Demux.ShardServer` re-reads every chunk it serves back out of history
  (`ChunkReader.read_from_version/3`, reached from `get_from_storage/3`).
  Neither object ever changed. This backend keeps a copy in a local
  directory so the second read of one comes off local disk.

  It is a decorator: it holds another backend as `:inner` and forwards
  every operation to it. The inner backend remains the authority for what
  exists — the cache only ever answers a `get/2` that it can satisfy from
  a copy of something the inner backend already gave it.

  ## What is cached, and why nothing needs invalidating

  Only keys of the shape `Keys.chunk_path/2` and `Keys.snapshot_path/2`
  build: `c/{tag}/{version}` and `s/{tag}/{version}`. Both are written
  once under a key naming the version they end at, and are never
  rewritten, so a copy cannot go stale.

  The other keys in the store are not like that. The bootstrap record is
  read with `get/2` (`Coordinator.Server`, recovery's `PersistencePhase`)
  and REWRITTEN in place by `put_if_version_matches/5`; serving a cached
  copy would hand a coordinator a cluster layout another node has already
  replaced. Those keys pass straight through. It is the whole SHAPE that
  is tested rather than the leading `c/` or `s/`, because the bootstrap
  key is free-form application config: a cluster named `c` would
  otherwise put its mutable record inside the cached namespace.

  Immutable is not eternal, though — consolidation reclaims chunks
  (bedrock-wxf.6) and retention drops old snapshots. Every operation
  through THIS backend that removes or replaces an object at a cached key
  drops the entry with it. Removals from another node are invisible here,
  and so is the racing case where a `get/2` already in flight repopulates
  a key a `delete/2` has just dropped. What makes those harmless is that
  nothing can name the object any more: `list/3` is never served from the
  cache, so a key the store no longer lists is never asked for again, and
  the entry is only ever dead weight against the bound.

  ## Bound

  `:max_bytes` caps what the directory holds. A sweep deletes entries
  oldest-mtime-first until the total fits, and emits
  `[:bedrock, :object_storage, :disk_cache, :evicted]` with the count and
  bytes dropped. mtime IS the recency clock: a hit touches its entry, so
  eviction is least-recently-USED rather than least-recently-written.

  The sweep walks the directory, so it costs O(entries) — and sweeping on
  every populate would make the cache slower than no cache at all, since
  a warm 1 GiB of 522-byte chunks is two million `stat` calls to save one
  GET. Instead a populate sweeps with probability `bytes / (slack ×
  max_bytes)`, so one sweep falls due for every `slack × max_bytes` bytes
  admitted however large the objects are. The expected work per populate
  is then `1 / slack` stats no matter how big the cache grows, and the
  cap is soft by that same fraction: the directory drifts above it
  between sweeps, and is pulled back at the next one. Nothing is
  registered anywhere to make this work, which is what lets a backend
  stay a plain `{module, config}` term any process can hold.

  An object larger than the whole cap is never written: it could only
  make room for itself by evicting everything, and would then be evicted
  in turn on the very next populate.

  ## Configuration

  - `:inner` - Required. The backend to wrap, as `{module, config}`.
  - `:root` - Required. Local directory holding the cached copies.
  - `:max_bytes` - Soft cap on the bytes held (default: 1 GiB).

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

  # How far the directory is allowed to drift above the cap between
  # sweeps, as a fraction of it. It is also the reciprocal of the
  # expected `stat` calls a populate pays: 10% slack, ~10 stats.
  @sweep_slack 0.1

  # Exactly what `Keys.chunk_path/2` and `Keys.snapshot_path/2` build:
  # a namespace, a base36 shard tag, and a 13-character base36 inverted
  # version. Nothing else in the store is write-once. (The pattern lives
  # here rather than in `Keys` because it is this module's question —
  # "may I keep a copy of this forever?" — not a key-formatting one.)
  @immutable_key ~r"^[cs]/[0-9a-z]+/[0-9a-z]{13}$"

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
    # In this cluster the only compare-and-swap is on the bootstrap
    # record, which `cacheable?/1` never admits, so the discard below has
    # nothing to do today. It stays because `put/4` and `delete/2` are
    # the other two ways an object at a cached key can change, and both
    # keep the entry honest; leaving the third as the exception would
    # make "an entry never outlives the object it copies" conditional on
    # a convention this module cannot enforce, for the price of one
    # `File.rm` on a path that runs once per cluster reconfiguration.
    result = ObjectStorage.put_if_version_matches(inner(config), key, version_token, data, opts)
    discard(config, key)
    result
  end

  defp inner(config), do: Keyword.fetch!(config, :inner)
  defp root(config), do: Keyword.fetch!(config, :root)
  defp max_bytes(config), do: Keyword.get(config, :max_bytes, @default_max_bytes)

  defp cacheable?(key), do: Regex.match?(@immutable_key, key)

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
    bytes = IO.iodata_length(data)
    max_bytes = max_bytes(config)

    if cacheable?(key) and bytes <= max_bytes do
      # LocalFilesystem writes to a scratch file in the target's own
      # directory, fsyncs it and renames, so an entry is whole or absent.
      # A short entry would be served as if it were the object.
      case LocalFilesystem.put([root: root(config)], key, data) do
        :ok -> maybe_sweep(config, bytes, max_bytes)
        {:error, _reason} -> :ok
      end
    else
      :ok
    end
  end

  # One sweep per `@sweep_slack * max_bytes` bytes admitted, decided by a
  # coin weighted by the size of THIS object so the rate holds whatever
  # the objects weigh. An independent draw per populate, not a hash of
  # the key: a workload that cycles a handful of keys must still
  # accumulate its way to a sweep rather than deterministically never
  # reaching one.
  @spec maybe_sweep(keyword(), pos_integer(), pos_integer()) :: :ok
  defp maybe_sweep(config, bytes, max_bytes) do
    interval = max(1, div(trunc(@sweep_slack * max_bytes), max(1, bytes)))

    if :rand.uniform(interval) == 1 do
      evict_to_fit(config, max_bytes)
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

  @spec evict_to_fit(keyword(), pos_integer()) :: :ok
  defp evict_to_fit(config, max_bytes) do
    root = root(config)
    entries = entries(root)
    total = Enum.reduce(entries, 0, fn {_path, size, _mtime}, sum -> sum + size end)

    if total > max_bytes do
      evict(root, entries, total, max_bytes)
    else
      :ok
    end
  end

  @spec evict(Path.t(), [{Path.t(), non_neg_integer(), integer()}], non_neg_integer(), pos_integer()) :: :ok
  defp evict(root, entries, total, max_bytes) do
    {victims, _condemned} =
      entries
      |> Enum.sort(&least_recently_used?/2)
      |> Enum.reduce_while({[], 0}, fn {path, size, _mtime}, {victims, condemned} ->
        if total - condemned > max_bytes do
          {:cont, {[{path, size} | victims], condemned + size}}
        else
          {:halt, {victims, condemned}}
        end
      end)

    # Only bytes actually removed are freed bytes: a removal that failed
    # left the entry, and the report must not claim room that is still
    # occupied.
    freed =
      Enum.reduce(victims, 0, fn {path, size}, freed -> if File.rm(path) == :ok, do: freed + size, else: freed end)

    Telemetry.execute(
      [:bedrock, :object_storage, :disk_cache, :evicted],
      %{objects: length(victims), bytes: freed, retained_bytes: total - freed},
      %{root: root}
    )
  end

  # mtime is whole seconds, so a replay populates a whole run of entries
  # that tie. The tie-break has to break SOMEWHERE, and reverse key order
  # is the least harmful direction: both cached namespaces are named by
  # inverted version, so the greatest key is the oldest object, and the
  # oldest object is the one a reader is least likely to want next.
  # Without it the order is whatever `Path.wildcard/1` returned —
  # ascending, i.e. newest object first, precisely backwards.
  @spec least_recently_used?({Path.t(), non_neg_integer(), integer()}, {Path.t(), non_neg_integer(), integer()}) ::
          boolean()
  defp least_recently_used?({path_a, _size_a, mtime}, {path_b, _size_b, mtime}), do: path_a > path_b
  defp least_recently_used?({_path_a, _size_a, mtime_a}, {_path_b, _size_b, mtime_b}), do: mtime_a < mtime_b

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
