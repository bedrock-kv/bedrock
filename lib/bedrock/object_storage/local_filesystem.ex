defmodule Bedrock.ObjectStorage.LocalFilesystem do
  @moduledoc """
  Local filesystem implementation of the ObjectStorage behaviour.

  This backend stores objects as files on the local filesystem, useful for
  development and testing. The directory structure mirrors the object keys.

  ## Write atomicity

  Readers are written against S3's contract: an object is complete or it
  is absent, and `put_if_not_exists/4` claims a key only by publishing a
  whole object.

  That matters most for `put_if_not_exists/4`, the writer for chunks,
  snapshots and the bootstrap record. Its callers in the data plane
  (`Demux.ShardServer`, `Snapshot`) read `:already_exists`
  as success, so a key claimed by a short object would report success to
  everyone forever and could never be rewritten. The bootstrap callers
  (`ClusterBootstrap.Discovery`, recovery's `PersistencePhase`) instead
  treat it as a lost race, which is only sound if the winner's object is
  whole.

  Every write therefore goes to a scratch file in the target's own
  directory, is fsynced, and is published in one step — `rename` for
  `put/4`, `link` for `put_if_not_exists/4` (rename would clobber an
  existing object and so cannot express create-only). A failure at any
  point removes the scratch file and leaves the key untouched, so a
  retry can still take it.

  ## Scratch files left by a killed writer

  In-process failures clean up after themselves. A process killed
  between the scratch write and the publish cannot, so its scratch file
  survives — full size, hidden from `list/3`, and reclaimed by nothing.
  That is the deliberate trade: before, the same crash left wreckage
  visible under the real key, where it was permanent and poisonous;
  now it is inert but invisible. Reclaiming it needs a sweep that can
  tell a dead scratch file from a live writer's, which is a separate
  piece of work (bedrock-ck3).

  ## Durability boundary

  Object content is fsynced before publication, but the parent
  DIRECTORY is not: Erlang's `:file.open/2` refuses a directory with
  `:eisdir`, so the directory entry cannot be synced from the BEAM
  without a NIF. On power loss a just-published object may therefore be
  absent rather than short. Absent is a state the writers already handle;
  short under a permanently-claimed key is the one that could not be
  recovered from.

  ## Configuration

  - `:root` - Required. The root directory for storing objects.

  ## Example

      backend = ObjectStorage.backend(ObjectStorage.LocalFilesystem, root: "/tmp/objects")
      :ok = ObjectStorage.put(backend, "test/key", "data")
      {:ok, "data"} = ObjectStorage.get(backend, "test/key")
  """

  @behaviour Bedrock.ObjectStorage

  alias Bedrock.ObjectStorage

  # Scratch files live in the target's own directory, so publishing is a
  # rename or link within one filesystem — the only way it is atomic. The
  # leading dot and the prefix keep them out of `list/3`. No key `Keys`
  # builds can begin with this prefix (they are `c/`, `s/` and a
  # base36 suffix), though the bootstrap key is free-form app-env text,
  # so that remains a convention rather than something enforced here.
  @scratch_prefix ".bedrock-tmp."

  # Exhausting these means the same node, pid and unique-integer collided
  # repeatedly, which is not a real filesystem state — surfacing :eexist
  # beats looping.
  @scratch_attempts 5

  @impl true
  def put(config, key, data, _opts \\ []) do
    root = Keyword.fetch!(config, :root)
    path = build_path(root, key)

    with :ok <- ensure_parent_dir(path),
         {:ok, scratch} <- write_scratch(path, data) do
      # rename consumes the scratch name on success, so there is nothing
      # to clean up there; on failure it is still ours to remove.
      case :file.rename(scratch, path) do
        :ok -> :ok
        {:error, reason} -> discard_scratch(scratch, reason)
      end
    end
  end

  @impl true
  def get(config, key) do
    root = Keyword.fetch!(config, :root)
    path = build_path(root, key)

    case File.read(path) do
      {:ok, data} -> {:ok, data}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def delete(config, key) do
    root = Keyword.fetch!(config, :root)
    path = build_path(root, key)

    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list(config, prefix, opts \\ []) do
    root = Keyword.fetch!(config, :root)
    limit = Keyword.get(opts, :limit)
    prefix_path = build_path(root, prefix)

    Stream.resource(
      fn -> init_list_state(root, prefix_path, prefix, limit) end,
      &list_next/1,
      fn _ -> :ok end
    )
  end

  @impl true
  def put_if_not_exists(config, key, data, _opts \\ []) do
    root = Keyword.fetch!(config, :root)
    path = build_path(root, key)

    # Advisory short-circuit; `link` in claim_key/2 is still what decides.
    # This only avoids writing and fsyncing an entire payload to discover
    # the key was already taken — not a rare path, since re-putting an
    # existing chunk or snapshot is the ordinary idempotent outcome.
    if File.exists?(path) do
      {:error, :already_exists}
    else
      claim_key(path, data)
    end
  end

  # Link, not rename: rename would clobber an existing object, so it
  # cannot express create-only. `link` fails with :eexist exactly when the
  # key is taken, and when it succeeds it publishes an object that is
  # already whole and synced — the same all-or-nothing claim S3 gives with
  # `if_none_match: "*"`.
  @spec claim_key(Path.t(), iodata()) :: :ok | {:error, :already_exists | File.posix()}
  defp claim_key(path, data) do
    with :ok <- ensure_parent_dir(path),
         {:ok, scratch} <- write_scratch(path, data) do
      result =
        case :file.make_link(scratch, path) do
          :ok -> :ok
          {:error, :eexist} -> {:error, :already_exists}
          {:error, reason} -> {:error, reason}
        end

      # link leaves the scratch name in place whether or not it
      # succeeded, so it is always ours to remove.
      _ = File.rm(scratch)
      result
    end
  end

  @impl true
  def get_with_version(config, key) do
    case get(config, key) do
      {:ok, data} ->
        hash = :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
        {:ok, data, "sha256:#{hash}"}

      error ->
        error
    end
  end

  @impl true
  def put_if_version_matches(config, key, version_token, data, opts \\ []) do
    case get(config, key) do
      {:ok, current_data} ->
        current_hash = :sha256 |> :crypto.hash(current_data) |> Base.encode16(case: :lower)
        "sha256:" <> expected_hash = version_token

        if current_hash == expected_hash do
          put(config, key, data, opts)
        else
          {:error, :version_mismatch}
        end

      {:error, :not_found} ->
        {:error, :not_found}

      error ->
        error
    end
  end

  # Private helpers

  defp build_path(root, key) do
    Path.join(root, key)
  end

  defp ensure_parent_dir(path) do
    path
    |> Path.dirname()
    |> File.mkdir_p()
  end

  # Write the whole object to a scratch file and fsync it, so that
  # whatever gets published is complete and on disk BEFORE it is
  # reachable under its key. Any failure cleans up after itself and
  # surfaces as an error: a half-written scratch file must never be left
  # to be mistaken for an object, and the key must stay unclaimed so a
  # retry can still take it.
  @spec write_scratch(Path.t(), iodata()) :: {:ok, Path.t()} | {:error, File.posix()}
  defp write_scratch(path, data) do
    # :exclusive makes the kernel arbitrate the scratch name, which is the
    # only thing that can. A root directory is routinely shared by more
    # than one node — the default one (`ObjectStorage.Config`) is derived
    # from the system tmp dir and carries nothing node-specific — and
    # :erlang.unique_integer/1 is unique only WITHIN a node: two VMs
    # readily produce the same values. Two writers landing on one scratch
    # name would interleave their bytes into a single inode and then
    # publish the splice, which is the exact failure this module exists
    # to prevent. Node and OS pid make a collision unlikely; :exclusive
    # makes it impossible, and a retry costs a filename.
    case open_scratch(path, @scratch_attempts) do
      {:ok, fd, scratch} ->
        result = with :ok <- :file.write(fd, data), do: :file.sync(fd)
        _ = :file.close(fd)
        finish_scratch(result, scratch)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec open_scratch(Path.t(), pos_integer()) :: {:ok, :file.io_device(), Path.t()} | {:error, File.posix()}
  defp open_scratch(_path, 0), do: {:error, :eexist}

  defp open_scratch(path, attempts_left) do
    scratch =
      Path.join(
        Path.dirname(path),
        "#{@scratch_prefix}#{Path.basename(path)}.#{node()}.#{System.pid()}.#{:erlang.unique_integer([:positive])}"
      )

    case :file.open(scratch, [:write, :binary, :raw, :exclusive]) do
      {:ok, fd} -> {:ok, fd, scratch}
      {:error, :eexist} -> open_scratch(path, attempts_left - 1)
      {:error, reason} -> {:error, reason}
    end
  end

  defp finish_scratch(:ok, scratch), do: {:ok, scratch}

  defp finish_scratch({:error, reason}, scratch) do
    _ = File.rm(scratch)
    {:error, reason}
  end

  @spec discard_scratch(Path.t(), term()) :: {:error, term()}
  defp discard_scratch(scratch, reason) do
    _ = File.rm(scratch)
    {:error, reason}
  end

  defp scratch_file?(path), do: path |> Path.basename() |> String.starts_with?(@scratch_prefix)

  # List state: {root, dirs_to_visit, files_collected, prefix, remaining_limit}
  defp init_list_state(root, prefix_path, prefix, limit) do
    if File.dir?(prefix_path) do
      {root, [prefix_path], [], prefix, limit}
    else
      parent = Path.dirname(prefix_path)

      if File.dir?(parent) do
        {root, [parent], [], prefix, limit}
      else
        {root, [], [], prefix, limit}
      end
    end
  end

  defp list_next({_root, [], [], _prefix, _limit}) do
    {:halt, nil}
  end

  defp list_next({_root, _dirs, _files, _prefix, 0}) do
    {:halt, nil}
  end

  defp list_next({root, dirs, [file | rest], prefix, limit}) do
    key = Path.relative_to(file, root)

    if String.starts_with?(key, prefix) do
      new_limit = if limit, do: limit - 1
      {[key], {root, dirs, rest, prefix, new_limit}}
    else
      list_next({root, dirs, rest, prefix, limit})
    end
  end

  defp list_next({root, [dir | rest_dirs], [], prefix, limit}) do
    case File.ls(dir) do
      {:ok, entries} ->
        {files, subdirs} =
          entries
          |> Enum.map(&Path.join(dir, &1))
          |> Enum.split_with(&File.regular?/1)

        # A scratch file is a write in progress or the wreckage of one.
        # It is never an object, and must not be reported as a key.
        sorted_files = files |> Enum.reject(&scratch_file?/1) |> Enum.sort()
        sorted_subdirs = subdirs |> Enum.filter(&may_contain_prefix?(&1, root, prefix)) |> Enum.sort()

        list_next({root, sorted_subdirs ++ rest_dirs, sorted_files, prefix, limit})

      # Absence, not ignorance: a directory that is not there (or is not
      # a directory at all) contributes nothing, and either can happen
      # benignly mid-walk. The module already treats :enotdir as
      # absence — see normalize_reason/1.
      {:error, reason} when reason in [:enoent, :enotdir] ->
        list_next({root, rest_dirs, [], prefix, limit})

      # Anything else (permissions, I/O) means we cannot see what is
      # there, and skipping it would report those keys as absent without
      # ever having looked.
      {:error, reason} ->
        raise ObjectStorage.ListError, reason: reason, prefix: prefix
    end
  end

  # Descend only into subtrees that could hold a matching key: either we
  # are still walking DOWN toward the prefix, or we are already INSIDE
  # it. Without this the walk descends into sibling shards whose keys can
  # never match — which was merely wasteful while listing failures were
  # silent, and is now a false alarm: one unreadable shard directory
  # would abort a healthy shard's listing.
  defp may_contain_prefix?(dir, root, prefix) do
    relative = Path.relative_to(dir, root)
    String.starts_with?(prefix, relative) or String.starts_with?(relative, prefix)
  end
end
