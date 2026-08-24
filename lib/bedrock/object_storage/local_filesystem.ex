defmodule Bedrock.ObjectStorage.LocalFilesystem do
  @moduledoc """
  Local filesystem implementation of the ObjectStorage behaviour.

  This backend stores objects as files on the local filesystem, useful for
  development and testing. The directory structure mirrors the object keys.

  ## Configuration

  - `:root` - Required. The root directory for storing objects.

  ## Example

      backend = ObjectStorage.backend(ObjectStorage.LocalFilesystem, root: "/tmp/objects")
      :ok = ObjectStorage.put(backend, "test/key", "data")
      {:ok, "data"} = ObjectStorage.get(backend, "test/key")
  """

  @behaviour Bedrock.ObjectStorage

  alias Bedrock.ObjectStorage

  @impl true
  def put(config, key, data, _opts \\ []) do
    root = Keyword.fetch!(config, :root)
    path = build_path(root, key)

    with :ok <- ensure_parent_dir(path) do
      File.write(path, data)
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

    with :ok <- ensure_parent_dir(path) do
      case :file.open(path, [:write, :binary, :exclusive]) do
        {:ok, fd} ->
          try do
            :ok = :file.write(fd, data)
            :ok
          after
            :file.close(fd)
          end

        {:error, :eexist} ->
          {:error, :already_exists}

        {:error, reason} ->
          {:error, reason}
      end
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

        sorted_files = Enum.sort(files)
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
