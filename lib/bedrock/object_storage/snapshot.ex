defmodule Bedrock.ObjectStorage.Snapshot do
  @moduledoc """
  Snapshot storage for materialized shard state.

  Snapshots capture the complete state of a shard at a specific version,
  allowing materializers to cold start from a known point rather than
  replaying all transactions.

  ## Path Structure

  Snapshots are stored at: `/{cluster}/shards/{tag}/snapshots/{inverted_version}`

  Using inverted versions ensures listing returns newest snapshots first,
  making it efficient to find the latest snapshot.

  ## Conditional Writes

  Snapshots use conditional writes (put_if_not_exists) to prevent duplicate
  writes. Since snapshot data is deterministic (same version = same state),
  concurrent attempts to write the same snapshot are idempotent - the first
  write wins, subsequent attempts see "already exists" and can safely skip.

  ## Usage

      snapshot = Snapshot.new(backend, "cluster", "shard-01")

      # Write a snapshot
      :ok = Snapshot.write(snapshot, version, state_binary)

      # Read latest snapshot
      case Snapshot.read_latest(snapshot) do
        {:ok, version, data} -> load_state(version, data)
        {:error, :not_found} -> start_from_scratch()
      end

      # Read specific snapshot
      {:ok, data} = Snapshot.read(snapshot, version)
  """

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys

  @type version :: non_neg_integer()
  @type snapshot_data :: binary()

  @type t :: %__MODULE__{
          backend: ObjectStorage.backend(),
          cluster: String.t(),
          shard_tag: String.t()
        }

  defstruct [:backend, :cluster, :shard_tag]

  @doc """
  Creates a new snapshot handler for a shard.
  """
  @spec new(ObjectStorage.backend(), String.t(), String.t()) :: t()
  def new(backend, cluster, shard_tag) do
    %__MODULE__{
      backend: backend,
      cluster: cluster,
      shard_tag: shard_tag
    }
  end

  @doc """
  Writes a snapshot using conditional put.

  If a snapshot already exists for this version, returns `:ok` (idempotent).
  The data must be the complete serialized state at the given version.

  ## Returns

  - `:ok` - Snapshot written (or already existed)
  - `{:error, reason}` - Write failed
  """
  @spec write(t(), version(), snapshot_data()) :: :ok | {:error, term()}
  def write(%__MODULE__{} = snapshot, version, data) when is_binary(data) do
    key = Keys.snapshot_path(snapshot.cluster, snapshot.shard_tag, version)

    case ObjectStorage.put_if_not_exists(snapshot.backend, key, data) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Reads the latest (highest version) snapshot.

  ## Returns

  - `{:ok, version, data}` - Latest snapshot found
  - `{:error, :not_found}` - No snapshots exist
  - `{:error, reason}` - Read failed
  """
  @spec read_latest(t()) :: {:ok, version(), snapshot_data()} | {:error, :not_found | term()}
  def read_latest(%__MODULE__{} = snapshot) do
    prefix = Keys.snapshots_prefix(snapshot.cluster, snapshot.shard_tag)

    with [key] <- snapshot.backend |> ObjectStorage.list(prefix, limit: 1) |> Enum.take(1),
         {:ok, version} <- Keys.extract_version(key),
         {:ok, data} <- ObjectStorage.get(snapshot.backend, key) do
      {:ok, version, data}
    else
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Reads a specific snapshot by version.

  ## Returns

  - `{:ok, data}` - Snapshot data
  - `{:error, :not_found}` - Snapshot doesn't exist
  - `{:error, reason}` - Read failed
  """
  @spec read(t(), version()) :: {:ok, snapshot_data()} | {:error, :not_found | term()}
  def read(%__MODULE__{} = snapshot, version) do
    key = Keys.snapshot_path(snapshot.cluster, snapshot.shard_tag, version)
    ObjectStorage.get(snapshot.backend, key)
  end

  @doc """
  Lists all snapshots in newest-first order.

  Returns a lazy stream of `{version, key}` tuples.

  ## Options

  - `:limit` - Maximum number of snapshots to return
  """
  @spec list(t(), keyword()) :: Enumerable.t()
  def list(%__MODULE__{} = snapshot, opts \\ []) do
    prefix = Keys.snapshots_prefix(snapshot.cluster, snapshot.shard_tag)

    snapshot.backend
    |> ObjectStorage.list(prefix, opts)
    |> Stream.map(fn key ->
      case Keys.extract_version(key) do
        {:ok, version} -> {version, key}
        {:error, _} -> nil
      end
    end)
    |> Stream.reject(&is_nil/1)
  end

  @doc """
  Gets the latest snapshot version without reading the data.

  ## Returns

  - `{:ok, version}` - Latest version
  - `{:error, :not_found}` - No snapshots exist
  """
  @spec latest_version(t()) :: {:ok, version()} | {:error, :not_found}
  def latest_version(%__MODULE__{} = snapshot) do
    prefix = Keys.snapshots_prefix(snapshot.cluster, snapshot.shard_tag)

    case snapshot.backend |> ObjectStorage.list(prefix, limit: 1) |> Enum.take(1) do
      [key] ->
        Keys.extract_version(key)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Deletes a specific snapshot.

  Deletion is idempotent - deleting a non-existent snapshot succeeds.

  ## Returns

  - `:ok` - Snapshot deleted (or didn't exist)
  - `{:error, reason}` - Delete failed
  """
  @spec delete(t(), version()) :: :ok | {:error, term()}
  def delete(%__MODULE__{} = snapshot, version) do
    key = Keys.snapshot_path(snapshot.cluster, snapshot.shard_tag, version)
    ObjectStorage.delete(snapshot.backend, key)
  end

  @doc """
  Deletes all snapshots older than the given version.

  Useful for cleanup after compaction or when retention policy expires.

  ## Returns

  - `{:ok, deleted_count}` - Number of snapshots deleted
  - `{:error, reason}` - Delete failed (partial deletions may have occurred)
  """
  @spec delete_older_than(t(), version()) :: {:ok, non_neg_integer()} | {:error, term()}
  def delete_older_than(%__MODULE__{} = snapshot, min_version_to_keep) do
    snapshot
    |> list()
    |> Enum.reduce_while({:ok, 0}, fn {version, _key}, {:ok, count} ->
      maybe_delete_older(snapshot, version, min_version_to_keep, count)
    end)
  end

  defp maybe_delete_older(snapshot, version, min_version, count) when version < min_version do
    case delete(snapshot, version) do
      :ok -> {:cont, {:ok, count + 1}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp maybe_delete_older(_snapshot, _version, _min_version, count) do
    {:cont, {:ok, count}}
  end

  @doc """
  Checks if any snapshots exist for this shard.
  """
  @spec exists?(t()) :: boolean()
  def exists?(%__MODULE__{} = snapshot) do
    case latest_version(snapshot) do
      {:ok, _} -> true
      {:error, :not_found} -> false
    end
  end

  @doc """
  Counts the number of snapshots.

  Note: This reads the full list, so it's not efficient for shards with
  many snapshots. Use `exists?/1` to just check for presence.
  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{} = snapshot) do
    snapshot
    |> list()
    |> Enum.count()
  end
end
