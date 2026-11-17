defmodule Bedrock.ObjectStorage do
  @moduledoc """
  Behaviour and API for object storage backends.

  ObjectStorage provides a simple key-value interface for storing and retrieving
  binary data objects. It supports multiple backends (local filesystem, S3, GCS)
  through a common behaviour.

  ## Operations

  - `put/4` - Store an object
  - `get/2` - Retrieve an object
  - `delete/2` - Remove an object
  - `list/2` - List objects with a prefix (returns lazy stream)
  - `put_if_not_exists/4` - Store an object only if it doesn't exist (conditional create)
  - `get_with_version/2` - Retrieve an object with version token
  - `put_if_version_matches/5` - Store an object only if version matches (conditional update)

  ## Canonical Error Reasons

  Backends may return storage-specific errors. The public API normalizes known
  errors to canonical reasons:

  - `:not_found`
  - `:already_exists`
  - `:version_mismatch`
  - `:access_denied`

  ## Path Structure

  Object keys follow a hierarchical structure:
  - Cluster state: `/{cluster}/state`
  - Chunks: `/{cluster}/shards/{tag}/chunks/{inverted_version}`
  - Snapshots: `/{cluster}/shards/{tag}/snapshots/{inverted_version}`

  ## Inverted Version Keys

  Object stores list in ascending order only. To get newest objects first,
  use inverted version numbers: `(2^64 - 1) - version`

  See `Bedrock.ObjectStorage.Keys` for key formatting helpers.
  """

  @type key :: String.t()
  @type data :: iodata()
  @type content_type :: String.t()
  @type opts :: keyword()
  @type error_reason ::
          :not_found
          | :already_exists
          | :version_mismatch
          | :access_denied
          | term()
  @type error :: {:error, error_reason()}
  @type backend :: {module(), keyword()}
  @type version_token :: String.t()

  @doc """
  Store an object at the given key.

  ## Options

  - `:content_type` - MIME type of the data (default: "application/octet-stream")

  ## Returns

  - `:ok` - Object stored successfully
  - `{:error, reason}` - Storage failed
  """
  @callback put(backend :: term(), key :: key(), data :: data(), opts :: opts()) ::
              :ok | error()

  @doc """
  Retrieve an object by key.

  ## Returns

  - `{:ok, data}` - Object data
  - `{:error, :not_found}` - Object does not exist
  - `{:error, reason}` - Retrieval failed
  """
  @callback get(backend :: term(), key :: key()) ::
              {:ok, data()} | error()

  @doc """
  Delete an object by key.

  Deletion is idempotent - deleting a non-existent object succeeds.

  ## Returns

  - `:ok` - Object deleted (or didn't exist)
  - `{:error, reason}` - Deletion failed
  """
  @callback delete(backend :: term(), key :: key()) ::
              :ok | error()

  defmodule ListError do
    @moduledoc """
    Raised when a listing cannot be completed.

    `list/3` returns a bare `Enumerable.t()`, which has no way to report
    failure — a stream can only yield elements or end. A backend that
    quietly stopped on error would be indistinguishable from a prefix
    that is genuinely empty, and consumers read that silence as fact: a
    materializer treats "no chunks here" as "this shard has no data at
    that version" and advances past everything it never received.

    So the stream raises instead. Absence and ignorance are different
    answers and must not share a representation.
    """
    defexception [:reason, :prefix]

    @impl true
    def message(%__MODULE__{reason: reason, prefix: prefix}),
      do: "failed to list objects under #{inspect(prefix)}: #{inspect(reason)}"
  end

  defmodule UnparseableKeyError do
    @moduledoc """
    Raised when an object in a prefix only Bedrock writes to carries a
    name this build cannot read a version out of.

    The listing itself succeeded, so this is not `ListError`: the object
    is there, and it is ours — a nested object that merely shares the
    prefix is foreign and is passed over instead (see
    `Bedrock.ObjectStorage.Keys.extract_version/2`). What we do not know
    is which version it holds, because the name is either corrupt or
    written in a format a later build understands and this one does not.

    Dropping it would shorten the history and make the shard look older
    or emptier than it is — the same lie `ListError` exists to prevent,
    one layer up. Absence and ignorance are different answers and must
    not share a representation.
    """
    defexception [:key, :prefix]

    @impl true
    def message(%__MODULE__{key: key, prefix: prefix}),
      do: "unparseable object key #{inspect(key)} under #{inspect(prefix)}"
  end

  @doc """
  List objects with the given prefix.

  Returns a lazy stream that fetches pages as needed. Objects are returned
  in ascending lexicographic order by key.

  RAISES `ListError` if the backend cannot complete the listing, so an
  empty stream means the prefix IS empty rather than unknown.

  ## Options

  - `:limit` - Maximum number of keys to return (default: unlimited)

  ## Returns

  A `Stream` of key strings.
  """
  @callback list(backend :: term(), prefix :: String.t(), opts :: opts()) ::
              Enumerable.t()

  @doc """
  Store an object only if it doesn't already exist (conditional write).

  This provides atomicity for write operations where concurrent writers
  might attempt to create the same object. Only one writer will succeed.

  ## Options

  Same as `put/4`.

  ## Returns

  - `:ok` - Object stored successfully (was new)
  - `{:error, :already_exists}` - Object already exists
  - `{:error, reason}` - Storage failed
  """
  @callback put_if_not_exists(backend :: term(), key :: key(), data :: data(), opts :: opts()) ::
              :ok | error()

  @doc """
  Retrieve an object with its version token for conditional updates.

  Returns the object data along with an opaque version token that can be
  passed to `put_if_version_matches/5` to implement optimistic concurrency.

  ## Returns

  - `{:ok, data, version_token}` - Object data and version token
  - `{:error, :not_found}` - Object does not exist
  - `{:error, reason}` - Retrieval failed
  """
  @callback get_with_version(backend :: term(), key :: key()) ::
              {:ok, data(), version_token()} | error()

  @doc """
  Store an object only if its version matches the expected token.

  This implements optimistic locking (compare-and-swap) semantics.
  The operation succeeds only if the current version of the object
  matches the provided version_token (obtained from `get_with_version/2`).

  ## Returns

  - `:ok` - Object updated successfully
  - `{:error, :version_mismatch}` - Object was modified since version_token was obtained
  - `{:error, :not_found}` - Object does not exist
  - `{:error, reason}` - Update failed
  """
  @callback put_if_version_matches(
              backend :: term(),
              key :: key(),
              version_token :: version_token(),
              data :: data(),
              opts :: opts()
            ) :: :ok | error()

  @doc """
  Normalizes backend-specific error reasons to canonical ObjectStorage reasons.

  Canonical reasons:

  - `:not_found`
  - `:already_exists`
  - `:version_mismatch`
  - `:access_denied`
  """
  @spec normalize_error({:error, term()}) :: error()
  def normalize_error({:error, reason}) do
    {:error, normalize_reason(reason)}
  end

  @doc """
  Creates an opaque backend reference for the given module and config.

  ## Examples

      backend = ObjectStorage.backend(ObjectStorage.LocalFilesystem, root: "/tmp/objects")
      ObjectStorage.put(backend, "test/key", "data")
  """
  @spec backend(module :: module(), config :: keyword()) :: {module(), keyword()}
  def backend(module, config \\ []) when is_atom(module) do
    {module, config}
  end

  @doc """
  Store an object at the given key.
  """
  @spec put(backend :: {module(), keyword()}, key :: key(), data :: data(), opts :: opts()) ::
          :ok | error()
  def put({module, config}, key, data, opts \\ []) do
    case module.put(config, key, data, opts) do
      :ok -> :ok
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  @doc """
  Retrieve an object by key.
  """
  @spec get(backend :: {module(), keyword()}, key :: key()) ::
          {:ok, data()} | error()
  def get({module, config}, key) do
    case module.get(config, key) do
      {:ok, data} -> {:ok, data}
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  @doc """
  Delete an object by key.
  """
  @spec delete(backend :: {module(), keyword()}, key :: key()) ::
          :ok | error()
  def delete({module, config}, key) do
    case module.delete(config, key) do
      :ok -> :ok
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  @doc """
  List objects with the given prefix.
  """
  @spec list(backend :: {module(), keyword()}, prefix :: String.t(), opts :: opts()) ::
          Enumerable.t()
  def list({module, config}, prefix, opts \\ []) do
    module.list(config, prefix, opts)
  end

  @doc """
  Store an object only if it doesn't already exist.
  """
  @spec put_if_not_exists(
          backend :: {module(), keyword()},
          key :: key(),
          data :: data(),
          opts :: opts()
        ) ::
          :ok | error()
  def put_if_not_exists({module, config}, key, data, opts \\ []) do
    case module.put_if_not_exists(config, key, data, opts) do
      :ok -> :ok
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  @doc """
  Retrieve an object with its version token.
  """
  @spec get_with_version(backend :: {module(), keyword()}, key :: key()) ::
          {:ok, data(), version_token()} | error()
  def get_with_version({module, config}, key) do
    case module.get_with_version(config, key) do
      {:ok, data, version_token} -> {:ok, data, version_token}
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  @doc """
  Store an object only if its version matches.
  """
  @spec put_if_version_matches(
          backend :: {module(), keyword()},
          key :: key(),
          version_token :: version_token(),
          data :: data(),
          opts :: opts()
        ) :: :ok | error()
  def put_if_version_matches({module, config}, key, version_token, data, opts \\ []) do
    case module.put_if_version_matches(config, key, version_token, data, opts) do
      :ok -> :ok
      {:error, _reason} = error -> normalize_error(error)
    end
  end

  defp normalize_reason(:enoent), do: :not_found
  defp normalize_reason(:enotdir), do: :not_found
  defp normalize_reason(:eexist), do: :already_exists
  defp normalize_reason(:eacces), do: :access_denied
  defp normalize_reason(:eperm), do: :access_denied
  defp normalize_reason({:http_error, 401}), do: :access_denied
  defp normalize_reason({:http_error, 403}), do: :access_denied
  defp normalize_reason({:http_error, 404}), do: :not_found
  defp normalize_reason({:http_error, 409}), do: :already_exists
  defp normalize_reason({:http_error, 412}), do: :version_mismatch
  defp normalize_reason({:precondition_failed, _reason}), do: :version_mismatch
  defp normalize_reason(reason), do: reason
end
