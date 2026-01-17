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
  @type error :: {:error, :not_found | :already_exists | :access_denied | term()}
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

  @doc """
  List objects with the given prefix.

  Returns a lazy stream that fetches pages as needed. Objects are returned
  in ascending lexicographic order by key.

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
    module.put(config, key, data, opts)
  end

  @doc """
  Retrieve an object by key.
  """
  @spec get(backend :: {module(), keyword()}, key :: key()) ::
          {:ok, data()} | error()
  def get({module, config}, key) do
    module.get(config, key)
  end

  @doc """
  Delete an object by key.
  """
  @spec delete(backend :: {module(), keyword()}, key :: key()) ::
          :ok | error()
  def delete({module, config}, key) do
    module.delete(config, key)
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
    module.put_if_not_exists(config, key, data, opts)
  end

  @doc """
  Retrieve an object with its version token.
  """
  @spec get_with_version(backend :: {module(), keyword()}, key :: key()) ::
          {:ok, data(), version_token()} | error()
  def get_with_version({module, config}, key) do
    module.get_with_version(config, key)
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
    module.put_if_version_matches(config, key, version_token, data, opts)
  end
end
