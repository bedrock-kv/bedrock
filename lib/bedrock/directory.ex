defprotocol Bedrock.Directory do
  @moduledoc """
  Protocol for FoundationDB directory layer operations.

  Directories provide a hierarchical namespace for organizing data within
  a FoundationDB database. Each directory corresponds to a unique prefix
  that can be used to create isolated keyspaces.

  Based on the FoundationDB directory layer specification.
  """
  alias Bedrock.KeyRange

  @doc """
  Creates a directory at the given path.

  Returns `{:ok, directory}` if successful, or `{:error, reason}` if the
  directory already exists or the parent directory does not exist.

  ## Options

  - `:layer` - Layer identifier for the directory (binary)
  - `:prefix` - Manual prefix assignment (binary)
  - `:version` - Version metadata for the directory (term)
  - `:metadata` - Additional metadata for the directory (term)
  """
  @spec create(t(), [String.t()], layer: binary(), prefix: binary(), version: term(), metadata: term()) ::
          {:ok, t()}
          | {:error, :directory_already_exists}
          | {:error, :parent_directory_does_not_exist}
          | {:error, :invalid_path}
  def create(dir, path, opts \\ [])

  @doc """
  Opens an existing directory at the given path.

  Returns `{:ok, directory}` if successful, or `{:error, :directory_does_not_exist}`
  if the directory does not exist.
  """
  @spec open(t(), [String.t()]) ::
          {:ok, t()}
          | {:error, :directory_does_not_exist}
          | {:error, :invalid_path}
  def open(dir, path)

  @doc """
  Creates or opens a directory at the given path.

  If the directory exists, opens it. Otherwise, creates it.

  ## Options

  - `:layer` - Layer identifier for the directory (binary)
  - `:prefix` - Manual prefix assignment (binary)
  - `:version` - Version metadata for the directory (term)
  - `:metadata` - Additional metadata for the directory (term)
  """
  @spec create_or_open(t(), [String.t()], layer: binary(), prefix: binary(), version: term(), metadata: term()) ::
          {:ok, t()}
          | {:error, :parent_directory_does_not_exist}
          | {:error, :invalid_path}
  def create_or_open(dir, path, opts \\ [])

  @doc """
  Moves a directory from old_path to new_path.

  The directory and all its subdirectories are moved atomically.
  """
  @spec move(t(), [String.t()], [String.t()]) ::
          :ok
          | {:error, :directory_does_not_exist}
          | {:error, :directory_already_exists}
          | {:error, :parent_directory_does_not_exist}
  def move(dir, old_path, new_path)

  @doc """
  Removes a directory and all its subdirectories.

  Returns an error if the directory does not exist.
  """
  @spec remove(t(), [String.t()]) :: :ok | {:error, :directory_does_not_exist}
  def remove(dir, path)

  @doc """
  Removes a directory if it exists.

  Returns `:ok` whether the directory existed or not.
  """
  @spec remove_if_exists(t(), [String.t()]) :: :ok
  def remove_if_exists(dir, path)

  @doc """
  Lists the immediate subdirectories of the given path.

  Returns a list of subdirectory names (strings).
  """
  @spec list(t(), [String.t()]) :: {:ok, [String.t()]}
  def list(dir, path \\ [])

  @doc """
  Checks if a directory exists at the given path.
  """
  @spec exists?(t(), [String.t()]) :: boolean()
  def exists?(dir, path)

  @doc """
  Returns the path of this directory as a list of strings.
  """
  @spec get_path(t()) :: [String.t()]
  def get_path(dir)

  @doc """
  Returns the layer identifier of this directory, or `nil` if none.
  """
  @spec get_layer(t()) :: binary() | nil
  def get_layer(dir)

  @doc """
  Returns a `Bedrock.Subspace` for this directory.

  The subspace can be used to store and retrieve data within
  this directory's keyspace.
  """
  @spec get_subspace(t()) :: Bedrock.Subspace.t()
  def get_subspace(dir)

  @spec range(t()) :: KeyRange.t()
  def range(dir)
end
