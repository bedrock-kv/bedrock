defmodule Bedrock.Directory do
  @moduledoc """
  Directory Layer implementation for Bedrock.

  The Directory Layer provides a hierarchical namespace for organizing data,
  similar to a filesystem. It automatically manages key prefixes and provides
  efficient path-to-prefix mapping using the High-Concurrency Allocator (HCA).

  ## Features

  - **Hierarchical Paths**: Organize data using path-like structures (e.g., ["app", "users", "profiles"])
  - **Automatic Prefix Management**: Converts human-readable paths to efficient binary prefixes
  - **High Performance**: Uses HCA for concurrent directory allocation
  - **Path Operations**: Create, move, remove, and list directories
  - **Efficient Scans**: Generated prefixes are optimized for range queries

  ## Usage

      # Create directory layer
      directory = Bedrock.Directory.new(MyApp.Repo)
      
      # Create directories and get prefixes
      user_prefix = MyApp.Repo.transaction(fn txn ->
        Bedrock.Directory.create_or_open(directory, txn, ["app", "users"])
      end)
      
      # Use prefix for data storage
      MyApp.Repo.transaction(fn txn ->
        user_key = user_prefix <> "user:123"
        Bedrock.Repo.put(txn, user_key, user_data)
      end)

  Based on the FoundationDB Directory Layer specification.
  """

  alias Bedrock.HCA
  alias Bedrock.Subspace

  # Directory layer constants
  # Version 0.1.0
  @version <<0, 1, 0>>
  @default_content_subspace "_content"
  @default_node_subspace "_node"

  defstruct [:repo_module, :content_subspace, :node_subspace, :allocator, :allow_manual_prefixes]

  @type t :: %__MODULE__{
          repo_module: module(),
          content_subspace: Subspace.t(),
          node_subspace: Subspace.t(),
          allocator: HCA.t(),
          allow_manual_prefixes: boolean()
        }

  @type path :: [binary()]
  @type prefix :: binary()

  @doc """
  Create a new Directory Layer instance.

  ## Parameters

    * `repo_module` - The Bedrock.Repo module to use for transactions
    * `content_subspace` - Subspace for storing directory metadata (optional)
    * `node_subspace` - Subspace for storing path-to-prefix mappings (optional)

  ## Options

    * `:allow_manual_prefixes` - Allow manually specified prefixes (default: false)
    * `:random_fn` - Custom random function for HCA testing (default: &:rand.uniform/1)

  ## Examples

      # Standard directory layer
      dir = Bedrock.Directory.new(MyApp.Repo)
      
      # Custom subspaces  
      dir = Bedrock.Directory.new(MyApp.Repo, "my_content", "my_nodes")
      
      # With options
      dir = Bedrock.Directory.new(MyApp.Repo, "content", "nodes", 
                                  allow_manual_prefixes: true)
                                  
      # For testing with controlled randomization
      deterministic_fn = fn _size -> 1 end
      dir = Bedrock.Directory.new(MyApp.Repo, "content", "nodes",
                                  random_fn: deterministic_fn)
  """
  @spec new(module(), binary(), binary(), keyword()) :: t()
  def new(
        repo_module,
        content_subspace \\ @default_content_subspace,
        node_subspace \\ @default_node_subspace,
        opts \\ []
      ) do
    allow_manual = Keyword.get(opts, :allow_manual_prefixes, false)

    # Create proper subspaces using tuple encoding
    content_space = Subspace.new({content_subspace})
    node_space = Subspace.new({node_subspace})

    # Extract HCA-specific options - HCA still uses binary prefix for now
    hca_opts = Keyword.take(opts, [:random_fn])
    hca_subspace = Subspace.pack(content_space, {"hca"})
    allocator = HCA.new(repo_module, hca_subspace, hca_opts)

    %__MODULE__{
      repo_module: repo_module,
      content_subspace: content_space,
      node_subspace: node_space,
      allocator: allocator,
      allow_manual_prefixes: allow_manual
    }
  end

  @doc """
  Create or open a directory at the given path.

  If the directory already exists, returns its prefix. If it doesn't exist,
  creates it with an automatically allocated prefix.

  ## Parameters

    * `directory` - The Directory Layer instance
    * `txn` - The transaction to use
    * `path` - Path components as a list of binaries

  ## Examples

      MyApp.Repo.transaction(fn txn ->
        # Create nested directory structure
        user_prefix = Bedrock.Directory.create_or_open(dir, txn, ["app", "users"])
        profile_prefix = Bedrock.Directory.create_or_open(dir, txn, ["app", "users", "profiles"])
        
        # Use prefixes for data storage
        user_key = user_prefix <> "user:123"
        profile_key = profile_prefix <> "profile:123"
      end)
  """
  @spec create_or_open(t(), term(), path()) :: prefix()
  def create_or_open(%__MODULE__{} = directory, txn, path) when is_list(path) do
    case lookup_prefix(directory, txn, path) do
      {:ok, prefix} ->
        prefix

      {:error, :not_found} ->
        create_directory(directory, txn, path)
    end
  end

  @doc """
  Create a new directory at the given path.

  Fails if the directory already exists. Use `create_or_open/3` for 
  idempotent directory creation.

  ## Parameters

    * `directory` - The Directory Layer instance
    * `txn` - The transaction to use
    * `path` - Path components as a list of binaries
    * `prefix` - Optional manual prefix (requires allow_manual_prefixes: true)
  """
  @spec create_directory(t(), term(), path(), prefix() | nil) :: prefix()
  def create_directory(%__MODULE__{} = directory, txn, path, manual_prefix \\ nil) when is_list(path) do
    # Ensure parent directories exist
    ensure_parent_directories(directory, txn, path)

    # Check if directory already exists
    case lookup_prefix(directory, txn, path) do
      {:ok, _existing} ->
        raise "Directory already exists: #{inspect(path)}"

      {:error, :not_found} ->
        # Allocate or use manual prefix
        prefix =
          case manual_prefix do
            nil -> allocate_prefix(directory, txn)
            prefix when directory.allow_manual_prefixes -> prefix
            _ -> raise "Manual prefixes not allowed"
          end

        # Store the mapping
        store_directory_mapping(directory, txn, path, prefix)
        prefix
    end
  end

  @doc """
  Open an existing directory and return its prefix.

  Fails if the directory doesn't exist. Use `create_or_open/3` to create
  directories automatically.
  """
  @spec open_directory(t(), term(), path()) :: prefix()
  def open_directory(%__MODULE__{} = directory, txn, path) when is_list(path) do
    case lookup_prefix(directory, txn, path) do
      {:ok, prefix} -> prefix
      {:error, :not_found} -> raise "Directory not found: #{inspect(path)}"
    end
  end

  @doc """
  Check if a directory exists at the given path.
  """
  @spec exists?(t(), term(), path()) :: boolean()
  def exists?(%__MODULE__{} = directory, txn, path) when is_list(path) do
    case lookup_prefix(directory, txn, path) do
      {:ok, _} -> true
      {:error, :not_found} -> false
    end
  end

  @doc """
  List all subdirectories of the given path.

  Returns a list of {name, subpath} tuples for immediate children.
  """
  @spec list_directories(t(), term(), path()) :: [{binary(), path()}]
  def list_directories(%__MODULE__{} = directory, txn, path) when is_list(path) do
    # Create tuple-based range for scanning subdirectories
    # We want all keys that start with the path tuple
    path_tuple = List.to_tuple(path)
    {start_key, end_key} = Subspace.range(directory.node_subspace, path_tuple)

    # Immediate children are one level deeper
    expected_depth = length(path) + 1

    txn
    |> directory.repo_module.range(start_key, end_key)
    |> Enum.map(fn {key, _prefix} ->
      # Extract subdirectory path from tuple-encoded key
      subpath = key_to_path(directory, key)

      case subpath do
        # Invalid key, skip
        [] -> nil
        _ -> {subpath, List.last(subpath)}
      end
    end)
    # Remove invalid keys
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(fn {subpath, _name} ->
      # Only include immediate children (not nested deeper)
      length(subpath) == expected_depth and List.starts_with?(subpath, path)
    end)
    |> Enum.map(fn {subpath, name} ->
      {name, subpath}
    end)
    # Remove duplicates
    |> Enum.uniq_by(fn {name, _} -> name end)
  end

  @doc """
  Remove a directory and all its subdirectories.

  This is a destructive operation that will remove all directory mappings
  under the given path. It does not remove the actual data - only the
  directory structure.
  """
  @spec remove_directory(t(), term(), path()) :: :ok
  def remove_directory(%__MODULE__{} = directory, txn, path) when is_list(path) do
    # Remove all subdirectories first
    subdirs = list_directories(directory, txn, path)

    Enum.each(subdirs, fn {_name, subpath} ->
      remove_directory(directory, txn, subpath)
    end)

    # Remove the directory itself
    path_key = path_to_key(directory, path)
    # Delete the mapping entirely
    directory.repo_module.clear(txn, path_key)

    :ok
  end

  @doc """
  Move a directory from one path to another.

  This operation is atomic - either the entire move succeeds or it fails.
  The move will fail if the destination already exists.
  """
  @spec move_directory(t(), term(), path(), path()) :: :ok
  def move_directory(%__MODULE__{} = directory, txn, from_path, to_path) when is_list(from_path) and is_list(to_path) do
    # Get the prefix for the source directory
    prefix = open_directory(directory, txn, from_path)

    # Ensure destination doesn't exist
    if exists?(directory, txn, to_path) do
      raise "Destination directory already exists: #{inspect(to_path)}"
    end

    # Ensure parent of destination exists
    ensure_parent_directories(directory, txn, to_path)

    # Create new mapping at destination
    store_directory_mapping(directory, txn, to_path, prefix)

    # Remove old mapping
    from_key = path_to_key(directory, from_path)
    directory.repo_module.clear(txn, from_key)

    # Move all subdirectories
    subdirs = list_directories(directory, txn, from_path)

    Enum.each(subdirs, fn {name, _subpath} ->
      old_subpath = from_path ++ [name]
      new_subpath = to_path ++ [name]
      move_directory(directory, txn, old_subpath, new_subpath)
    end)

    :ok
  end

  # Private implementation functions

  defp lookup_prefix(directory, txn, path) do
    path_key = path_to_key(directory, path)

    case directory.repo_module.get(txn, path_key) do
      nil -> {:error, :not_found}
      prefix -> {:ok, prefix}
    end
  end

  defp ensure_parent_directories(directory, txn, path) do
    case path do
      # Root always exists
      [] ->
        :ok

      # Top-level directory
      [_] ->
        :ok

      _ ->
        parent_path = Enum.drop(path, -1)
        create_or_open(directory, txn, parent_path)
        :ok
    end
  end

  defp allocate_prefix(directory, txn) do
    {:ok, prefix_id} = HCA.allocate(directory.allocator, txn)

    # Convert ID to binary prefix
    # Use a compact encoding: version + ID as variable-length integer
    id_binary = encode_prefix_id(prefix_id)
    @version <> id_binary
  end

  defp store_directory_mapping(directory, txn, path, prefix) do
    path_key = path_to_key(directory, path)
    directory.repo_module.put(txn, path_key, prefix)
  end

  defp path_to_key(directory, path) do
    # Convert path to a tuple-encoded key for storage
    # Path components are stored as a tuple within the node subspace
    path_tuple = List.to_tuple(path)
    Subspace.pack(directory.node_subspace, path_tuple)
  end

  # Convert tuple-encoded storage key back to path
  defp key_to_path(directory, key) do
    path_tuple = Subspace.unpack(directory.node_subspace, key)
    # Using Erlang's built-in tuple_to_list/1
    :erlang.tuple_to_list(path_tuple)
  rescue
    ArgumentError ->
      # Key doesn't belong to our subspace
      []
  end

  defp encode_prefix_id(id) when id < 128 do
    # Single byte for small IDs
    <<id>>
  end

  defp encode_prefix_id(id) when id < 16_384 do
    # Two bytes for medium IDs
    <<id::16-big>>
  end

  defp encode_prefix_id(id) do
    # Four bytes for large IDs
    <<id::32-big>>
  end

  @doc """
  Get statistics about the directory layer.

  Returns information about allocated prefixes, directory count, etc.
  """
  @spec stats(t(), term()) :: %{
          directory_count: non_neg_integer(),
          allocated_prefixes: non_neg_integer(),
          hca_stats: map()
        }
  def stats(%__MODULE__{} = directory, txn) do
    hca_stats = HCA.stats(directory.allocator, txn)

    # Count directory entries using proper subspace range
    {start_key, end_key} = Subspace.range(directory.node_subspace)

    directory_count =
      txn
      |> directory.repo_module.range(start_key, end_key)
      |> Enum.count()

    %{
      directory_count: directory_count,
      allocated_prefixes: hca_stats.estimated_allocated,
      hca_stats: hca_stats
    }
  end
end
