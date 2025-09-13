defmodule Bedrock.Directory do
  @moduledoc """
  FoundationDB directory layer operations.

  Directories provide a hierarchical namespace for organizing data within
  a FoundationDB database. Each directory corresponds to a unique prefix
  that can be used to create isolated keyspaces.

  Based on the FoundationDB directory layer specification.
  """

  alias Bedrock.HCA
  alias Bedrock.Key
  alias Bedrock.KeyRange
  alias Bedrock.Subspace

  # Struct definitions

  defmodule Node do
    @moduledoc """
    A directory node returned by directory operations.

    Contains the metadata and prefix for a directory, and can generate
    a subspace for data storage within the directory.
    """

    defstruct [:prefix, :path, :layer, :directory_layer, :version, :metadata]

    @type t :: %__MODULE__{
            prefix: binary(),
            path: [String.t()],
            layer: binary() | nil,
            directory_layer: Bedrock.Directory.Layer.t(),
            version: term() | nil,
            metadata: term() | nil
          }
  end

  defmodule Partition do
    @moduledoc """
    A directory partition provides an isolated namespace within a directory.

    Partitions have their own prefix allocation and prevent operations
    outside their boundary, providing isolation between different parts
    of an application.
    """

    defstruct [:directory_layer, :path, :prefix, :version, :metadata]

    @type t :: %__MODULE__{
            directory_layer: Bedrock.Directory.Layer.t(),
            path: [String.t()],
            prefix: binary(),
            version: term() | nil,
            metadata: term() | nil
          }
  end

  defmodule Layer do
    @moduledoc """
    Main implementation of the FoundationDB directory layer.

    Manages directory hierarchy and metadata storage using the same
    key/value layout as FoundationDB's directory layer.
    """

    defstruct [
      :node_subspace,
      :content_subspace,
      :repo,
      :next_prefix_fn,
      :path
    ]

    @type t :: %__MODULE__{
            node_subspace: Subspace.t(),
            content_subspace: Subspace.t(),
            repo: module(),
            next_prefix_fn: (Bedrock.Internal.Repo.transaction() -> binary()),
            path: [String.t()]
          }
  end

  @type directory :: Node.t() | Partition.t() | Layer.t()

  # System subspace prefixes - match FDB exactly
  @node_subspace_prefix <<0xFE>>
  @content_subspace_prefix <<>>

  # Reserved prefix ranges - match FDB system prefixes
  @reserved_prefixes [
    # Node subspace
    <<0xFE>>,
    # System keys
    <<0xFF>>
  ]

  # Version management
  @layer_version {1, 0, 0}
  @version_key "version"

  @doc """
  Creates a new directory layer.

  ## Arguments

  - `repo` - Module implementing Bedrock.Repo behaviour (required)

  ## Options

  - `:next_prefix_fn` - Function for prefix allocation (default: uses HCA)
  - `:node_subspace` - Custom node storage location
  - `:content_subspace` - Custom content storage location
  """
  @spec root(module(), keyword()) :: Node.t()
  def root(repo, opts \\ []) when is_atom(repo) do
    # Default HCA-based prefix allocation
    hca = HCA.new(repo, @content_subspace_prefix)

    next_prefix_fn =
      opts[:next_prefix_fn] ||
        fn ->
          case HCA.allocate(hca) do
            {:ok, prefix} -> prefix
            {:error, reason} -> raise "Failed to allocate prefix: #{inspect(reason)}"
          end
        end

    node_subspace = Keyword.get(opts, :node_subspace, Subspace.new(@node_subspace_prefix))
    content_subspace = Keyword.get(opts, :content_subspace, Subspace.new(@content_subspace_prefix))

    layer = %Layer{
      node_subspace: node_subspace,
      content_subspace: content_subspace,
      repo: repo,
      next_prefix_fn: next_prefix_fn,
      path: []
    }

    # Return the root directory directly
    %Node{
      prefix: Subspace.key(content_subspace),
      path: [],
      layer: nil,
      directory_layer: layer,
      version: nil,
      metadata: nil
    }
  end

  # Directory operations - function overloading based on struct types

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
  @spec create(directory(), [String.t()], keyword()) ::
          {:ok, directory()}
          | {:error, :directory_already_exists}
          | {:error, :parent_directory_does_not_exist}
          | {:error, :invalid_path}
  def create(dir, path, opts \\ [])

  # Node implementation - delegate to directory_layer
  def create(%Node{directory_layer: layer}, path, opts), do: create(layer, path, opts)

  # Partition implementation - validate boundaries, then delegate
  def create(%Partition{directory_layer: layer}, path, opts) do
    validate_within_partition!(path)
    create(layer, path, opts)
  end

  # Layer implementation - performs actual operations
  def create(%Layer{repo: repo} = layer, path, opts) do
    with :ok <- validate_path(path),
         true <- not root?(path) || {:error, :cannot_create_root} do
      repo.transaction(fn txn -> do_create(layer, txn, path, opts) end)
    end
  end

  @doc """
  Opens an existing directory at the given path.

  Returns `{:ok, directory}` if successful, or `{:error, :directory_does_not_exist}`
  if the directory does not exist.
  """
  @spec open(directory(), [String.t()]) ::
          {:ok, directory()}
          | {:error, :directory_does_not_exist}
          | {:error, :invalid_path}
  def open(%Node{directory_layer: layer}, path), do: open(layer, path)

  def open(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    open(layer, path)
  end

  def open(%Layer{repo: repo} = layer, path) do
    with :ok <- validate_path(path),
         true <- not root?(path) || {:error, :cannot_open_root} do
      repo.transaction(fn txn -> do_open(layer, txn, path) end)
    end
  end

  @doc """
  Creates or opens a directory at the given path.

  If the directory exists, opens it. Otherwise, creates it.

  ## Options

  - `:layer` - Layer identifier for the directory (binary)
  - `:prefix` - Manual prefix assignment (binary)
  - `:version` - Version metadata for the directory (term)
  - `:metadata` - Additional metadata for the directory (term)
  """
  @spec create_or_open(directory(), [String.t()], keyword()) ::
          {:ok, directory()}
          | {:error, :parent_directory_does_not_exist}
          | {:error, :invalid_path}
  def create_or_open(dir, path, opts \\ [])

  def create_or_open(%Node{directory_layer: layer}, path, opts), do: create_or_open(layer, path, opts)

  def create_or_open(%Partition{directory_layer: layer}, path, opts) do
    validate_within_partition!(path)
    create_or_open(layer, path, opts)
  end

  def create_or_open(%Layer{repo: repo} = layer, path, opts) do
    with :ok <- validate_path(path),
         true <- not root?(path) || {:error, :cannot_create_or_open_root} do
      repo.transaction(fn txn ->
        case do_open(layer, txn, path) do
          {:ok, node} -> {:ok, node}
          {:error, :directory_does_not_exist} -> do_create(layer, txn, path, opts)
        end
      end)
    end
  end

  @doc """
  Moves a directory from old_path to new_path.

  The directory and all its subdirectories are moved atomically.
  """
  @spec move(directory(), [String.t()], [String.t()]) ::
          :ok
          | {:error, :directory_does_not_exist}
          | {:error, :directory_already_exists}
          | {:error, :parent_directory_does_not_exist}
  def move(%Node{directory_layer: layer}, old_path, new_path), do: move(layer, old_path, new_path)

  def move(%Partition{directory_layer: layer}, old_path, new_path) do
    validate_within_partition!(old_path)
    validate_within_partition!(new_path)
    move(layer, old_path, new_path)
  end

  def move(%Layer{repo: repo} = layer, old_path, new_path),
    do: repo.transaction(fn txn -> do_move(layer, txn, old_path, new_path) end)

  @doc """
  Removes a directory and all its subdirectories.

  Returns an error if the directory does not exist.
  """
  @spec remove(directory(), [String.t()]) :: :ok | {:error, :directory_does_not_exist}
  def remove(%Node{directory_layer: layer}, path), do: remove(layer, path)

  def remove(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    remove(layer, path)
  end

  def remove(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> do_remove(layer, txn, path) end)

  @doc """
  Removes a directory if it exists.

  Returns `:ok` whether the directory existed or not.
  """
  @spec remove_if_exists(directory(), [String.t()]) :: :ok
  def remove_if_exists(%Node{directory_layer: layer}, path), do: remove_if_exists(layer, path)

  def remove_if_exists(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    remove_if_exists(layer, path)
  end

  def remove_if_exists(%Layer{repo: repo} = layer, path) do
    repo.transaction(fn txn ->
      case do_remove(layer, txn, path) do
        :ok -> :ok
        {:error, :directory_does_not_exist} -> :ok
        {:error, :cannot_remove_root} = error -> error
        error -> error
      end
    end)
  end

  @doc """
  Lists the immediate subdirectories of the given path.

  Returns a list of subdirectory names (strings).
  """
  @spec list(directory(), [String.t()]) :: {:ok, [String.t()]}
  def list(dir, path \\ [])

  def list(%Node{directory_layer: layer}, path), do: list(layer, path)

  def list(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    list(layer, path)
  end

  def list(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> do_list(layer, txn, path) end)

  @doc """
  Checks if a directory exists at the given path.
  """
  @spec exists?(directory(), [String.t()]) :: boolean()
  def exists?(%Node{directory_layer: layer}, path), do: exists?(layer, path)

  def exists?(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    exists?(layer, path)
  end

  def exists?(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> do_exists?(layer, txn, path) end)

  @doc """
  Returns the path of this directory as a list of strings.
  """
  @spec get_path(directory()) :: [String.t()]
  def get_path(%Node{path: path}), do: path
  def get_path(%Partition{path: path}), do: path
  def get_path(%Layer{path: path}), do: path

  @doc """
  Returns the layer identifier of this directory, or `nil` if none.
  """
  @spec get_layer(directory()) :: binary() | nil
  def get_layer(%Node{layer: layer}), do: layer
  def get_layer(%Partition{}), do: "partition"
  def get_layer(%Layer{}), do: nil

  @doc """
  Returns a `Bedrock.Subspace` for this directory.

  The subspace can be used to store and retrieve data within
  this directory's keyspace.
  """
  @spec get_subspace(directory()) :: Subspace.t()
  def get_subspace(%Node{prefix: prefix}), do: Subspace.new(prefix)
  def get_subspace(%Partition{prefix: prefix}), do: Subspace.new(prefix)

  def get_subspace(%Layer{}),
    do: raise(ArgumentError, "Cannot get subspace from directory layer - use open/create first")

  defimpl Bedrock.Subspace.Subspaceable, for: Node do
    def to_subspace(%Node{prefix: prefix}), do: Subspace.new(prefix)
  end

  defimpl Bedrock.Subspace.Subspaceable, for: Partition do
    def to_subspace(%Partition{prefix: prefix}), do: Subspace.new(prefix)
  end

  @spec range(directory()) :: KeyRange.t()
  def range(%Node{prefix: prefix}), do: Key.to_range(prefix)
  def range(%Partition{prefix: prefix}), do: Key.to_range(prefix)
  def range(%Layer{}), do: raise(ArgumentError, "Cannot perform range on directory layer - use open/create first")

  # Helper functions (moved from Layer module)

  # Helpers for path validation
  def validate_path(path) when is_list(path), do: validate_path_components(path)
  def validate_path(path) when is_binary(path), do: validate_path_components([path])
  def validate_path(path) when is_tuple(path), do: validate_path_components(Tuple.to_list(path))
  def validate_path(_), do: {:error, :invalid_path_format}

  defp validate_path_components([]), do: :ok

  defp validate_path_components([component | rest]) do
    with :ok <- validate_component(component) do
      validate_path_components(rest)
    end
  end

  defp validate_component(component) when not is_binary(component), do: {:error, :invalid_path_component}

  defp validate_component(""), do: {:error, :empty_path_component}
  defp validate_component("."), do: {:error, :invalid_directory_name}
  defp validate_component(".."), do: {:error, :invalid_directory_name}

  defp validate_component(<<0xFE, _rest::binary>>), do: {:error, :reserved_prefix_in_path}
  defp validate_component(<<0xFF, _rest::binary>>), do: {:error, :reserved_prefix_in_path}

  defp validate_component(component) do
    cond do
      not String.valid?(component) ->
        {:error, :invalid_utf8_in_path}

      String.contains?(component, <<0>>) ->
        {:error, :null_byte_in_path}

      true ->
        :ok
    end
  end

  # Ensure operations don't escape the partition boundary
  defp validate_within_partition!(path) when is_list(path) do
    # Paths starting with ".." or containing ".." are trying to escape
    if Enum.any?(path, &(&1 == ".." or String.starts_with?(&1, "../"))) do
      raise ArgumentError, "Cannot access path outside partition boundary"
    end

    :ok
  end

  defp validate_within_partition!(_), do: raise(ArgumentError, "Invalid path format for partition operation")

  # Root directory helpers
  def root?([]), do: true
  def root?(_), do: false

  # Key encoding for node metadata
  defp node_key(%Layer{node_subspace: node_subspace, path: base_path}, path) do
    case base_path ++ path do
      [] -> Subspace.key(node_subspace)
      full_path -> Subspace.pack(node_subspace, full_path)
    end
  end

  # Encode node metadata value - must match FDB format
  defp encode_node_value(prefix, layer, nil, nil), do: Key.pack({prefix, layer || ""})
  defp encode_node_value(prefix, layer, version, nil), do: Key.pack({prefix, layer || "", version})
  defp encode_node_value(prefix, layer, nil, metadata), do: Key.pack({prefix, layer || "", nil, metadata})
  defp encode_node_value(prefix, layer, version, metadata), do: Key.pack({prefix, layer || "", version, metadata})

  defp decode_node_value(value) do
    value
    |> Key.unpack()
    |> case do
      # Legacy 2-tuple format (backward compatibility)
      {prefix, <<>>} -> {prefix, nil, nil, nil}
      {prefix, layer} -> {prefix, layer, nil, nil}
      # 3-tuple format with version
      {prefix, <<>>, version} -> {prefix, nil, version, nil}
      {prefix, layer, version} -> {prefix, layer, version, nil}
      # Full 4-tuple format with version and metadata
      {prefix, <<>>, version, metadata} -> {prefix, nil, version, metadata}
      {prefix, layer, version, metadata} -> {prefix, layer, version, metadata}
    end
  end

  # Version management implementation

  defp check_version(%Layer{repo: repo} = layer, txn, allow_writes) do
    # Version key is at the root of the node subspace
    version_key = Subspace.pack(layer.node_subspace, [@version_key])

    case repo.get(txn, version_key) do
      nil ->
        # No version stored yet - writing operations will initialize
        :ok

      stored_version_binary ->
        stored_version = decode_version(stored_version_binary)
        compare_versions(stored_version, @layer_version, allow_writes)
    end
  end

  defp ensure_version_initialized(%Layer{repo: repo} = layer, txn) do
    # Version key is at the root of the node subspace
    version_key = Subspace.pack(layer.node_subspace, [@version_key])

    case repo.get(txn, version_key) do
      nil -> init_version(layer, txn)
      _ -> :ok
    end
  end

  defp init_version(%Layer{repo: repo} = layer, txn) do
    version_key = Subspace.pack(layer.node_subspace, [@version_key])
    version_binary = encode_version(@layer_version)
    repo.put(txn, version_key, version_binary)
    :ok
  end

  defp encode_version({major, minor, patch}), do: <<major::little-32, minor::little-32, patch::little-32>>
  defp decode_version(<<major::little-32, minor::little-32, patch::little-32>>), do: {major, minor, patch}

  defp compare_versions({stored_major, _, _}, {current_major, _, _}, _) when stored_major > current_major,
    do: {:error, :incompatible_directory_version}

  defp compare_versions({stored_major, stored_minor, _}, {current_major, current_minor, _}, true)
       when stored_major == current_major and stored_minor > current_minor,
       do: {:error, :directory_version_write_restricted}

  defp compare_versions(_, _, _), do: :ok

  # Prefix collision detection

  def is_prefix_free?(%Layer{repo: repo, content_subspace: content_subspace}, txn, prefix) when is_binary(prefix) do
    # Check if this prefix would collide with any existing keys
    # A prefix is free if:
    # 1. Not a reserved prefix
    # 2. No existing keys start with this prefix
    # 3. This prefix doesn't start with any existing key

    # First check: Is this a reserved prefix?
    if reserved_prefix?(prefix) do
      false
    else
      # Second check: Are there any keys that start with our prefix?
      # The content subspace prefix combined with the directory prefix
      full_prefix = Subspace.key(content_subspace) <> prefix
      prefix_range = KeyRange.from_prefix(full_prefix)

      case repo.range(txn, prefix_range, limit: 1) do
        [] ->
          # No keys start with our prefix, now check the reverse
          # Are there any existing keys that our prefix starts with?
          check_prefix_ancestors(repo, txn, content_subspace, prefix)

        _ ->
          # Found keys that start with our prefix
          false
      end
    end
  end

  defp reserved_prefix?(prefix) do
    Enum.any?(@reserved_prefixes, fn reserved ->
      # Check if the prefix starts with any reserved prefix
      String.starts_with?(prefix, reserved)
    end)
  end

  defp check_prefix_ancestors(_repo, _txn, _content_subspace, <<>>), do: true
  defp check_prefix_ancestors(_repo, _txn, _content_subspace, <<_>>), do: true

  defp check_prefix_ancestors(repo, txn, content_subspace, prefix) when byte_size(prefix) > 1 do
    # Check each prefix of our prefix to see if it exists as a key
    # For example, if prefix is <<1, 2, 3>>, check <<1>>, <<1, 2>>
    prefix_size = byte_size(prefix)

    Enum.all?(1..(prefix_size - 1), fn size ->
      ancestor_prefix = binary_part(prefix, 0, size)
      key = Subspace.key(content_subspace) <> ancestor_prefix

      # If this exact key exists, we have a collision
      txn |> repo.get(key) |> is_nil()
    end)
  end

  # Core operations implementation

  def do_create(%Layer{repo: repo} = layer, txn, path, opts) do
    key = node_key(layer, path)

    with :ok <- check_version(layer, txn, true),
         :ok <- ensure_version_initialized(layer, txn) do
      existing_value = repo.get(txn, key)

      case existing_value do
        nil ->
          if parent_exists?(layer, txn, path) do
            create_directory(layer, txn, path, key, opts)
          else
            {:error, :parent_directory_does_not_exist}
          end

        _value ->
          {:error, :directory_already_exists}
      end
    else
      {:error, _} = error -> error
    end
  end

  defp create_directory(%Layer{repo: repo} = layer, txn, path, key, opts) do
    with {:ok, prefix} <- allocate_or_validate_prefix(layer, txn, opts) do
      layer_id = Keyword.get(opts, :layer)
      version = Keyword.get(opts, :version)
      metadata = Keyword.get(opts, :metadata)

      # Store the directory metadata
      encoded_value = encode_node_value(prefix, layer_id, version, metadata)
      repo.put(txn, key, encoded_value)

      {:ok,
       %Node{
         prefix: prefix,
         path: layer.path ++ path,
         layer: layer_id,
         directory_layer: layer,
         version: version,
         metadata: metadata
       }}
    end
  end

  defp allocate_or_validate_prefix(layer, txn, opts) do
    case Keyword.get(opts, :prefix) do
      nil ->
        # Automatic allocation via HCA - uses nested transaction internally
        {:ok, layer.next_prefix_fn.()}

      prefix ->
        # Manual prefix assignment - check for collisions
        if is_prefix_free?(layer, txn, prefix) do
          {:ok, prefix}
        else
          {:error, :prefix_collision}
        end
    end
  end

  def do_open(%Layer{repo: repo} = layer, txn, path) do
    with true <- not root?(path) || {:error, :cannot_open_root},
         key = node_key(layer, path),
         {:ok, value} <- fetch_directory(repo, txn, key),
         :ok <- check_version(layer, txn, false) do
      value
      |> decode_node_value()
      |> case do
        {prefix, "partition", version, metadata} ->
          %Partition{
            directory_layer: layer,
            path: layer.path ++ path,
            prefix: prefix,
            version: version,
            metadata: metadata
          }

        {prefix, layer_id, version, metadata} ->
          %Node{
            prefix: prefix,
            path: layer.path ++ path,
            layer: layer_id,
            directory_layer: layer,
            version: version,
            metadata: metadata
          }
      end
      |> then(&{:ok, &1})
    end
  end

  defp fetch_directory(repo, txn, key) do
    case repo.get(txn, key) do
      nil -> {:error, :directory_does_not_exist}
      value -> {:ok, value}
    end
  end

  def do_exists?(%Layer{repo: repo} = layer, txn, path) do
    # Root directory always exists (it's virtual)
    if root?(path) do
      true
    else
      key = node_key(layer, path)

      case repo.get(txn, key) do
        nil -> false
        _value -> true
      end
    end
  end

  def do_list(%Layer{repo: repo} = layer, txn, path) do
    # Check version compatibility for reads
    case check_version(layer, txn, false) do
      {:error, _} = error ->
        error

      _ ->
        prefix_key = node_key(layer, path)
        prefix_size = byte_size(prefix_key)

        children =
          txn
          |> repo.range(KeyRange.from_prefix(prefix_key))
          |> Stream.map(&extract_child_name(&1, prefix_size))
          |> Stream.reject(&is_nil/1)
          |> Enum.uniq()

        {:ok, children}
    end
  end

  defp extract_child_name({key, _value}, prefix_size) when byte_size(key) <= prefix_size, do: nil

  defp extract_child_name({key, _value}, prefix_size) do
    remaining_size = byte_size(key) - prefix_size
    remaining = binary_part(key, prefix_size, remaining_size)

    case Key.unpack(remaining) do
      [child_name | _rest] -> child_name
      child_name when is_binary(child_name) -> child_name
      _ -> nil
    end
  end

  def do_remove(%Layer{repo: repo} = layer, txn, path) do
    with true <- not root?(path) || {:error, :cannot_remove_root},
         :ok <- check_version(layer, txn, true),
         :ok <- ensure_version_initialized(layer, txn),
         true <- do_exists?(layer, txn, path) || {:error, :directory_does_not_exist} do
      layer
      |> node_key(path)
      |> KeyRange.from_prefix()
      |> then(&repo.clear_range(txn, &1))

      :ok
    end
  end

  def do_move(%Layer{} = layer, txn, old_path, new_path) do
    with true <- not root?(old_path) || {:error, :cannot_move_root},
         true <- not root?(new_path) || {:error, :cannot_move_to_root},
         true <- not List.starts_with?(new_path, old_path) || {:error, :cannot_move_to_subdirectory},
         :ok <- check_version(layer, txn, true),
         :ok <- ensure_version_initialized(layer, txn),
         true <- do_exists?(layer, txn, old_path) || {:error, :directory_does_not_exist},
         true <- not do_exists?(layer, txn, new_path) || {:error, :directory_already_exists},
         true <- parent_exists?(layer, txn, new_path) || {:error, :parent_directory_does_not_exist} do
      do_move_recursive(layer, txn, old_path, new_path)
    end
  end

  defp parent_exists?(_layer, _txn, []), do: true
  defp parent_exists?(layer, txn, path), do: do_exists?(layer, txn, Enum.drop(path, -1))

  defp do_move_recursive(%Layer{repo: repo} = layer, txn, old_path, new_path) do
    old_key = node_key(layer, old_path)

    with {:ok, value} <- fetch_directory(repo, txn, old_key),
         :ok <- check_not_partition(value) do
      key_range = KeyRange.from_prefix(old_key)

      txn
      |> repo.range(key_range)
      |> Enum.each(fn {old_key, value} ->
        relative_path = extract_relative_path(layer, old_key, old_path)
        new_key = build_moved_key(layer, new_path, relative_path)
        repo.put(txn, new_key, value)
      end)

      repo.clear_range(txn, key_range)
      :ok
    end
  end

  defp check_not_partition(value) do
    {_prefix, layer_type, _version, _metadata} = decode_node_value(value)

    if layer_type == "partition" do
      {:error, :cannot_move_partition}
    else
      :ok
    end
  end

  defp extract_relative_path(%Layer{} = layer, key, base_path) do
    node_subspace_prefix = Subspace.key(layer.node_subspace)

    case key do
      <<^node_subspace_prefix::binary, remaining::binary>> ->
        remaining
        |> unpack_key_remaining()
        |> normalize_path_to_list()
        |> compute_relative_to_base(base_path)

      _ ->
        []
    end
  end

  defp unpack_key_remaining(<<>>), do: []
  defp unpack_key_remaining(packed), do: Key.unpack(packed)

  defp normalize_path_to_list(list) when is_list(list), do: list
  defp normalize_path_to_list(single) when is_binary(single), do: [single]
  defp normalize_path_to_list(_), do: []

  defp compute_relative_to_base(full_path_list, base_path) do
    cond do
      full_path_list == base_path -> []
      List.starts_with?(full_path_list, base_path) -> Enum.drop(full_path_list, length(base_path))
      true -> []
    end
  end

  defp build_moved_key(%Layer{} = layer, new_base_path, []), do: node_key(layer, new_base_path)

  defp build_moved_key(%Layer{} = layer, new_base_path, relative_path),
    do: node_key(layer, new_base_path ++ relative_path)
end
