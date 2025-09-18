defmodule Bedrock.Directory do
  @moduledoc """
  FoundationDB directory layer operations.

  Directories provide a hierarchical namespace for organizing data within
  a FoundationDB database. Each directory corresponds to a unique prefix
  that can be used to create isolated keyspaces.

  Based on the FoundationDB directory layer specification.
  """

  alias Bedrock.HighContentionAllocator, as: HCA
  alias Bedrock.KeyRange
  alias Bedrock.Keyspace

  # Struct definitions

  defmodule Node do
    @moduledoc """
    A directory node returned by directory operations.

    Contains the metadata and prefix for a directory, and can generate
    a keyspace for data storage within the directory.
    """

    @type t :: %__MODULE__{
            prefix: binary(),
            path: [String.t()],
            layer: binary() | nil,
            directory_layer: Bedrock.Directory.Layer.t(),
            version: term() | nil,
            metadata: term() | nil
          }
    defstruct [:prefix, :path, :layer, :directory_layer, :version, :metadata]

    defimpl String.Chars do
      def to_string(%{path: path, layer: layer}) do
        path_str = Enum.join(path, "/")
        layer_suffix = if layer && layer != "", do: "@#{inspect(layer)}", else: ""
        "Directory<Node|path:#{path_str}#{layer_suffix}>"
      end
    end

    defimpl Inspect do
      def inspect(%{path: path, layer: layer, prefix: prefix}, _opts) do
        path_str = Enum.join(path, "/")
        layer_suffix = if layer && layer != "", do: "@#{inspect(layer)}", else: ""
        "#Directory<Node|path:#{path_str}#{layer_suffix},prefix:0x#{:binary.encode_hex(prefix, :lowercase)}>"
      end
    end
  end

  defmodule Partition do
    @moduledoc """
    A directory partition provides an isolated namespace within a directory.

    Partitions have their own prefix allocation and prevent operations
    outside their boundary, providing isolation between different parts
    of an application.
    """

    @type t :: %__MODULE__{
            directory_layer: Bedrock.Directory.Layer.t(),
            path: [String.t()],
            prefix: binary(),
            version: term() | nil,
            metadata: term() | nil
          }
    defstruct [:directory_layer, :path, :prefix, :version, :metadata]

    defimpl String.Chars do
      def to_string(%{path: path}), do: "Directory<Partition|#{Enum.join(path, "/")}>"
    end

    defimpl Inspect do
      def inspect(%Partition{path: path, prefix: prefix}, _opts),
        do: "#Directory<Partition|path:#{Enum.join(path, "/")},prefix:0x#{:binary.encode_hex(prefix, :lowercase)}>"
    end
  end

  defmodule Layer do
    @moduledoc """
    Main implementation of the FoundationDB directory layer.

    Manages directory hierarchy and metadata storage using the same
    key/value layout as FoundationDB's directory layer.
    """

    @type t :: %__MODULE__{
            node_keyspace: Keyspace.t(),
            content_keyspace: Keyspace.t(),
            repo: module(),
            next_prefix_fn: (-> binary()),
            path: [String.t()]
          }
    defstruct [:node_keyspace, :content_keyspace, :repo, :next_prefix_fn, :path]

    defimpl String.Chars do
      def to_string(%{path: path}), do: "Directory<Layer|#{Enum.join(path, "/")}>"
    end

    defimpl Inspect do
      def inspect(%{path: path, repo: repo}, _opts),
        do: "#Directory<Layer|path:#{Enum.join(path, "/")},repo:#{inspect(repo)}>"
    end
  end

  defmodule NodeKey do
    @moduledoc false
    def pack([]), do: <<>>
    def pack(path) when is_list(path), do: Bedrock.Encoding.Tuple.pack(path)

    def unpack(<<>>), do: []
    def unpack(packed), do: Bedrock.Encoding.Tuple.unpack(packed)
  end

  @type directory :: Node.t() | Partition.t() | Layer.t()

  # System keyspace prefixes - match FDB exactly
  @node_keyspace_prefix <<0xFE>>
  @content_keyspace_prefix <<>>

  # Reserved prefix ranges - match FDB system prefixes
  @reserved_prefixes [
    # Node keyspace
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

  - `:next_prefix_fn` - Function for prefix allocation (default: uses HighContentionAllocator)
  - `:node_keyspace` - Custom node storage location
  - `:content_keyspace` - Custom content storage location
  """
  @spec root(module(), keyword()) :: Node.t()
  def root(repo, opts \\ []) when is_atom(repo) do
    %Keyspace{} =
      node_keyspace =
      Keyword.get(
        opts,
        :node_keyspace,
        Keyspace.new(@node_keyspace_prefix, key_encoding: NodeKey, value_encoding: Bedrock.Encoding.Tuple)
      )

    %Keyspace{} = content_keyspace = Keyword.get(opts, :content_keyspace, Keyspace.new(@content_keyspace_prefix))

    hca = HCA.new(repo, @content_keyspace_prefix)

    next_prefix_fn =
      opts[:next_prefix_fn] ||
        fn ->
          case HCA.allocate(hca) do
            {:ok, prefix} -> prefix
            {:error, reason} -> raise "Failed to allocate prefix: #{inspect(reason)}"
          end
        end

    %Node{
      prefix: Keyspace.prefix(content_keyspace),
      path: [],
      layer: nil,
      directory_layer: %Layer{
        node_keyspace: node_keyspace,
        content_keyspace: content_keyspace,
        repo: repo,
        next_prefix_fn: next_prefix_fn,
        path: []
      },
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
  def create(%Node{directory_layer: layer}, path, opts), do: create(layer, path, opts)

  def create(%Partition{directory_layer: layer}, path, opts) do
    validate_within_partition!(path)
    create(layer, path, opts)
  end

  # Layer implementation - performs actual operations
  def create(%Layer{repo: repo} = layer, path, opts) do
    with :ok <- validate_path(path),
         true <- not root?(path) || {:error, :cannot_create_root} do
      repo.transact(fn -> do_create(layer, nil, path, opts) end)
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
      repo.transact(fn -> do_open(layer, nil, path) end)
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
      repo.transact(fn ->
        case do_open(layer, nil, path) do
          {:ok, node} -> {:ok, node}
          {:error, :directory_does_not_exist} -> do_create(layer, nil, path, opts)
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
    do: repo.transact(fn -> do_move(layer, nil, old_path, new_path) end)

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

  def remove(%Layer{repo: repo} = layer, path), do: repo.transact(fn -> do_remove(layer, nil, path) end)

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
    repo.transact(fn ->
      case do_remove(layer, nil, path) do
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

  def list(%Layer{repo: repo} = layer, path), do: repo.transact(fn -> do_list(layer, nil, path) end)

  @doc """
  Checks if a directory exists at the given path.
  """
  @spec exists?(directory(), [String.t()]) :: boolean()
  def exists?(%Node{directory_layer: layer}, path), do: exists?(layer, path)

  def exists?(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    exists?(layer, path)
  end

  def exists?(%Layer{repo: repo} = layer, path), do: repo.transact(fn -> do_exists?(layer, path) end)

  @doc """
  Returns the path of this directory as a list of strings.
  """
  @spec path(directory()) :: [String.t()]
  def path(%Node{path: path}), do: path
  def path(%Partition{path: path}), do: path
  def path(%Layer{path: path}), do: path

  @doc """
  Returns the layer identifier of this directory, or `nil` if none.
  """
  @spec layer(directory()) :: binary() | nil
  def layer(%Node{layer: layer}), do: layer
  def layer(%Partition{}), do: "partition"
  def layer(%Layer{}), do: nil

  @doc """
  Returns a `Bedrock.Keyspace` for this directory.

  The keyspace can be used to store and retrieve data within
  this directory's keyspace.
  """
  @spec to_keyspace(directory(), opts :: [key_encoding: module(), value_encoding: module()]) :: Keyspace.t()
  def to_keyspace(directory, opts \\ [])

  def to_keyspace(%Node{prefix: prefix}, opts), do: Keyspace.new(prefix, opts)
  def to_keyspace(%Partition{prefix: prefix}, opts), do: Keyspace.new(prefix, opts)

  def to_keyspace(%Layer{}, _opts),
    do: raise(ArgumentError, "Cannot get keyspace from directory layer - use open/create first")

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

  # Encode node metadata value - must match FDB format
  defp encode_node_value(prefix, layer, nil, nil), do: {prefix, layer || ""}
  defp encode_node_value(prefix, layer, version, nil), do: {prefix, layer || "", version}
  defp encode_node_value(prefix, layer, nil, metadata), do: {prefix, layer || "", nil, metadata}
  defp encode_node_value(prefix, layer, version, metadata), do: {prefix, layer || "", version, metadata}

  defp decode_node_value(nil), do: nil

  defp decode_node_value(value) do
    case value do
      {prefix, <<>>} -> {prefix, nil, nil, nil}
      {prefix, layer} -> {prefix, layer, nil, nil}
      {prefix, <<>>, version} -> {prefix, nil, version, nil}
      {prefix, layer, version} -> {prefix, layer, version, nil}
      {prefix, <<>>, version, metadata} -> {prefix, nil, version, metadata}
      {prefix, layer, version, metadata} -> {prefix, layer, version, metadata}
    end
  end

  # Version management implementation

  defp check_version(%Layer{repo: repo} = layer, allow_writes) do
    case repo.get(layer.node_keyspace, [@version_key]) do
      nil ->
        # No version stored yet - writing operations will initialize
        :ok

      stored_version_binary ->
        stored_version = decode_version(stored_version_binary)
        compare_versions(stored_version, @layer_version, allow_writes)
    end
  end

  defp ensure_version_initialized(%Layer{repo: repo} = layer, _txn) do
    case repo.get(layer.node_keyspace, [@version_key]) do
      nil -> init_version(layer)
      _ -> :ok
    end
  end

  defp init_version(%Layer{repo: repo} = layer),
    do: repo.put(layer.node_keyspace, [@version_key], encode_version(@layer_version))

  defp encode_version({major, minor, patch}), do: <<major::little-32, minor::little-32, patch::little-32>>
  defp decode_version(<<major::little-32, minor::little-32, patch::little-32>>), do: {major, minor, patch}

  defp compare_versions({stored_major, _, _}, {current_major, _, _}, _) when stored_major > current_major,
    do: {:error, :incompatible_directory_version}

  defp compare_versions({stored_major, stored_minor, _}, {current_major, current_minor, _}, true)
       when stored_major == current_major and stored_minor > current_minor,
       do: {:error, :directory_version_write_restricted}

  defp compare_versions(_, _, _), do: :ok

  # Prefix collision detection

  def is_prefix_free?(%Layer{repo: repo, content_keyspace: content_keyspace}, _txn, prefix) when is_binary(prefix) do
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
      # The content keyspace prefix combined with the directory prefix
      full_prefix = Keyspace.prefix(content_keyspace) <> prefix
      prefix_range = KeyRange.from_prefix(full_prefix)

      case repo.get_range(prefix_range, limit: 1) do
        [] ->
          # No keys start with our prefix, now check the reverse
          # Are there any existing keys that our prefix starts with?
          check_prefix_ancestors(repo, content_keyspace, prefix)

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

  defp check_prefix_ancestors(_repo, _content_keyspace, <<>>), do: true
  defp check_prefix_ancestors(_repo, _content_keyspace, <<_>>), do: true

  defp check_prefix_ancestors(repo, content_keyspace, prefix) when byte_size(prefix) > 1 do
    # Check each prefix of our prefix to see if it exists as a key
    # For example, if prefix is <<1, 2, 3>>, check <<1>>, <<1, 2>>
    prefix_size = byte_size(prefix)

    Enum.all?(1..(prefix_size - 1), fn size ->
      ancestor_prefix = binary_part(prefix, 0, size)
      key = Keyspace.prefix(content_keyspace) <> ancestor_prefix

      # If this exact key exists, we have a collision
      key |> repo.get() |> is_nil()
    end)
  end

  # Core operations implementation

  def do_create(%Layer{repo: repo} = layer, txn, path, opts) do
    with :ok <- check_version(layer, true),
         :ok <- ensure_version_initialized(layer, txn) do
      existing_value = repo.get(layer.node_keyspace, path)

      case existing_value do
        nil ->
          if parent_exists?(layer, path) do
            create_directory(layer, txn, path, opts)
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

  defp create_directory(%Layer{repo: repo} = layer, txn, path, opts) do
    with {:ok, prefix} <- allocate_or_validate_prefix(layer, txn, opts) do
      layer_id = Keyword.get(opts, :layer)
      version = Keyword.get(opts, :version)
      metadata = Keyword.get(opts, :metadata)

      # Store the directory metadata
      encoded_value = encode_node_value(prefix, layer_id, version, metadata)
      repo.put(layer.node_keyspace, path, encoded_value)

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
        # Automatic allocation via HighContentionAllocator
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

  def do_open(%Layer{repo: repo} = layer, _txn, path) do
    with true <- not root?(path) || {:error, :cannot_open_root},
         value = repo.get(layer.node_keyspace, path),
         :ok <- check_version(layer, false) do
      value
      |> decode_node_value()
      |> case do
        nil ->
          {:error, :directory_does_not_exist}

        {prefix, "partition", version, metadata} ->
          {:ok,
           %Partition{
             directory_layer: layer,
             path: layer.path ++ path,
             prefix: prefix,
             version: version,
             metadata: metadata
           }}

        {prefix, layer_id, version, metadata} ->
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
  end

  defp fetch_directory(%Layer{repo: repo} = layer, path) do
    case repo.get(layer.node_keyspace, path) do
      nil -> {:error, :directory_does_not_exist}
      value -> {:ok, value}
    end
  end

  def do_exists?(%Layer{repo: repo} = layer, path) do
    # Root directory always exists (it's virtual)
    if root?(path) do
      true
    else
      case repo.get(layer.node_keyspace, path) do
        nil -> false
        _value -> true
      end
    end
  end

  def do_list(%Layer{repo: repo} = layer, _txn, path) do
    # Check version compatibility for reads
    case check_version(layer, false) do
      {:error, _} = error ->
        error

      _ ->
        path_depth = length(path)

        children =
          layer.node_keyspace
          |> Keyspace.partition(path)
          |> Keyspace.prefix()
          |> repo.get_range()
          |> Stream.map(&extract_child_name(&1, layer.node_keyspace, path_depth))
          |> Stream.reject(&is_nil/1)
          |> Enum.uniq()

        {:ok, children}
    end
  end

  defp extract_child_name({key, _value}, keyspace, path_depth) do
    case Keyspace.unpack(keyspace, key) do
      full_path when length(full_path) > path_depth ->
        full_path |> Enum.drop(path_depth) |> List.first()

      _ ->
        nil
    end
  end

  def do_remove(%Layer{repo: repo} = layer, txn, path) do
    with true <- not root?(path) || {:error, :cannot_remove_root},
         :ok <- check_version(layer, true),
         :ok <- ensure_version_initialized(layer, txn),
         true <- do_exists?(layer, path) || {:error, :directory_does_not_exist} do
      # Create a subkeyspace for this path to clear all its children
      path_key = Keyspace.pack(layer.node_keyspace, path)
      path_range = KeyRange.from_prefix(path_key)
      repo.clear_range(path_range)

      :ok
    end
  end

  def do_move(%Layer{} = layer, txn, old_path, new_path) do
    with true <- not root?(old_path) || {:error, :cannot_move_root},
         true <- not root?(new_path) || {:error, :cannot_move_to_root},
         true <- not List.starts_with?(new_path, old_path) || {:error, :cannot_move_to_subdirectory},
         :ok <- check_version(layer, true),
         :ok <- ensure_version_initialized(layer, txn),
         true <- do_exists?(layer, old_path) || {:error, :directory_does_not_exist},
         true <- not do_exists?(layer, new_path) || {:error, :directory_already_exists},
         true <- parent_exists?(layer, new_path) || {:error, :parent_directory_does_not_exist} do
      do_move_recursive(layer, txn, old_path, new_path)
    end
  end

  defp parent_exists?(_layer, []), do: true
  defp parent_exists?(layer, path), do: do_exists?(layer, Enum.drop(path, -1))

  defp do_move_recursive(%Layer{repo: repo} = layer, _txn, old_path, new_path) do
    with {:ok, value} <- fetch_directory(layer, old_path),
         :ok <- check_not_partition(value) do
      # Use keyspace range for efficient scanning
      {start_key, end_key} =
        layer.node_keyspace |> Keyspace.partition(old_path) |> Bedrock.ToKeyRange.to_key_range()

      start_key
      |> repo.get_range(end_key)
      |> Enum.each(fn {raw_key, packed_value} ->
        # Unpack the value since we're doing raw key scanning
        value = Bedrock.Encoding.Tuple.unpack(packed_value)
        # Extract the path from the raw key using keyspace
        old_unpacked_path = Keyspace.unpack(layer.node_keyspace, raw_key)
        relative_path = compute_relative_to_base(old_unpacked_path, old_path)
        new_full_path = new_path ++ relative_path
        repo.put(layer.node_keyspace, new_full_path, value)
      end)

      # Clear the range using keyspace range
      repo.clear_range(start_key, end_key)
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

  defp compute_relative_to_base(full_path_list, base_path) do
    cond do
      full_path_list == base_path -> []
      List.starts_with?(full_path_list, base_path) -> Enum.drop(full_path_list, length(base_path))
      true -> []
    end
  end

  # Protocol implementations for ToKeyRange
  defimpl Bedrock.ToKeyRange, for: Bedrock.Directory.Node do
    def to_key_range(%Bedrock.Directory.Node{prefix: prefix}), do: KeyRange.from_prefix(prefix)
  end

  defimpl Bedrock.ToKeyRange, for: Bedrock.Directory.Partition do
    def to_key_range(%Bedrock.Directory.Partition{prefix: prefix}), do: KeyRange.from_prefix(prefix)
  end

  defimpl Bedrock.ToKeyRange, for: Bedrock.Directory.Layer do
    def to_key_range(%Bedrock.Directory.Layer{}),
      do: raise(ArgumentError, "Cannot get key range from directory layer - use open/create first")
  end

  # ToKeyspace protocol implementations
  defimpl Bedrock.ToKeyspace, for: Bedrock.Directory.Node do
    def to_keyspace(%Bedrock.Directory.Node{prefix: prefix}), do: Keyspace.new(prefix)
  end

  defimpl Bedrock.ToKeyspace, for: Bedrock.Directory.Partition do
    def to_keyspace(%Bedrock.Directory.Partition{prefix: prefix}), do: Keyspace.new(prefix)
  end

  defimpl Bedrock.ToKeyspace, for: Bedrock.Directory.Layer do
    def to_keyspace(%Bedrock.Directory.Layer{}),
      do: raise(ArgumentError, "Cannot get keyspace from directory layer - use open/create first")
  end
end
