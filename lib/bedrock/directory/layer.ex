defmodule Bedrock.Directory.Layer do
  @moduledoc """
  Main implementation of the FoundationDB directory layer.

  Manages directory hierarchy and metadata storage using the same
  key/value layout as FoundationDB's directory layer.
  """

  alias Bedrock.Directory.Node
  alias Bedrock.Directory.Partition
  alias Bedrock.Key
  alias Bedrock.KeyRange
  alias Bedrock.Subspace

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
          next_prefix_fn: (-> binary()),
          path: [String.t()]
        }

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
  @spec new(module(), keyword()) :: t()
  def new(repo, opts \\ []) when is_atom(repo) do
    # Default HCA-based prefix allocation
    hca = Bedrock.HCA.new(repo, @content_subspace_prefix)

    next_prefix_fn =
      opts[:next_prefix_fn] ||
        fn ->
          {:ok, prefix} = Bedrock.HCA.allocate(hca)
          prefix
        end

    node_subspace = Keyword.get(opts, :node_subspace, Subspace.new(@node_subspace_prefix))
    content_subspace = Keyword.get(opts, :content_subspace, Subspace.new(@content_subspace_prefix))

    %__MODULE__{
      node_subspace: node_subspace,
      content_subspace: content_subspace,
      repo: repo,
      next_prefix_fn: next_prefix_fn,
      path: []
    }
  end

  # Helpers for path validation
  @doc false
  def validate_path(path) do
    path
    |> normalize_path()
    |> do_validate_path()
  end

  # Normalize various input formats to a list
  defp normalize_path(path) when is_list(path), do: {:ok, path}
  defp normalize_path(path) when is_binary(path), do: {:ok, [path]}
  defp normalize_path(path) when is_tuple(path), do: {:ok, Tuple.to_list(path)}
  defp normalize_path(_), do: {:error, :invalid_path_format}

  defp do_validate_path({:error, _} = error), do: error
  defp do_validate_path({:ok, []}), do: :ok

  defp do_validate_path({:ok, [component | rest]}) do
    cond do
      not is_binary(component) ->
        {:error, :invalid_path_component}

      byte_size(component) == 0 ->
        {:error, :empty_path_component}

      # Check for reserved bytes before UTF-8 validation
      # (reserved bytes might not be valid UTF-8)
      byte_size(component) > 0 and
          (binary_part(component, 0, 1) == <<0xFE>> or binary_part(component, 0, 1) == <<0xFF>>) ->
        {:error, :reserved_prefix_in_path}

      not String.valid?(component) ->
        {:error, :invalid_utf8_in_path}

      String.contains?(component, <<0>>) ->
        {:error, :null_byte_in_path}

      true ->
        do_validate_path({:ok, rest})
    end
  end

  # Root directory helpers
  @doc false
  def root?([]), do: true
  def root?(_), do: false

  # Key encoding for node metadata
  defp node_key(%__MODULE__{node_subspace: node_subspace, path: base_path}, path) do
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

  defp check_version(%__MODULE__{repo: repo} = layer, txn, allow_writes) do
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

  defp ensure_version_initialized(%__MODULE__{repo: repo} = layer, txn) do
    # Version key is at the root of the node subspace
    version_key = Subspace.pack(layer.node_subspace, [@version_key])

    case repo.get(txn, version_key) do
      nil -> init_version(layer, txn)
      _ -> :ok
    end
  end

  defp init_version(%__MODULE__{repo: repo} = layer, txn) do
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

  @doc false
  def is_prefix_free?(%__MODULE__{repo: repo, content_subspace: content_subspace}, txn, prefix)
      when is_binary(prefix) do
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

  @doc false
  def do_create(%__MODULE__{repo: repo} = layer, txn, path, opts) do
    key = node_key(layer, path)

    with :ok <- check_version(layer, txn, true),
         :ok <- ensure_version_initialized(layer, txn),
         nil <- repo.get(txn, key),
         true <- parent_exists?(layer, txn, path) || {:error, :parent_directory_does_not_exist} do
      create_directory(layer, txn, path, key, opts)
    else
      {:error, _} = error -> error
      _existing_value -> {:error, :directory_already_exists}
    end
  end

  defp create_directory(%__MODULE__{repo: repo} = layer, txn, path, key, opts) do
    with {:ok, prefix} <- allocate_or_validate_prefix(layer, txn, opts) do
      layer_id = Keyword.get(opts, :layer)
      version = Keyword.get(opts, :version)
      metadata = Keyword.get(opts, :metadata)

      # Store the directory metadata
      prefix
      |> encode_node_value(layer_id, version, metadata)
      |> then(&repo.put(txn, key, &1))

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
        # Automatic allocation via HCA
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

  @doc false
  def do_open(%__MODULE__{repo: repo} = layer, txn, path) do
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

  @doc false
  def do_exists?(%__MODULE__{repo: repo} = layer, txn, path) do
    key = node_key(layer, path)

    case repo.get(txn, key) do
      nil -> false
      _value -> true
    end
  end

  @doc false
  def do_list(%__MODULE__{repo: repo} = layer, txn, path) do
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

  @doc false
  def do_remove(%__MODULE__{repo: repo} = layer, txn, path) do
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

  @doc false
  def do_move(%__MODULE__{} = layer, txn, old_path, new_path) do
    with true <- not root?(old_path) || {:error, :cannot_move_root},
         true <- not root?(new_path) || {:error, :cannot_move_to_root},
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

  defp do_move_recursive(%__MODULE__{repo: repo} = layer, txn, old_path, new_path) do
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

  defp extract_relative_path(%__MODULE__{} = layer, key, base_path) do
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

  defp build_moved_key(%__MODULE__{} = layer, new_base_path, []), do: node_key(layer, new_base_path)

  defp build_moved_key(%__MODULE__{} = layer, new_base_path, relative_path),
    do: node_key(layer, new_base_path ++ relative_path)
end

defimpl Bedrock.Directory, for: Bedrock.Directory.Layer do
  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node

  def create(%Layer{repo: repo} = layer, path, opts) do
    with :ok <- Layer.validate_path(path) do
      repo.transaction(fn txn -> Layer.do_create(layer, txn, path, opts) end)
    end
  end

  def open(%Layer{repo: repo} = layer, path) do
    with :ok <- Layer.validate_path(path) do
      repo.transaction(fn txn -> Layer.do_open(layer, txn, path) end)
    end
  end

  def create_or_open(%Layer{repo: repo} = layer, path, opts) do
    repo.transaction(fn txn ->
      case Layer.do_open(layer, txn, path) do
        {:ok, node} -> {:ok, node}
        {:error, :directory_does_not_exist} -> Layer.do_create(layer, txn, path, opts)
      end
    end)
  end

  def move(%Layer{repo: repo} = layer, old_path, new_path),
    do: repo.transaction(fn txn -> Layer.do_move(layer, txn, old_path, new_path) end)

  def remove(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> Layer.do_remove(layer, txn, path) end)

  def remove_if_exists(%Layer{repo: repo} = layer, path) do
    repo.transaction(fn txn ->
      case Layer.do_remove(layer, txn, path) do
        :ok -> :ok
        {:error, :directory_does_not_exist} -> :ok
        {:error, :cannot_remove_root} = error -> error
        error -> error
      end
    end)
  end

  def list(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> Layer.do_list(layer, txn, path) end)

  def exists?(%Layer{repo: repo} = layer, path), do: repo.transaction(fn txn -> Layer.do_exists?(layer, txn, path) end)

  def get_path(%Layer{path: path}), do: path

  def get_layer(%Layer{}), do: nil

  def get_subspace(%Layer{}) do
    raise ArgumentError, "Cannot get subspace from directory layer - use open/create first"
  end
end
