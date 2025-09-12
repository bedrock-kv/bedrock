defmodule Bedrock.Directory.Partition do
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

defimpl Bedrock.Directory, for: Bedrock.Directory.Partition do
  alias Bedrock.Directory
  alias Bedrock.Directory.Partition
  alias Bedrock.Key
  alias Bedrock.Subspace

  # Partitions provide isolated namespaces - operations are scoped to the partition
  def create(%Partition{directory_layer: layer}, path, opts) do
    validate_within_partition!(path)
    Directory.create(layer, path, opts)
  end

  def open(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    Directory.open(layer, path)
  end

  def create_or_open(%Partition{directory_layer: layer}, path, opts) do
    validate_within_partition!(path)
    Directory.create_or_open(layer, path, opts)
  end

  def move(%Partition{directory_layer: layer}, old_path, new_path) do
    validate_within_partition!(old_path)
    validate_within_partition!(new_path)
    Directory.move(layer, old_path, new_path)
  end

  def remove(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    Directory.remove(layer, path)
  end

  def remove_if_exists(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    Directory.remove_if_exists(layer, path)
  end

  def list(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    Directory.list(layer, path)
  end

  def exists?(%Partition{directory_layer: layer}, path) do
    validate_within_partition!(path)
    Directory.exists?(layer, path)
  end

  def get_path(%Partition{path: path}), do: path
  def get_layer(%Partition{}), do: "partition"
  def get_subspace(%Partition{prefix: prefix}), do: Subspace.new(prefix)
  def range(%Partition{prefix: prefix}), do: Key.to_range(prefix)

  # Ensure operations don't escape the partition boundary
  defp validate_within_partition!(path) when is_list(path) do
    # Paths starting with ".." or containing ".." are trying to escape
    if Enum.any?(path, &(&1 == ".." or String.starts_with?(&1, "../"))) do
      raise ArgumentError, "Cannot access path outside partition boundary"
    end

    :ok
  end

  defp validate_within_partition!(_), do: raise(ArgumentError, "Invalid path format for partition operation")
end
