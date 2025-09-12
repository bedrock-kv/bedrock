defmodule Bedrock.Directory.Node do
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

defimpl Bedrock.Directory, for: Bedrock.Directory.Node do
  alias Bedrock.Directory
  alias Bedrock.Directory.Node
  alias Bedrock.Key
  alias Bedrock.Subspace

  def create(%Node{directory_layer: layer}, path, opts), do: Directory.create(layer, path, opts)
  def open(%Node{directory_layer: layer}, path), do: Directory.open(layer, path)
  def create_or_open(%Node{directory_layer: layer}, path, opts), do: Directory.create_or_open(layer, path, opts)
  def move(%Node{directory_layer: layer}, old_path, new_path), do: Directory.move(layer, old_path, new_path)
  def remove(%Node{directory_layer: layer}, path), do: Directory.remove(layer, path)
  def remove_if_exists(%Node{directory_layer: layer}, path), do: Directory.remove_if_exists(layer, path)
  def list(%Node{directory_layer: layer}, path), do: Directory.list(layer, path)
  def exists?(%Node{directory_layer: layer}, path), do: Directory.exists?(layer, path)
  def get_path(%Node{path: path}), do: path
  def get_layer(%Node{layer: layer}), do: layer
  def get_subspace(%Node{prefix: prefix}), do: Subspace.new(prefix)
  def range(%Node{prefix: prefix}), do: Key.to_range(prefix)
end
