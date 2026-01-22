defmodule Bedrock.ObjectStorage.Config do
  @moduledoc "Configuration helpers for ObjectStorage backends."

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  @spec backend() :: ObjectStorage.backend()
  def backend do
    config()
    |> Keyword.get_lazy(:backend, &default_backend/0)
    |> normalize_backend()
  end

  @spec bootstrap_key() :: String.t() | nil
  def bootstrap_key, do: Keyword.get(config(), :bootstrap_key)

  @spec bootstrap_key!() :: String.t()
  def bootstrap_key! do
    bootstrap_key() || raise "ObjectStorage bootstrap_key not configured"
  end

  @spec config() :: keyword()
  def config, do: Application.get_env(:bedrock, ObjectStorage, [])

  defp default_backend, do: {LocalFilesystem, root: Path.join(System.tmp_dir!(), "bedrock_objects")}

  defp normalize_backend({module, config}) when is_atom(module) and is_list(config),
    do: ObjectStorage.backend(module, config)

  defp normalize_backend(module) when is_atom(module), do: ObjectStorage.backend(module, [])
end
