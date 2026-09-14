defmodule Bedrock.ObjectStorage.Config do
  @moduledoc "Configuration helpers for ObjectStorage backends."

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.S3

  @doc """
  The backend a cluster's durable state lives in: worker chunks and
  snapshots, and the bootstrap the director writes and the coordinator
  cold-boots from. Every one of those resolves it here, so no config can
  split that state across backends: `object_storage:` at the top level of
  the node config (the `:s3` shorthand works too: `object_storage: :s3,
  s3: [...]`), else a `LocalFilesystem` at `<first role path>/object_storage`.
  Returns `nil` when neither applies.

  An application-config `:backend` is refused rather than consulted.
  """
  @spec cluster_backend(node_config :: keyword()) :: ObjectStorage.backend() | nil
  def cluster_backend(node_config) do
    if Keyword.has_key?(config(), :backend) do
      raise "Bedrock: `config :bedrock, Bedrock.ObjectStorage, backend: ...` is no longer read; set " <>
              "`object_storage:` at the top level of the node config instead. That setting reached only " <>
              "materializer snapshots: the bootstrap and chunks were written to `<path>/object_storage`, " <>
              "so relocate existing data deliberately before pointing the cluster at a different store."
    end

    cond do
      Keyword.has_key?(node_config, :object_storage) ->
        normalize_backend(Keyword.fetch!(node_config, :object_storage), node_config)

      path = role_path(node_config) ->
        ObjectStorage.backend(LocalFilesystem, root: Path.join(path, "object_storage"))

      true ->
        nil
    end
  end

  defp role_path(node_config) do
    Enum.find_value([:coordinator, :log, :storage, :materializer, :coordination, :worker], fn role ->
      node_config |> Keyword.get(role, []) |> Keyword.get(:path)
    end)
  end

  @spec config() :: keyword()
  def config, do: Application.get_env(:bedrock, ObjectStorage, [])

  defp normalize_backend({:s3, backend_config}, app_config) when is_list(backend_config) do
    normalize_backend({S3, backend_config}, app_config)
  end

  defp normalize_backend(:s3, app_config) do
    normalize_backend({S3, Keyword.get(app_config, :s3, [])}, app_config)
  end

  defp normalize_backend({:local_filesystem, backend_config}, _app_config) when is_list(backend_config) do
    ObjectStorage.backend(LocalFilesystem, backend_config)
  end

  defp normalize_backend(:local_filesystem, app_config) do
    ObjectStorage.backend(LocalFilesystem, Keyword.get(app_config, :local_filesystem, []))
  end

  defp normalize_backend({module, backend_config}, app_config) when is_atom(module) and is_list(backend_config) do
    ObjectStorage.backend(module, normalize_module_config(module, backend_config, app_config))
  end

  defp normalize_backend(module, app_config) when is_atom(module) do
    normalize_backend({module, []}, app_config)
  end

  defp normalize_module_config(S3, backend_config, app_config) do
    merged = Keyword.merge(Keyword.get(app_config, :s3, []), backend_config)

    {bucket, merged} = Keyword.pop(merged, :bucket)
    {explicit_request_config, merged} = Keyword.pop(merged, :config, [])

    request_config =
      explicit_request_config
      |> Keyword.merge(merged)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    Enum.reject([bucket: bucket, config: request_config], fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_module_config(_module, backend_config, _app_config), do: backend_config
end
