defmodule Bedrock.ObjectStorage.Config do
  @moduledoc "Configuration helpers for ObjectStorage backends."

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.DiskCache
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.S3

  @spec backend() :: ObjectStorage.backend()
  def backend do
    app_config = config()
    backend_config = Keyword.get_lazy(app_config, :backend, &default_backend/0)

    normalize_backend(backend_config, app_config)
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

  defp normalize_backend({:disk_cache, backend_config}, app_config) when is_list(backend_config) do
    normalize_backend({DiskCache, backend_config}, app_config)
  end

  defp normalize_backend(:disk_cache, app_config) do
    normalize_backend({DiskCache, Keyword.get(app_config, :disk_cache, [])}, app_config)
  end

  defp normalize_backend({module, backend_config}, app_config) when is_atom(module) and is_list(backend_config) do
    ObjectStorage.backend(module, normalize_module_config(module, backend_config, app_config))
  end

  defp normalize_backend(module, app_config) when is_atom(module) do
    normalize_backend({module, []}, app_config)
  end

  # The cache wraps a backend, so its `:inner` is itself something to
  # normalize — that is what lets `inner: :s3` pick up the top-level `:s3`
  # config rather than having to be spelled out again.
  defp normalize_module_config(DiskCache, backend_config, app_config) do
    Keyword.update!(backend_config, :inner, &normalize_backend(&1, app_config))
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
