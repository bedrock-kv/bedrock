defmodule Bedrock.Service.Foreman.WorkingDirectory do
  @moduledoc false
  import Bedrock.Service.Manifest, only: [load_from_file: 1]

  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  @manifest_file_name "manifest.json"

  @doc """
  The manifest's path within a worker's working directory.

  The manifest is what makes a directory a worker directory: the foreman's
  path also holds `object_storage/` and `raft/`, so enumeration keys off
  this file's presence. Writer and reader must therefore agree on the
  name exactly — hence one definition.
  """
  @spec manifest_path(Path.t()) :: Path.t()
  def manifest_path(working_directory), do: Path.join(working_directory, @manifest_file_name)

  @spec initialize_working_directory(Path.t(), Manifest.t()) ::
          :ok | {:error, File.posix()}
  def initialize_working_directory(working_directory, manifest) do
    path_to_manifest = manifest_path(working_directory)

    with :ok <- File.mkdir_p(working_directory),
         :ok <- manifest.worker.one_time_initialization(working_directory) do
      Manifest.write_to_file(manifest, path_to_manifest)
    end
  end

  @spec read_and_validate_manifest(Path.t(), Worker.id(), cluster_name :: String.t()) ::
          {:ok, Manifest.t()}
          | {:error,
             :cluster_name_in_manifest_does_not_match
             | :id_in_manifest_does_not_match
             | :invalid_cluster_id
             | :invalid_cluster_name
             | :invalid_worker_name
             | :manifest_does_not_exist
             | :manifest_is_invalid
             | :manifest_is_not_a_dictionary
             | :worker_module_does_not_exist
             | :worker_module_does_not_implement_behaviour
             | :worker_module_failed_to_load
             | :worker_module_is_invalid}
  def read_and_validate_manifest(path, worker_id, cluster_name) do
    with {:ok, manifest} <- load_from_file(manifest_path(path)),
         :ok <- check_manifest_id(manifest, worker_id),
         :ok <- check_manifest_cluster_name(manifest, cluster_name) do
      {:ok, manifest}
    end
  end

  @spec check_manifest_id(manifest :: Manifest.t(), id :: Worker.id()) ::
          :ok | {:error, :id_in_manifest_does_not_match}
  defp check_manifest_id(%{id: id}, id), do: :ok
  defp check_manifest_id(_, _), do: {:error, :id_in_manifest_does_not_match}

  @spec check_manifest_cluster_name(manifest :: Manifest.t(), cluster_name :: String.t()) ::
          :ok | {:error, :cluster_name_in_manifest_does_not_match}
  defp check_manifest_cluster_name(%{cluster: cluster_name}, cluster_name), do: :ok
  defp check_manifest_cluster_name(_, _), do: {:error, :cluster_name_in_manifest_does_not_match}
end
