defmodule Bedrock.Service.Foreman.WorkingDirectory do
  @moduledoc false
  import Bedrock.Service.Manifest, only: [load_from_file: 1]

  alias Bedrock.Service.Manifest
  alias Bedrock.Service.RecoveryControl
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

  @spec initialize_working_directory(Path.t(), Manifest.t(), keyword()) ::
          :ok | {:error, File.posix()}
  def initialize_working_directory(working_directory, manifest, opts \\ []) do
    with :ok <- reject_symlink(working_directory, :worker_directory_is_symlink),
         :ok <- reject_symlink(manifest_path(working_directory), :manifest_is_symlink),
         :ok <- reject_symlink(RecoveryControl.path(working_directory), :control_record_is_symlink) do
      do_initialize_working_directory(working_directory, manifest, opts)
    end
  end

  defp do_initialize_working_directory(working_directory, manifest, opts) do
    case Manifest.load_from_file(manifest_path(working_directory)) do
      {:ok, existing} ->
        validate_existing_creation(existing, manifest)

      {:error, :manifest_does_not_exist} ->
        initialize_or_resume(working_directory, mark_protocol(manifest), opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec worker_path(Path.t(), Worker.id()) :: {:ok, Path.t()} | {:error, {:recovery_authority, :unsafe_worker_id}}
  def worker_path(root, id) when is_binary(id) do
    if id != "" and id not in [".", ".."] and Path.basename(id) == id and not String.contains?(id, ["/", "\\", <<0>>]) do
      {:ok, Path.join(root, id)}
    else
      {:error, {:recovery_authority, :unsafe_worker_id}}
    end
  end

  def worker_path(_, _), do: {:error, {:recovery_authority, :unsafe_worker_id}}

  defp initialize_or_resume(path, manifest, opts) do
    with :ok <- File.mkdir_p(path) do
      case RecoveryControl.load(path) do
        {:ok, record} ->
          if opts[:resume_incomplete_creation] == manifest.id do
            with :ok <- validate_control_creation(record, manifest),
                 true <- record.phase == :no_grant || {:error, {:recovery_authority, :unsafe_incomplete_creation}} do
              Manifest.write_to_file(manifest, manifest_path(path))
            end
          else
            {:error, {:recovery_authority, :incomplete_creation_requires_explicit_resume}}
          end

        {:error, :missing} ->
          with :ok <- manifest.worker.one_time_initialization(path),
               :ok <-
                 RecoveryControl.write(path, RecoveryControl.no_grant(manifest.cluster, manifest.id, manifest.worker)) do
            Manifest.write_to_file(manifest, manifest_path(path))
          end

        {:error, reason} ->
          {:error, {:recovery_authority, reason}}
      end
    end
  end

  defp validate_control_creation(record, manifest) do
    case RecoveryControl.validate_creation(record, manifest.cluster, manifest.id, manifest.worker) do
      :ok -> :ok
      {:error, reason} -> {:error, {:recovery_authority, reason}}
    end
  end

  defp validate_existing_creation(existing, requested) do
    if {existing.cluster, existing.id, existing.worker} == {requested.cluster, requested.id, requested.worker},
      do: :ok,
      else: {:error, {:recovery_authority, :creation_identity_mismatch}}
  end

  defp mark_protocol(%Manifest{} = manifest),
    do: %{manifest | params: Map.put(manifest.params || %{}, "recovery_authority_protocol", 1)}

  defp reject_symlink(path, reason) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}} -> {:error, {:recovery_authority, reason}}
      {:ok, _} -> :ok
      {:error, :enoent} -> :ok
      {:error, file_reason} -> {:error, {:recovery_authority, {:unable_to_inspect_path, file_reason}}}
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
