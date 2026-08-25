defmodule Bedrock.Service.Foreman.StartingWorkers do
  @moduledoc false
  import Bedrock.Service.Foreman.WorkerInfo,
    only: [put_health: 2, put_manifest: 2, put_otp_name: 2]

  import Bedrock.Service.Foreman.WorkingDirectory,
    only: [initialize_working_directory: 2, read_and_validate_manifest: 3]

  alias Bedrock.Cluster
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Foreman.WorkingDirectory
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  @spec worker_info_from_path(Path.t(), otp_namer :: (Worker.id() -> Worker.otp_name())) ::
          [WorkerInfo.t()]
  def worker_info_from_path(path, otp_namer) do
    path
    |> worker_paths_from_disk()
    |> Enum.map(&worker_info_for_id(Path.basename(&1), &1, otp_namer))
  end

  # Directories the foreman's path holds that belong to other components.
  # The cluster supervisor derives object_storage/ from the same :path it
  # hands the foreman; the coordinator's raft/ is a sibling by the
  # convention every deployment config follows. Neither is a worker, and
  # neither is news.
  @infrastructure_dirs ~w(object_storage raft)

  @doc """
  The worker directories under the foreman's path.

  A worker directory is one holding a manifest — not merely one that
  exists. The foreman's path is shared with other components, so "every
  entry under path is a worker" is never true in a real deployment.

  Presence of the manifest is the test, not readability: a directory
  whose manifest is corrupt is a worker in trouble, and one whose
  manifest cannot be stat'ed for any reason OTHER than absence is a
  worker we cannot rule out. Both are enumerated so they surface as
  `:failed_to_start`. Only a definite absence excludes a directory —
  a live worker must never vanish from the foreman's view because of a
  permissions mistake.
  """
  @spec worker_paths_from_disk(Path.t()) :: [Path.t()]
  def worker_paths_from_disk(path) do
    path
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.filter(&worker_directory?/1)
  end

  @doc """
  Directories under the foreman's path that are neither workers nor
  another component's, in sorted order.

  These are the remains of workers that cannot be started: a worker whose
  manifest is gone is unstartable, and because retirement runs THROUGH
  the worker — `Foreman.worker_retired/2` from a live process holding the
  id and foreman ref its manifest supplied — an unstartable directory can
  never retire itself either. It will be re-attempted and re-fail on
  every boot, holding its disk forever.

  The foreman reports them rather than reclaiming them: the directory may
  hold a WAL, and deleting data on a guess is not the foreman's call.
  """
  @spec abandoned_paths_from_disk(Path.t()) :: [Path.t()]
  def abandoned_paths_from_disk(path) do
    path
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.reject(&(worker_directory?(&1) or Path.basename(&1) in @infrastructure_dirs))
    |> Enum.filter(&File.dir?/1)
    |> Enum.sort()
  end

  @spec worker_directory?(Path.t()) :: boolean()
  defp worker_directory?(path) do
    case File.stat(WorkingDirectory.manifest_path(path)) do
      {:ok, _stat} -> true
      # A definite "no manifest here": either nothing at that path, or
      # the entry is a plain file so it has no children at all.
      {:error, reason} when reason in [:enoent, :enotdir] -> false
      # Anything else (EACCES on the directory, EIO) is ignorance, not
      # absence — enumerate and let the start attempt report the truth.
      {:error, _reason} -> true
    end
  end

  @spec worker_info_for_id(Worker.id(), Path.t(), (Worker.id() -> Worker.otp_name())) ::
          WorkerInfo.t()
  def worker_info_for_id(id, path, otp_namer),
    do: %WorkerInfo{id: id, path: path, otp_name: otp_namer.(id), health: :stopped}

  @spec try_to_start_workers([WorkerInfo.t()], cluster :: Cluster.t(), object_storage :: term()) ::
          [WorkerInfo.t()]
  def try_to_start_workers(worker_info, cluster, object_storage) do
    worker_info
    |> Task.async_stream(&try_to_start_worker(&1, cluster, object_storage))
    |> Enum.map(fn
      {:ok, worker_info} -> worker_info
      {:error, reason} -> put_health(worker_info, {:failed_to_start, reason})
    end)
    |> Enum.to_list()
  end

  defmodule(StartWorkerOp) do
    @moduledoc false

    @type t :: %__MODULE__{}
    defstruct [:path, :id, :otp_name, :cluster, :manifest, :child_spec, :pid, :error, :object_storage]
  end

  @spec try_to_start_worker(WorkerInfo.t(), cluster :: Cluster.t(), object_storage :: term()) ::
          WorkerInfo.t()
  def try_to_start_worker(worker_info, cluster, object_storage) do
    %StartWorkerOp{
      id: worker_info.id,
      path: worker_info.path,
      otp_name: worker_info.otp_name,
      cluster: cluster,
      object_storage: object_storage
    }
    |> load_manifest()
    |> build_child_spec()
    |> start_supervised_child()
    |> find_worker()
    |> then(fn op ->
      worker_info
      |> put_manifest(op.manifest)
      |> put_otp_name(op.otp_name)
      |> put_health(
        case op.error do
          nil -> {:ok, op.pid}
          {:error, reason} -> {:failed_to_start, reason}
        end
      )
    end)
  end

  @spec load_manifest(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp load_manifest(%{error: nil} = op) do
    case read_and_validate_manifest(op.path, op.id, op.cluster.name()) do
      {:ok, manifest} -> %{op | manifest: manifest}
      error -> %{op | error: error}
    end
  end

  @spec build_child_spec(StartWorkerOp.t()) :: StartWorkerOp.t()
  def build_child_spec(%{error: nil} = op) do
    [
      cluster: op.cluster,
      path: op.path,
      id: op.id,
      otp_name: op.otp_name,
      foreman: op.cluster.otp_name(:foreman),
      params: op.manifest.params,
      object_storage: op.object_storage
    ]
    |> op.manifest.worker.child_spec()
    |> Map.put(:restart, :transient)
    |> then(&%{op | child_spec: &1})
  end

  def build_child_spec(op), do: op

  @spec start_supervised_child(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp start_supervised_child(%{error: nil} = op) do
    case DynamicSupervisor.start_child(op.cluster.otp_name(:worker_supervisor), op.child_spec) do
      {:ok, _root_pid} -> op
      {:error, {:already_started, _root_pid}} -> op
      error -> %{op | error: error}
    end
  end

  defp start_supervised_child(op), do: op

  @spec find_worker(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp find_worker(%{error: nil} = op) do
    case Process.whereis(op.otp_name) do
      nil -> %{op | error: {:error, :process_not_started}}
      pid -> %{op | pid: pid}
    end
  end

  defp find_worker(op), do: op

  @spec initialize_new_worker(
          Worker.id(),
          worker :: module(),
          params :: map(),
          Path.t(),
          cluster :: Cluster.t()
        ) :: WorkerInfo.t()
  @spec initialize_new_worker(Worker.id(), module(), map(), Path.t(), Cluster.t()) ::
          WorkerInfo.t()
  def initialize_new_worker(id, worker, params, path, cluster) do
    working_directory = Path.join(path, id)
    worker_info = worker_info_for_id(id, working_directory, &cluster.otp_name_for_worker/1)
    manifest = Manifest.new(cluster.name(), id, worker, params)

    case initialize_working_directory(working_directory, manifest) do
      :ok -> worker_info
      {:error, reason} -> put_health(worker_info, {:failed_to_start, reason})
    end
  end
end
