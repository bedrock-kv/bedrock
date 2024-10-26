defmodule Bedrock.Service.Controller.StartingWorkers do
  alias Bedrock.Service.Controller.WorkerInfo
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  import Bedrock.Service.Controller.WorkerInfo,
    only: [
      with_health_changed: 2
    ]

  import Bedrock.Service.Controller.WorkingDirectory,
    only: [
      initialize_working_directory: 2,
      read_and_validate_manifest: 3
    ]

  @spec worker_info_from_path(Path.t(), (Worker.id() -> atom())) :: [WorkerInfo.t()]
  def worker_info_from_path(path, otp_namer) do
    path
    |> worker_paths_from_disk()
    |> Enum.map(&worker_info_for_id(Path.basename(&1), &1, otp_namer))
  end

  @spec worker_paths_from_disk(Path.t()) :: [Path.t()]
  def worker_paths_from_disk(path) do
    path
    |> Path.join("*")
    |> Path.wildcard()
  end

  def worker_info_for_id(id, path, otp_namer),
    do: %WorkerInfo{id: id, otp_name: otp_namer.(id), path: path, health: :stopped}

  @spec start_workers([WorkerInfo.t()], cluster :: module(), Supervisor.name()) ::
          [WorkerInfo.t()]
  def start_workers(worker_info, cluster, worker_supervisor) do
    worker_info
    |> Task.async_stream(&start_worker(&1, cluster, worker_supervisor))
    |> Enum.map(fn
      {:ok, worker_info} -> worker_info
      {:error, reason} -> worker_info |> with_health_changed({:failed_to_start, reason})
    end)
    |> Enum.to_list()
  end

  defmodule(StartWorkerOp) do
    @type t :: %__MODULE__{}
    defstruct [:path, :id, :otp_name, :supervisor, :cluster, :manifest, :child_spec, :pid]
  end

  @spec start_worker(WorkerInfo.t(), module(), Supervisor.name()) :: WorkerInfo.t()
  def start_worker(worker_info, cluster, supervisor) do
    worker_info
    |> with_health_changed(
      do_start_worker_op(%StartWorkerOp{
        otp_name: worker_info.otp_name,
        id: worker_info.id,
        path: worker_info.path,
        supervisor: supervisor,
        cluster: cluster
      })
    )
  end

  @spec do_start_worker_op(StartWorkerOp.t()) :: {:ok, pid()} | {:failed_to_start, term()}
  defp do_start_worker_op(op) do
    with {:ok, op} <- load_manifest(op),
         {:ok, op} <- build_child_spec(op),
         {:ok, op} <- start_supervised_child(op),
         {:ok, op} <- find_worker(op) do
      {:ok, op.pid}
    else
      {:error, reason} -> {:failed_to_start, reason}
    end
  end

  @spec load_manifest(StartWorkerOp.t()) :: {:ok, StartWorkerOp.t()} | {:error, term()}
  defp load_manifest(op) do
    case read_and_validate_manifest(op.path, op.id, op.cluster.name()) do
      {:ok, manifest} -> {:ok, %{op | manifest: manifest}}
      error -> error
    end
  end

  @spec build_child_spec(StartWorkerOp.t()) :: {:ok, StartWorkerOp.t()}
  defp build_child_spec(op) do
    op.manifest.worker.child_spec(
      path: op.path,
      id: op.id,
      otp_name: op.otp_name,
      controller: op.cluster.otp_name(:controller)
    )
    |> Map.put(:restart, :transient)
    |> then(&{:ok, %{op | child_spec: &1}})
  end

  @spec start_supervised_child(StartWorkerOp.t()) :: {:ok, StartWorkerOp.t()} | {:error, term()}
  defp start_supervised_child(op) do
    case DynamicSupervisor.start_child(op.supervisor, op.child_spec) do
      {:ok, _root_pid} -> {:ok, op}
      error -> error
    end
  end

  @spec find_worker(StartWorkerOp.t()) ::
          {:ok, StartWorkerOp.t()} | {:error, :process_not_started}
  defp find_worker(op) do
    case Process.whereis(op.otp_name) do
      nil -> {:error, :process_not_started}
      pid -> {:ok, %{op | pid: pid}}
    end
  end

  @spec initialize_new_worker(
          Worker.id(),
          :log | :storage,
          params :: map(),
          Path.t(),
          cluster :: module(),
          (Worker.id() -> atom())
        ) :: WorkerInfo.t()
  def initialize_new_worker(id, kind, params, path, cluster, otp_namer) do
    worker = worker_for_kind(kind)
    manifest = Manifest.new(cluster.name(), id, worker, params)
    worker_info = worker_info_for_id(id, path, otp_namer)

    case initialize_working_directory(path, manifest) do
      :ok -> worker_info
      {:error, reason} -> worker_info |> with_health_changed({:failed_to_start, reason})
    end
  end

  @spec worker_for_kind(:log | :storage) :: module()
  defp worker_for_kind(:log), do: Bedrock.DataPlane.Log.Limestone
  defp worker_for_kind(:storage), do: Bedrock.DataPlane.Storage.Basalt

  def random_worker_id, do: :crypto.strong_rand_bytes(5) |> Base.encode32(case: :lower)

  @spec otp_name_for_worker(otp_name :: atom(), Worker.id()) :: atom()
  def otp_name_for_worker(otp_name, id),
    do: :"#{otp_name}_#{id}"
end
