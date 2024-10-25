defmodule Bedrock.Service.Controller.Logic do
  alias Bedrock.Cluster.Monitor
  alias Bedrock.Service.Controller.Data
  alias Bedrock.Service.Controller.WorkerInfo
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  def startup(subsystem, cluster, path, default_worker, worker_supervisor_otp_name, otp_name) do
    {:ok,
     %Data{
       subsystem: subsystem,
       cluster: cluster,
       path: path,
       default_worker: default_worker,
       worker_supervisor_otp_name: worker_supervisor_otp_name,
       otp_name: otp_name,
       #
       health: :starting,
       waiting_for_healthy: [],
       workers: %{}
     }}
  end

  @spec worker_ids_from_disk(Data.t()) :: [Worker.id()]
  def worker_ids_from_disk(t) do
    t.path
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.map(&Path.basename/1)
  end

  @spec start_workers(Data.t(), [Worker.id()]) :: Data.t()
  def start_workers(t, worker_ids) do
    workers =
      worker_ids
      |> Enum.into(t.workers, fn instance_id ->
        health =
          start_worker_if_necessary(t, instance_id)
          |> case do
            {:ok, _pid} -> :ok
            {:error, reason} -> {:failed_to_start, reason}
          end

        {instance_id,
         %WorkerInfo{
           id: instance_id,
           health: health,
           otp_name: otp_name_for_worker(t.otp_name, instance_id)
         }}
      end)

    %{t | workers: workers}
  end

  @spec start_worker_if_necessary(Data.t(), Worker.id()) :: {:ok, pid()} | {:error, term()}
  def start_worker_if_necessary(t, id) do
    worker_otp_name = otp_name_for_worker(t.otp_name, id)

    worker_otp_name
    |> Process.whereis()
    |> case do
      nil -> start_worker(t, id, worker_otp_name)
      pid -> {:ok, pid}
    end
  end

  @spec start_worker(Data.t(), Worker.id(), Worker.otp_name()) ::
          {:ok, pid()} | {:error, term()}
  def start_worker(t, worker_id, worker_otp_name) do
    with path <- Path.join(t.path, worker_id),
         {:ok, manifest} <- Manifest.load_from_file(Path.join(path, "manifest.json")),
         :ok <- check_manifest_id(manifest, worker_id),
         :ok <- check_manifest_cluster_name(manifest, t.cluster.name()),
         {:ok, _top_level_pid} <-
           DynamicSupervisor.start_child(
             t.worker_supervisor_otp_name,
             manifest.worker.child_spec(
               path: path,
               id: worker_id,
               controller: t.otp_name,
               otp_name: worker_otp_name
             )
             |> Map.put(:restart, :transient)
           ) do
      Process.whereis(worker_otp_name)
      |> case do
        nil ->
          raise "Unable to locate server #{worker_otp_name}"

        worker_pid ->
          :ok = advertise_new_worker(t, worker_pid)
          {:ok, worker_pid}
      end
    end
  end

  @spec advertise_new_worker(Data.t(), worker_pid :: pid()) :: :ok
  def advertise_new_worker(t, worker_pid) do
    Monitor.advertise_worker(
      t.cluster.otp_name(:monitor),
      worker_pid
    )
  end

  @spec check_manifest_id(manifest :: Manifest.t(), id :: Worker.id()) ::
          :ok | {:error, :id_in_manifest_does_not_match}
  defp check_manifest_id(manifest, id) when manifest.id == id, do: :ok
  defp check_manifest_id(_, _), do: {:error, :id_in_manifest_does_not_match}

  @spec check_manifest_cluster_name(manifest :: Manifest.t(), cluster_name :: String.t()) ::
          :ok | {:error, :cluster_name_in_manifest_does_not_match}
  defp check_manifest_cluster_name(manifest, cluster_name)
       when manifest.cluster == cluster_name,
       do: :ok

  defp check_manifest_cluster_name(_, _), do: {:error, :cluster_name_in_manifest_does_not_match}

  @spec new_worker(Data.t()) :: {:ok, Worker.id()} | {:error, term()}
  def new_worker(t) do
    with id <- random_worker_id(),
         path <- Path.join(t.path, id),
         :ok <- File.mkdir_p(path),
         manifest <- Manifest.new(t.cluster.name(), id, t.default_worker),
         :ok <- manifest.worker.one_time_initialization(path),
         :ok <- manifest |> Manifest.write_to_file(path |> Path.join("manifest.json")) do
      {:ok, id}
    end
  end

  def random_worker_id, do: :crypto.strong_rand_bytes(5) |> Base.encode32(case: :lower)

  @spec otp_name_for_worker(otp_name :: atom(), Worker.id()) :: atom()
  defp otp_name_for_worker(otp_name, id),
    do: :"#{otp_name}_#{id |> String.replace("-", "_")}"

  @spec otp_names_for_running_workers(Data.t()) :: [atom()]
  def otp_names_for_running_workers(t) do
    t.workers
    |> Enum.map(fn
      {_id, %{otp_name: otp_name}} -> otp_name
    end)
  end

  @spec update_health_for_worker(Data.t(), Worker.id(), Worker.health()) :: Data.t()
  def update_health_for_worker(t, worker_id, health) do
    %{
      t
      | workers:
          Map.update!(t.workers, worker_id, fn info ->
            Map.put(info, :health, health)
          end)
    }
  end

  @spec recompute_controller_health(Data.t()) :: Data.t()
  def recompute_controller_health(t), do: %{t | health: compute_health(t)}

  @spec compute_health(Data.t()) :: Worker.health()
  defp compute_health(t) do
    t.workers
    |> Map.values()
    |> Enum.map(& &1.health)
    |> Enum.reduce(:ok, fn
      :ok, :ok -> :ok
      :ok, _ -> :starting
      {:failed_to_start, _}, :ok -> :starting
      {:failed_to_start, _}, _ -> {:failed_to_start, :at_least_one_failed_to_start}
    end)
  end

  @spec add_pid_to_waiting_for_healthy(Data.t(), pid()) :: Data.t()
  def add_pid_to_waiting_for_healthy(t, pid),
    do: %{t | waiting_for_healthy: [pid | t.waiting_for_healthy]}

  @spec notify_waiting_for_healthy(Data.t()) :: Data.t()
  def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
      when waiting_for_healthy != [] do
    Enum.each(t.waiting_for_healthy, fn from -> GenServer.reply(from, :ok) end)

    %{t | waiting_for_healthy: []}
  end

  def notify_waiting_for_healthy(t), do: t
end
