defmodule Bedrock.Service.Controller do
  use GenServer

  alias Bedrock.Service.Worker
  alias Bedrock.Service.Manifest

  defstruct ~w[
    cluster
    subsystem
    default_worker
    worker_supervisor_otp_name
    workers
    health
    otp_name
    path
    registry
    waiting_for_healthy
  ]a
  @type t :: %__MODULE__{}
  @type worker_id :: String.t()
  @type controller_name :: GenServer.name()

  defguard is_controller(t) when is_pid(t) or is_atom(t) or is_tuple(t)

  defmodule WorkerInfo do
    @type t :: %__MODULE__{}
    defstruct [
      #
      :id,
      :health,
      :otp_name
    ]
  end

  def wait_for_healthy(t, timeout) when is_controller(t) do
    GenServer.call(t, :wait_for_healthy, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @spec report_worker_health(
          controller_name(),
          worker_id(),
          any()
        ) :: :ok
  def report_worker_health(t, worker_id, health) when is_controller(t),
    do: GenServer.cast(t, {:worker_health, worker_id, health})

  @spec workers(t :: controller_name()) :: {:ok, [Engine.t()]} | {:error, term()}
  def workers(t) when is_controller(t) do
    GenServer.call(t, :workers)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    subsystem = opts[:subsystem] || raise "Missing :subsystem option"
    path = opts[:path] || raise "Missing :path option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    default_worker = opts[:default_worker] || raise "Missing :default_worker option"

    worker_supervisor_otp_name =
      Keyword.get(opts, :worker_supervisor_otp_name) ||
        raise "Missing :worker_supervisor_otp_name option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             subsystem,
             cluster,
             path,
             default_worker,
             worker_supervisor_otp_name,
             otp_name
           },
           [name: otp_name]
         ]}
    }
  end

  @impl GenServer
  def init({
        subsystem,
        cluster,
        path,
        default_worker,
        worker_supervisor_otp_name,
        otp_name
      }) do
    t =
      %__MODULE__{
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
      }

    {:ok, t, {:continue, :sync_existing}}
  end

  @impl GenServer
  def handle_continue(:sync_existing, t) do
    workers =
      t.worker_supervisor_otp_name
      |> DynamicSupervisor.which_children()
      |> Enum.map(fn
        {_, worker_pid, _, _} when is_pid(worker_pid) ->
          Worker.info(worker_pid, [:id, :health, :otp_name])
          |> case do
            {:ok, info} ->
              {info[:id],
               %WorkerInfo{
                 id: info[:id],
                 health: info[:health] || :ok,
                 otp_name: info[:otp_name]
               }}

            _ ->
              :skip
          end

        _ ->
          :skip
      end)
      |> Enum.reject(&(&1 == :skip))
      |> Map.new()

    {:noreply, %{t | workers: workers} |> recompute_controller_health(), {:continue, :spin_up}}
  end

  def handle_continue(:spin_up, t) do
    worker_ids_to_start =
      t.path
      |> Path.join("*")
      |> Path.wildcard()
      |> Enum.map(&Path.basename/1)
      |> Enum.reject(&Map.has_key?(t.workers, &1))

    if [] == worker_ids_to_start && map_size(t.workers) == 0 do
      {:noreply, t}
    else
      {:noreply, t, {:continue, {:start_workers, worker_ids_to_start}}}
    end
  end

  def handle_continue({:start_workers, instance_ids}, t) do
    workers =
      instance_ids
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

    {:noreply, %{t | workers: workers} |> recompute_controller_health()}
  end

  @impl GenServer
  def handle_call(:ping, _from, t),
    do: {:reply, :pong, t}

  def handle_call(:workers, _from, t) do
    workers =
      t.workers
      |> Enum.map(fn
        {_id, %{otp_name: otp_name}} -> otp_name
      end)

    {:reply, {:ok, workers}, t}
  end

  def handle_call(:wait_for_healthy, _from, %{health: :ok} = t),
    do: {:reply, :ok, t}

  def handle_call(:wait_for_healthy, from, t),
    do: {:noreply, %{t | waiting_for_healthy: [from | t.waiting_for_healthy]}}

  @impl GenServer
  def handle_cast({:worker_health, worker_id, health}, t) do
    {:noreply,
     t
     |> update_worker_health(worker_id, health)}
  end

  def update_worker_health(t, worker_id, health) do
    %{
      t
      | workers:
          Map.update!(t.workers, worker_id, fn info ->
            Map.put(info, :health, health)
          end)
    }
    |> recompute_controller_health()
  end

  def recompute_controller_health(t) do
    %{
      t
      | health:
          t.workers
          |> Map.values()
          |> Enum.map(& &1.health)
          |> Enum.reduce(:ok, fn
            :ok, :ok -> :ok
            :ok, _ -> :starting
            {:failed_to_start, _}, :ok -> :starting
            {:failed_to_start, _}, _ -> {:failed_to_start, :at_least_one_failed_to_start}
          end)
    }
    |> notify_waiting_for_healthy_if_necessary()
  end

  def notify_waiting_for_healthy_if_necessary(%{waiting_for_healthy: []} = t), do: t

  def notify_waiting_for_healthy_if_necessary(%{health: :ok} = t) do
    t.waiting_for_healthy
    |> Enum.each(fn from -> GenServer.reply(from, :ok) end)

    %{t | wait_for_healthy: []}
  end

  @spec new_worker(t()) :: {:ok, worker_id()} | {:error, term()}
  def new_worker(t) do
    with id <- UUID.uuid4(),
         path <- Path.join(t.path, id),
         :ok <- File.mkdir_p(path),
         manifest <- Manifest.new(t.cluster.name(), id, t.default_worker),
         :ok <- manifest.worker.one_time_initialization(path),
         :ok <- manifest |> Manifest.write_to_file(path |> Path.join("manifest.json")) do
      {:ok, id}
    end
  end

  @spec start_worker_if_necessary(t(), worker_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_worker_if_necessary(t, id) do
    t.otp_name
    |> otp_name_for_worker(id)
    |> Process.whereis()
    |> case do
      nil -> start_worker(t, id)
      pid -> {:ok, pid}
    end
  end

  @spec start_worker(t(), worker_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_worker(t, id) do
    with path <- Path.join(t.path, id),
         {:ok, manifest} <- Manifest.load_from_file(Path.join(path, "manifest.json")),
         :ok <- check_manifest_id(manifest, id),
         :ok <- check_manifest_cluster_name(manifest, t.cluster.name()) do
      t.worker_supervisor_otp_name
      |> DynamicSupervisor.start_child(
        manifest.worker.child_spec(
          path: path,
          id: id,
          controller: t.otp_name,
          otp_name: otp_name_for_worker(t.otp_name, id)
        )
        |> Map.put(:restart, :transient)
      )
    end
  end

  @spec check_manifest_id(manifest :: Manifest.t(), id :: worker_id()) ::
          :ok | {:error, :id_in_manifest_does_not_match}
  defp check_manifest_id(manifest, id) when manifest.id == id, do: :ok
  defp check_manifest_id(_, _), do: {:error, :id_in_manifest_does_not_match}

  @spec check_manifest_cluster_name(manifest :: Manifest.t(), cluster_name :: String.t()) ::
          :ok | {:error, :cluster_name_in_manifest_does_not_match}
  defp check_manifest_cluster_name(manifest, cluster_name)
       when manifest.cluster == cluster_name,
       do: :ok

  defp check_manifest_cluster_name(_, _), do: {:error, :cluster_name_in_manifest_does_not_match}

  @spec otp_name_for_worker(otp_name :: atom(), worker_id()) :: atom()
  def otp_name_for_worker(otp_name, id),
    do: :"#{otp_name}_#{id |> String.replace("-", "_")}"
end
