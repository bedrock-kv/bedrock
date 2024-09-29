defmodule Bedrock.Service.Controller do
  @moduledoc """
  """

  alias Bedrock.Service.Worker

  @type t :: GenServer.server()
  @type epoch :: pos_integer()
  @type server :: GenServer.server()

  @doc """
  Wait until the controller signals that it (and all of it's workers) are
  reporting that they are healthy, or the timeout happens... whichever comes
  first.
  """
  @spec wait_for_healthy(controller :: t(), timeout()) :: :ok | {:error, :unavailable}
  def wait_for_healthy(controller, timeout) do
    GenServer.call(controller, :wait_for_healthy, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc """
  Called by a worker to report it's health to the controller.
  """
  @spec report_worker_health(controller :: t(), Worker.id(), any()) :: :ok
  def report_worker_health(controller, worker_id, health),
    do: GenServer.cast(controller, {:worker_health, worker_id, health})

  @doc """
  Return a list of running workers.
  """
  @spec workers(controller :: t()) :: {:ok, [Worker.t()]} | {:error, term()}
  def workers(controller) do
    GenServer.call(controller, :workers)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @doc false
  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: __MODULE__.Server

  defmodule Data do
    @type t :: %__MODULE__{}
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
  end

  defmodule WorkerInfo do
    @type t :: %__MODULE__{}
    defstruct [
      :id,
      :health,
      :otp_name
    ]
  end

  defmodule Logic do
    alias Bedrock.Service.Manifest

    @type t :: Data.t()

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

    @spec worker_ids_from_disk(t()) :: [Worker.id()]
    def worker_ids_from_disk(t) do
      t.path
      |> Path.join("*")
      |> Path.wildcard()
      |> Enum.map(&Path.basename/1)
    end

    @spec start_workers(t(), [Worker.id()]) :: t()
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

    @spec start_worker_if_necessary(t(), Worker.id()) :: {:ok, pid()} | {:error, term()}
    def start_worker_if_necessary(t, id) do
      worker_otp_name = otp_name_for_worker(t.otp_name, id)

      worker_otp_name
      |> Process.whereis()
      |> case do
        nil -> start_worker(t, id, worker_otp_name)
        pid -> {:ok, pid}
      end
    end

    @spec start_worker(t(), Worker.id(), Worker.otp_name()) :: {:ok, pid()} | {:error, term()}
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
          nil -> raise "Unable to locate server #{worker_otp_name}"
          worker_pid -> {:ok, worker_pid}
        end
      end
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

    @spec new_worker(t()) :: {:ok, Worker.id()} | {:error, term()}
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

    @spec otp_name_for_worker(otp_name :: atom(), Worker.id()) :: atom()
    defp otp_name_for_worker(otp_name, id),
      do: :"#{otp_name}_#{id |> String.replace("-", "_")}"

    @spec otp_names_for_running_workers(t) :: [atom()]
    def otp_names_for_running_workers(t) do
      t.workers
      |> Enum.map(fn
        {_id, %{otp_name: otp_name}} -> otp_name
      end)
    end

    @spec update_health_for_worker(t(), Worker.id(), Worker.health()) :: t()
    def update_health_for_worker(t, worker_id, health) do
      %{
        t
        | workers:
            Map.update!(t.workers, worker_id, fn info ->
              Map.put(info, :health, health)
            end)
      }
    end

    @spec recompute_controller_health(t()) :: t()
    def recompute_controller_health(t), do: %{t | health: compute_health(t)}

    @spec compute_health(t()) :: Worker.health()
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

    @spec add_pid_to_waiting_for_healthy(t(), pid()) :: t()
    def add_pid_to_waiting_for_healthy(t, pid),
      do: %{t | waiting_for_healthy: [pid | t.waiting_for_healthy]}

    @spec notify_waiting_for_healthy(t()) :: t()
    def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
        when waiting_for_healthy != [] do
      Enum.each(t.waiting_for_healthy, fn from -> GenServer.reply(from, :ok) end)

      %{t | waiting_for_healthy: []}
    end

    def notify_waiting_for_healthy(t), do: t
  end

  defmodule Server do
    use GenServer

    @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
    def child_spec(opts) do
      cluster = opts[:cluster] || raise "Missing :cluster option"
      subsystem = opts[:subsystem] || raise "Missing :subsystem option"
      path = opts[:path] || raise "Missing :path option"
      otp_name = opts[:otp_name] || raise "Missing :otp_name option"
      default_worker = opts[:default_worker] || raise "Missing :default_worker option"

      worker_supervisor_otp_name =
        opts[:worker_supervisor_otp_name] || raise "Missing :worker_supervisor_otp_name option"

      %{
        id: __MODULE__,
        start:
          {GenServer, :start_link,
           [
             __MODULE__.Server,
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
    def init({subsystem, cluster, path, default_worker, worker_supervisor_otp_name, otp_name}) do
      {:ok, t} =
        Logic.startup(
          subsystem,
          cluster,
          path,
          default_worker,
          worker_supervisor_otp_name,
          otp_name
        )

      {:ok, t, {:continue, :spin_up}}
    end

    @impl GenServer
    def handle_call(:ping, _from, t),
      do: {:reply, :pong, t}

    def handle_call(:workers, _from, t),
      do: {:reply, {:ok, Logic.otp_names_for_running_workers(t)}, t}

    def handle_call(:wait_for_healthy, _from, %{health: :ok} = t),
      do: {:reply, :ok, t}

    def handle_call(:wait_for_healthy, from, t),
      do: {:noreply, t |> Logic.add_pid_to_waiting_for_healthy(from)}

    @impl GenServer
    def handle_cast({:worker_health, worker_id, health}, t),
      do:
        {:noreply, t |> Logic.update_health_for_worker(worker_id, health),
         {:continue, :recompute_health}}

    @impl GenServer
    def handle_continue(:spin_up, t),
      do: {:noreply, t, {:continue, {:start_workers, Logic.worker_ids_from_disk(t)}}}

    def handle_continue({:start_workers, instance_ids}, t),
      do: {:noreply, t |> Logic.start_workers(instance_ids), {:continue, :recompute_health}}

    def handle_continue(:recompute_health, t),
      do:
        {:noreply, t |> Logic.recompute_controller_health(),
         {:continue, :notify_waiting_for_healthy}}

    def handle_continue(:notify_waiting_for_healthy, t),
      do: {:noreply, t |> Logic.notify_waiting_for_healthy()}
  end
end
