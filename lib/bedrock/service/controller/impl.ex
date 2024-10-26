defmodule Bedrock.Service.Controller.Impl do
  alias Bedrock.Service.Controller.State
  alias Bedrock.Service.Controller.WorkerInfo
  alias Bedrock.Service.Worker

  import Bedrock.Service.Controller.State

  import Bedrock.Service.Controller.StartingWorkers,
    only: [
      worker_info_from_path: 2,
      start_workers: 3,
      start_worker: 3,
      initialize_new_worker: 6,
      otp_name_for_worker: 2
    ]

  import Bedrock.Service.Controller.Health,
    only: [compute_health_from_worker_info: 1]

  def do_fetch_workers(t),
    do: otp_names_for_running_workers(t)

  @spec do_new_worker(binary(), :log | :storage, any()) :: {State.t(), Worker.health()}
  def do_new_worker(id, kind, t) do
    worker_info =
      id
      |> initialize_new_worker(kind, %{}, t.path, t.cluster, otp_namer(t))
      |> start_worker(t.cluster, t.worker_supervisor_otp_name)

    t =
      t
      |> update_in([:workers], &Map.put(&1, id, worker_info))

    {t, worker_info.health}
  end

  @spec do_wait_for_healthy(State.t(), GenServer.from()) :: :ok | State.t()
  def do_wait_for_healthy(%{health: :ok}, _), do: :ok
  def do_wait_for_healthy(t, from), do: t |> add_pid_to_waiting_for_healthy(from)

  def do_worker_health(worker_id, health, t) do
    t
    |> put_health_for_worker(worker_id, health)
    |> recompute_health()
    |> notify_waiting_for_healthy()
  end

  @spec do_spin_up(State.t()) :: State.t()
  def do_spin_up(t) do
    t
    |> load_workers_from_disk()
    |> start_workers_that_are_stopped()
  end

  @spec load_workers_from_disk(State.t()) :: State.t()
  def load_workers_from_disk(t) do
    t
    |> update_workers(fn workers ->
      worker_info_from_path(t.path, otp_namer(t))
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @spec start_workers_that_are_stopped(State.t()) :: State.t()
  def start_workers_that_are_stopped(t) do
    t
    |> update_workers(fn workers ->
      workers
      |> Map.values()
      |> Enum.filter(&(&1.health == :stopped))
      |> start_workers(t.cluster, t.worker_supervisor_otp_name)
      |> merge_worker_info_into_workers(workers)
    end)
  end

  def recompute_health(t) do
    t
    |> update_health(fn _ ->
      t.workers()
      |> Map.values()
      |> compute_health_from_worker_info()
    end)
  end

  @spec merge_worker_info_into_workers(
          [WorkerInfo.t()],
          workers :: %{Worker.id() => WorkerInfo.t()}
        ) ::
          %{Worker.id() => WorkerInfo.t()}
  defp merge_worker_info_into_workers(worker_info, workers),
    do: Enum.into(worker_info, workers, &{&1.id, &1})

  @spec add_pid_to_waiting_for_healthy(State.t(), pid()) :: State.t()
  def add_pid_to_waiting_for_healthy(t, pid),
    do: t |> update_waiting_for_healthy(&[pid | &1])

  @spec notify_waiting_for_healthy(State.t()) :: State.t()
  def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
      when waiting_for_healthy != [] do
    :ok = Enum.each(t.waiting_for_healthy, &GenServer.reply(&1, :ok))

    t |> put_waiting_for_healthy([])
  end

  def notify_waiting_for_healthy(t), do: t

  @spec otp_names_for_running_workers(State.t()) :: [atom()]
  def otp_names_for_running_workers(t),
    do: Enum.map(t.workers, fn {_id, %{otp_name: otp_name}} -> otp_name end)

  def otp_namer(t), do: &otp_name_for_worker(t.otp_name, &1)
end
