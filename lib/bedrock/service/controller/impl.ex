defmodule Bedrock.Service.Controller.Impl do
  alias Bedrock.Service.Controller.State
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

  def do_pong(t),
    do: t |> reply(:pong)

  def do_fetch_workers(t),
    do: t |> reply({:ok, otp_names_for_running_workers(t)})

  def do_new_worker(id, kind, t) do
    worker_info =
      id
      |> initialize_new_worker(kind, %{}, t.path, t.cluster, otp_namer(t))
      |> start_worker(t.cluster, t.worker_supervisor_otp_name)

    t
    |> update_in([:workers], &Map.put(&1, id, worker_info))
    |> reply(worker_info.health)
  end

  def do_wait_for_healthy(_, %{health: :ok} = t), do: t |> reply(:ok)
  def do_wait_for_healthy(from, t), do: t |> add_pid_to_waiting_for_healthy(from) |> noreply()

  def do_worker_health(worker_id, health, t) do
    t
    |> put_health_for_worker(worker_id, health)
    |> recompute_health()
    |> notify_waiting_for_healthy()
    |> noreply()
  end

  def do_spin_up(t) do
    t
    |> start_workers_from_cold(worker_info_from_path(t.path, otp_namer(t)))
    |> noreply()
  end

  def start_workers_from_cold(t, worker_info) do
    t
    |> update_workers(fn workers ->
      worker_info
      |> start_workers(t.cluster, t.worker_supervisor_otp_name)
      |> Enum.into(workers, &{&1.id, &1})
    end)
  end

  def recompute_health(t) do
    t
    |> update_health(fn _ ->
      t.workers() |> Map.values() |> compute_health_from_worker_info()
    end)
  end

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

  defp reply(t, result), do: {:reply, result, t}
  defp noreply(t), do: {:noreply, t}

  def otp_namer(t), do: &otp_name_for_worker(t.otp_name, &1)
end
