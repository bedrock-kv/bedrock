defmodule Bedrock.Service.Controller.Server do
  alias Bedrock.Service.Controller.State

  import Bedrock.Service.Controller.State, only: [new_state: 1]

  import Bedrock.Service.Controller.StartingWorkers,
    only: [
      otp_names_for_running_workers: 1,
      worker_info_from_path: 2,
      start_workers: 3,
      start_worker: 3,
      initialize_new_worker: 6,
      otp_name_for_worker: 2
    ]

  import Bedrock.Service.Controller.Health,
    only: [recompute_controller_health: 1, update_health_for_worker: 3]

  import Bedrock.Service.Controller.WaitingForHealthy,
    only: [add_pid_to_waiting_for_healthy: 2, notify_waiting_for_healthy: 1]

  use GenServer

  def required_opt_keys,
    do: [:cluster, :subsystem, :path, :otp_name, :default_worker, :worker_supervisor_otp_name]

  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    args = opts |> Keyword.take(required_opt_keys()) |> Map.new()
    %{id: __MODULE__, start: {GenServer, :start_link, [__MODULE__, args, [name: args.otp_name]]}}
  end

  @impl true
  @spec init(map()) :: {:ok, State.t(), {:continue, :spin_up}} | {:stop, :missing_required_params}
  def init(args) do
    args
    |> new_state()
    |> case do
      {:ok, t} -> {:ok, t, {:continue, :spin_up}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  @spec handle_call(any(), any(), State.t()) :: {:reply, any(), State.t()} | {:noreply, State.t()}
  def handle_call(:ping, _from, t),
    do: t |> reply(:pong)

  def handle_call(:workers, _from, t),
    do: t |> reply({:ok, otp_names_for_running_workers(t)})

  def handle_call({:new_worker, id, kind}, _from, t) do
    worker_info =
      id
      |> initialize_new_worker(kind, %{}, t.path, t.cluster, otp_namer(t))
      |> start_worker(t.cluster, t.worker_supervisor_otp_name)

    t
    |> update_in([:workers], &Map.put(&1, id, worker_info))
    |> reply(worker_info.health)
  end

  def handle_call(:wait_for_healthy, _from, %{health: :ok} = t),
    do: t |> reply(:ok)

  def handle_call(:wait_for_healthy, from, t),
    do: t |> add_pid_to_waiting_for_healthy(from) |> noreply()

  @impl true
  @spec handle_cast(any(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, any()}}
  def handle_cast({:worker_health, worker_id, health}, t) do
    t
    |> update_health_for_worker(worker_id, health)
    |> noreply(continue: :recompute_health)
  end

  @impl true
  @spec handle_continue(any(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, any()}}
  def handle_continue(:spin_up, t),
    do: t |> noreply(continue: {:start_workers, worker_info_from_path(t.path, otp_namer(t))})

  def handle_continue({:start_workers, worker_info}, t),
    do:
      t
      |> update_in([:workers], fn workers ->
        worker_info
        |> start_workers(t.cluster, t.worker_supervisor_otp_name)
        |> Enum.into(workers, fn worker_info -> {worker_info.id, worker_info} end)
      end)
      |> noreply(continue: :recompute_health)

  def handle_continue(:recompute_health, t),
    do: t |> recompute_controller_health() |> noreply(continue: :notify_waiting_for_healthy)

  def handle_continue(:notify_waiting_for_healthy, t),
    do: t |> notify_waiting_for_healthy() |> noreply()

  defp reply(t, result), do: {:reply, result, t}
  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}

  defp otp_namer(t), do: &otp_name_for_worker(t.otp_name, &1)
end
