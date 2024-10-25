defmodule Bedrock.Service.Controller.Server do
  alias Bedrock.Service.Controller.State

  import Bedrock.Service.Controller.State, only: [new_state: 1]

  import Bedrock.Service.Controller.Workers,
    only: [
      add_pid_to_waiting_for_healthy: 2,
      otp_names_for_running_workers: 1,
      update_health_for_worker: 3,
      start_workers: 2,
      recompute_controller_health: 1,
      notify_waiting_for_healthy: 1,
      worker_ids_from_disk: 1
    ]

  use GenServer

  def required_keys,
    do: [
      :cluster,
      :subsystem,
      :path,
      :otp_name,
      :default_worker,
      :worker_supervisor_otp_name
    ]

  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    args = opts |> Keyword.take(required_keys()) |> Map.new()
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
    do: t |> noreply(continue: {:start_workers, worker_ids_from_disk(t.path)})

  def handle_continue({:start_workers, instance_ids}, t),
    do: t |> start_workers(instance_ids) |> noreply(continue: :recompute_health)

  def handle_continue(:recompute_health, t),
    do: t |> recompute_controller_health() |> noreply(continue: :notify_waiting_for_healthy)

  def handle_continue(:notify_waiting_for_healthy, t),
    do: t |> notify_waiting_for_healthy() |> noreply()

  defp reply(t, result), do: {:reply, result, t}
  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
end
