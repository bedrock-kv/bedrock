defmodule Bedrock.Service.Controller.Server do
  alias Bedrock.Service.Controller.Logic

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
  def init({subsystem, cluster, path, default_worker, worker_supervisor_otp_name, otp_name}) do
    Logic.startup(
      subsystem,
      cluster,
      path,
      default_worker,
      worker_supervisor_otp_name,
      otp_name
    )
    |> case do
      {:ok, t} -> {:ok, t, {:continue, :spin_up}}
    end
  end

  @impl GenServer
  def handle_call(:ping, _from, t),
    do: t |> reply(:pong)

  def handle_call(:workers, _from, t),
    do: t |> reply({:ok, Logic.otp_names_for_running_workers(t)})

  def handle_call(:wait_for_healthy, _from, %{health: :ok} = t),
    do: t |> reply(:ok)

  def handle_call(:wait_for_healthy, from, t),
    do: t |> Logic.add_pid_to_waiting_for_healthy(from) |> noreply()

  @impl GenServer
  def handle_cast({:worker_health, worker_id, health}, t),
    do:
      t
      |> Logic.update_health_for_worker(worker_id, health)
      |> noreply(continue: :recompute_health)

  @impl GenServer
  def handle_continue(:spin_up, t),
    do: t |> noreply(continue: {:start_workers, Logic.worker_ids_from_disk(t)})

  def handle_continue({:start_workers, instance_ids}, t),
    do: t |> Logic.start_workers(instance_ids) |> noreply(continue: :recompute_health)

  def handle_continue(:recompute_health, t),
    do: t |> Logic.recompute_controller_health() |> noreply(continue: :notify_waiting_for_healthy)

  def handle_continue(:notify_waiting_for_healthy, t),
    do: t |> Logic.notify_waiting_for_healthy() |> noreply()

  defp reply(t, result), do: {:reply, result, t}
  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
end
