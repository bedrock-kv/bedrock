defmodule Bedrock.Service.Controller.Server do
  alias Bedrock.Service.Controller.State

  import Bedrock.Service.Controller.State, only: [new_state: 1]
  import Bedrock.Service.Controller.Impl

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
  def handle_call(:ping, _from, t), do: do_pong(t)
  def handle_call(:workers, _from, t), do: do_fetch_workers(t)
  def handle_call({:new_worker, id, kind}, _from, t), do: do_new_worker(id, kind, t)
  def handle_call(:wait_for_healthy, from, t), do: do_wait_for_healthy(from, t)
  def handle_call(_, _from, t), do: {:reply, {:error, :unsupported}, t}

  @impl true
  def handle_cast({:worker_health, worker_id, health}, t),
    do: do_worker_health(worker_id, health, t)

  def handle_cast(_, t), do: {:noreply, t}

  @impl true
  def handle_continue(:spin_up, t), do: do_spin_up(t)
end
