defmodule Bedrock.Service.Foreman.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies
  import Bedrock.Service.Foreman.Impl
  import Bedrock.Service.Foreman.State, only: [new_state: 1]

  alias Bedrock.Cluster
  alias Bedrock.Service.Foreman.State

  @spec required_opt_keys() :: [atom()]
  def required_opt_keys, do: [:cluster, :path, :capabilities, :otp_name, :object_storage]

  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            path: Path.t(),
            capabilities: [Cluster.capability()],
            otp_name: atom(),
            object_storage: term()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    args = opts |> Keyword.take(required_opt_keys()) |> Map.new()
    %{id: __MODULE__, start: {GenServer, :start_link, [__MODULE__, args, [name: args.otp_name]]}}
  end

  @impl true
  @spec init(%{
          cluster: Cluster.t(),
          path: Path.t(),
          capabilities: [Cluster.capability()],
          otp_name: atom(),
          object_storage: term()
        }) :: {:ok, State.t(), {:continue, :spin_up}} | {:stop, :missing_required_params}
  def init(args) do
    args
    |> new_state()
    |> case do
      {:ok, t} -> {:ok, t, {:continue, :spin_up}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:ping, _from, t), do: reply(t, :pong)

  @impl true
  def handle_call(:workers, _from, t), do: t |> do_fetch_workers() |> then(&reply(t, {:ok, &1}))

  @impl true
  def handle_call(:materializer_workers, _from, t),
    do: t |> do_fetch_materializer_workers() |> then(&reply(t, {:ok, &1}))

  @impl true
  def handle_call(:get_all_running_services, _from, t),
    do: t |> do_get_all_running_services() |> then(&reply(t, {:ok, &1}))

  @impl true
  def handle_call({:new_worker, id, kind, params}, _from, t),
    do: t |> do_new_worker(id, kind, params) |> then(fn {t, health} -> reply(t, {:ok, health}) end)

  @impl true
  def handle_call({:new_worker, id, kind}, _from, t),
    do: t |> do_new_worker(id, kind) |> then(fn {t, health} -> reply(t, {:ok, health}) end)

  @impl true
  def handle_call({:remove_worker, worker_id}, _from, t),
    do: t |> do_remove_worker(worker_id) |> then(fn {t, result} -> reply(t, result) end)

  @impl true
  def handle_call({:remove_workers, worker_ids}, _from, t),
    do: t |> do_remove_workers(worker_ids) |> then(fn {t, results} -> reply(t, results) end)

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :unknown_command})

  @impl true
  def handle_cast({:worker_health, worker_id, reporter, health}, t),
    do: t |> do_worker_health(worker_id, reporter, health) |> noreply()

  # The PID-bearing legacy form still establishes registration authority.
  # Unattributed nonhealthy reports and retirement fall through unchanged.
  def handle_cast({:worker_health, worker_id, {:ok, pid}}, t),
    do: t |> do_worker_health(worker_id, pid, {:ok, pid}) |> noreply()

  # A hosted worker decided its own retirement; the foreman only janitors.
  @impl true
  def handle_cast({:worker_retired, worker_id, reporter}, t),
    do: t |> do_worker_retired(worker_id, reporter) |> noreply()

  @impl true
  def handle_cast(_, t), do: noreply(t)

  # A newly durable transaction system layout arrived (forwarded by this
  # node's Link): relay it to the hosted workers, which self-detect
  # displacement. The foreman never answers a membership question.
  @impl true
  def handle_info({:tsl_updated, transaction_system_layout}, t),
    do: t |> do_relay_tsl(transaction_system_layout) |> noreply()

  # How long to keep looking for the replacement a supervisor starts for
  # a worker that died, and how often. The :DOWN beats the restart every
  # time, so the first look is always too early; these bound how long a
  # restarted worker can stay recorded as :stopped.
  @recheck_interval_ms 25
  @recheck_attempts 20

  # A hosted worker's process is gone. Without this the monitor's :DOWN
  # would fall into the catch-all below and the foreman would go on
  # naming a dead process as running.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, t) do
    case do_worker_down(t, ref, reason) do
      {t, :no_such_worker} -> noreply(t)
      {t, worker_id} -> t |> schedule_recheck(worker_id, @recheck_attempts) |> noreply()
    end
  end

  # The worker died; see whether its supervisor has since replaced it.
  @impl true
  def handle_info({:worker_recheck, worker_id, attempts_left}, t) do
    case do_worker_recheck(t, worker_id, attempts_left) do
      {t, :done} -> noreply(t)
      {t, :retry} -> t |> schedule_recheck(worker_id, attempts_left - 1) |> noreply()
    end
  end

  @impl true
  def handle_info(_, t), do: noreply(t)

  defp schedule_recheck(t, worker_id, attempts_left) do
    Process.send_after(self(), {:worker_recheck, worker_id, attempts_left}, @recheck_interval_ms)
    t
  end

  @impl true
  def handle_continue(:spin_up, t), do: t |> do_spin_up() |> noreply()
end
