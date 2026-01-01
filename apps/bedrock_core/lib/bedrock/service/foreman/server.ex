defmodule Bedrock.Service.Foreman.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies
  import Bedrock.Service.Foreman.Impl
  import Bedrock.Service.Foreman.State, only: [new_state: 1]

  alias Bedrock.Cluster
  alias Bedrock.Service.Foreman.State

  @spec required_opt_keys() :: [atom()]
  def required_opt_keys, do: [:cluster, :path, :capabilities, :otp_name]

  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            path: Path.t(),
            capabilities: [Cluster.capability()],
            otp_name: atom()
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
          otp_name: atom()
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
  def handle_call(:storage_workers, _from, t), do: t |> do_fetch_storage_workers() |> then(&reply(t, {:ok, &1}))

  @impl true
  def handle_call(:get_all_running_services, _from, t),
    do: t |> do_get_all_running_services() |> then(&reply(t, {:ok, &1}))

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
  def handle_call(:wait_for_healthy, from, t) do
    t
    |> do_wait_for_healthy(from)
    |> case do
      :ok -> reply(t, :ok)
      t -> noreply(t)
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :unknown_command})

  @impl true
  def handle_cast({:worker_health, worker_id, health}, t), do: t |> do_worker_health(worker_id, health) |> noreply()

  @impl true
  def handle_cast(_, t), do: noreply(t)

  @impl true
  def handle_continue(:spin_up, t), do: t |> do_spin_up() |> noreply()
end
