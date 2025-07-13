defmodule Bedrock.ControlPlane.Director.Server do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Director.NodeTracking
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator

  import Bedrock.ControlPlane.Director.State.Changes,
    only: [put_my_relief: 2, put_state: 2]

  import Bedrock.ControlPlane.Director.Nodes,
    only: [
      request_to_rejoin: 5,
      node_added_worker: 4,
      update_last_seen_at: 3,
      # determine_dead_nodes: 2,
      update_minimum_read_version: 3,
      ping_all_coordinators: 1,
      request_worker_creation: 4
    ]

  import Bedrock.ControlPlane.Director.Recovery,
    only: [
      try_to_recover: 1
    ]

  use GenServer
  import Bedrock.Internal.GenServer.Replies
  require Logger

  @doc false
  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster param"
    config = opts[:config] || raise "Missing :config param"
    epoch = opts[:epoch] || raise "Missing :epoch param"
    coordinator = opts[:coordinator] || raise "Missing :coordinator param"
    relieving = opts[:relieving]

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, config, epoch, coordinator, relieving}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, config, epoch, coordinator, relieving}) do
    %State{
      epoch: epoch,
      cluster: cluster,
      config: config,
      coordinator: coordinator,
      node_tracking: config |> Config.coordinators() |> NodeTracking.new(),
      lock_token: :crypto.strong_rand_bytes(32)
    }
    |> then(&{:ok, &1, {:continue, {:start_recovery, relieving}}})
  end

  @impl true
  def handle_continue({:start_recovery, {_epoch, old_director}}, %State{} = t) do
    if :unavailable != old_director do
      old_director |> Director.stand_relieved({t.epoch, self()})
    end

    t
    |> Map.put(:services, get_services_from_config(t.config))
    |> ping_all_coordinators()
    |> try_to_recover()
    |> store_changes_to_config()
    |> noreply()
  end

  @impl true
  def handle_info({:timeout, :ping_all_coordinators}, t) do
    t
    |> ping_all_coordinators()
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _monitor_ref, :process, failed_pid, reason}, _t) do
    # ANY transaction component failure triggers immediate director exit
    Logger.error(
      "Transaction component #{inspect(failed_pid)} failed with reason: #{inspect(reason)}"
    )

    Logger.error("Director exiting immediately due to component failure")

    # Exit immediately - let coordinator restart us with fresh epoch
    exit({:component_failure, failed_pid, reason})
  end

  @impl true
  # If we have been relieved by another director in a newer epoch, we should
  # not accept any calls from the cluster. We should reply with an error
  # informing the caller that we haven been relieved and who controls now
  # controls the cluster (and for what epoch).
  def handle_call(_, _from, t) when not is_nil(t.my_relief),
    do: t |> reply({:error, {:relieved_by, t.my_relief}})

  def handle_call(:fetch_transaction_system_layout, _from, t),
    do: t |> reply({:ok, t.config.transaction_system_layout})

  def handle_call({:request_worker_creation, node, worker_id, kind}, _from, t) do
    t
    |> request_worker_creation(node, worker_id, kind)
    |> then(&reply(t, &1))
  end

  def handle_call({:request_to_rejoin, node, capabilities, running_services}, _from, t) do
    t
    |> request_to_rejoin(node, capabilities, running_services |> Map.values(), now())
    |> case do
      {:ok, t} ->
        t
        |> try_to_recover()
        |> store_changes_to_config()
        |> reply(:ok)

      {:error, _reason} = error ->
        t |> reply(error)
    end
  end

  @impl true
  # If we are relieved by another director, we should not accept any casts
  # from the cluster. We will ignore them. We are no longer relevant and are of
  # no further use.
  def handle_cast({:ping, from, _}, t) when not is_nil(t.my_relief),
    do: GenServer.cast(from, {:pong, t.my_relief})

  def handle_cast({:ping, from, minimum_read_version}, t) do
    GenServer.cast(from, {:pong, {t.epoch, self()}})
    node = node(from)

    t
    |> update_last_seen_at(node, now())
    |> update_minimum_read_version(node, minimum_read_version)
    |> noreply()
  end

  def handle_cast(_, t) when not is_nil(t.my_relief),
    do: t |> noreply()

  def handle_cast({:pong, _from}, t),
    do: t |> noreply()

  def handle_cast({:stand_relieved, {new_epoch, _}}, t) when new_epoch <= t.epoch,
    do: t |> noreply()

  def handle_cast({:stand_relieved, {_new_epoch, _new_director} = my_relief}, t),
    do: t |> put_my_relief(my_relief) |> put_state(:stopped) |> noreply()

  def handle_cast({:node_added_worker, node, worker_info}, %State{} = t) do
    t
    |> node_added_worker(node, worker_info, now())
    |> try_to_recover()
    |> store_changes_to_config()
    |> noreply()
  end

  defp now, do: DateTime.utc_now()

  def get_services_from_config(%{transaction_system_layout: %{services: services}}),
    do: services || %{}

  def store_changes_to_config(%{config: nil} = t), do: t

  def store_changes_to_config(%State{coordinator: coordinator, config: config} = t) do
    case Coordinator.write_config(coordinator, config) do
      :ok ->
        t

      unexpected ->
        Logger.warning("Unexpected response from write_config: #{inspect(unexpected)}")
        t
    end
  end
end
