defmodule Bedrock.ControlPlane.ClusterController.Server do
  use GenServer

  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator

  import Bedrock.ControlPlane.ClusterController.State.Changes,
    only: [put_last_transaction_layout_id: 2, put_my_relief: 2, put_state: 2]

  import Bedrock.ControlPlane.ClusterController.Nodes,
    only: [
      request_to_rejoin: 5,
      node_added_worker: 4,
      node_last_seen_at: 3,
      ping_all_coordinators: 1
    ]

  import Bedrock.ControlPlane.ClusterController.Recovery,
    only: [
      try_to_recover: 1
    ]

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
      last_transaction_layout_id: config.transaction_system_layout.id
    }
    |> then(&{:ok, &1, {:continue, {:start_recovery, relieving}}})
  end

  @impl true
  def handle_continue({:start_recovery, {_epoch, old_controller}}, t) do
    if :unavailable != old_controller do
      old_controller |> ClusterController.stand_relieved({t.epoch, self()})
    end

    t
    |> ping_all_coordinators()
    |> try_to_recover()
    |> store_changes_to_config()
    |> noreply()
  end

  @impl true
  def handle_info({:timeout, :ping_all_coordinators}, t),
    do: t |> ping_all_coordinators() |> noreply()

  @impl true
  # If we have been relieved by another controller in a newer epoch, we should
  # not accept any calls from the cluster. We should reply with an error
  # informing the caller that we haven been relieved and who controls now
  # controls the cluster (and for what epoch).
  def handle_call(_, _from, t) when not is_nil(t.my_relief),
    do: t |> reply({:error, {:relieved_by, t.my_relief}})

  def handle_call(:fetch_transaction_system_layout, _from, t),
    do: t |> reply({:ok, t.config.transaction_system_layout})

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
  # If we are relieved by another controller, we should not accept any casts
  # from the cluster. We will ignore them. We are no longer relevant and are of
  # no further use.
  def handle_cast({:ping, from}, t) when not is_nil(t.my_relief),
    do: GenServer.cast(from, {:pong, t.my_relief})

  def handle_cast({:ping, from}, t) do
    GenServer.cast(from, {:pong, {t.epoch, self()}})

    t
    |> node_last_seen_at(node(from), now())
    |> store_changes_to_config()
    |> noreply()
  end

  def handle_cast(_, t) when not is_nil(t.my_relief),
    do: t |> noreply()

  def handle_cast({:pong, _from}, t),
    do: t |> noreply()

  def handle_cast({:stand_relieved, {new_epoch, _}}, t) when new_epoch <= t.epoch,
    do: t |> noreply()

  def handle_cast({:stand_relieved, {_new_epoch, _new_controller} = my_relief}, t),
    do: t |> put_my_relief(my_relief) |> put_state(:stopped) |> noreply()

  def handle_cast({:node_added_worker, node, worker_info}, t) do
    t
    |> node_added_worker(node, worker_info, now())
    |> try_to_recover()
    |> store_changes_to_config()
    |> noreply()
  end

  defp now, do: DateTime.utc_now()

  defp store_changes_to_config(t)
       when t.config.transaction_system_layout.id != t.last_transaction_layout_id do
    with :ok <- Coordinator.write_config(t.coordinator, t.config) do
      t |> put_last_transaction_layout_id(t.config.transaction_system_layout.id)
    else
      {:error, _reason} -> t
    end
  end

  defp store_changes_to_config(t), do: t

  defp noreply(t, opts \\ [])
  defp noreply(t, _opts), do: {:noreply, t}

  defp reply(t, result), do: {:reply, result, t}
end
