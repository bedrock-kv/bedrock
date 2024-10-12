defmodule Bedrock.ControlPlane.ClusterController.Server do
  use GenServer

  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator

  import Bedrock.ControlPlane.ClusterController.Nodes,
    only: [
      determine_dead_nodes: 2,
      request_to_rejoin: 5,
      node_added_worker: 4,
      node_last_seen_at: 3,
      ping_all_nodes: 1
    ]

  import Bedrock.ControlPlane.ClusterController.Recovery,
    only: [
      lock_config: 1,
      recover: 1
    ]

  @doc false
  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster param"
    config = opts[:config] || raise "Missing :config param"
    epoch = opts[:epoch] || raise "Missing :epoch param"
    coordinator = opts[:coordinator] || raise "Missing :coordinator param"
    otp_name = opts[:otp_name] || raise "Missing :otp_name param"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, config, epoch, coordinator, otp_name},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, config, epoch, coordinator, otp_name}) do
    %State{
      epoch: epoch,
      cluster: cluster,
      config: config,
      otp_name: otp_name,
      coordinator: coordinator,
      node_tracking: config |> Config.coordinators() |> NodeTracking.new(),
      last_transaction_layout_id: config.transaction_system_layout.id
    }
    |> then(&{:ok, &1, {:continue, :finish_init}})
  end

  @impl true
  def handle_continue(:finish_init, t) do
    t
    |> lock_config()
    |> ping_all_nodes()
    |> recover()
    |> store_changes_to_config()
    |> noreply()
  end

  @impl true
  def handle_info({:timeout, :ping_all_nodes}, t) do
    t
    |> ping_all_nodes()
    |> determine_dead_nodes(now())
    |> store_changes_to_config()
    |> noreply()
  end

  def handle_info({:ping, from}, t) do
    send(from, {:pong, self()})

    t
    |> node_last_seen_at(node(from), now())
    |> store_changes_to_config()
    |> noreply()
  end

  @impl true
  def handle_call({:request_to_rejoin, node, capabilities, running_services}, _from, t) do
    t
    |> request_to_rejoin(node, capabilities, running_services, now())
    |> then(fn
      {:ok, t} -> t |> store_changes_to_config() |> reply(:ok)
      {:error, _reason} = error -> t |> reply(error)
    end)
  end

  @impl true
  def handle_cast({:pong, node}, t) do
    t
    |> node_last_seen_at(node, now())
    |> store_changes_to_config()
    |> noreply()
  end

  def handle_cast({:node_added_worker, node, worker_info}, t) do
    t
    |> node_added_worker(node, worker_info, now())
    |> store_changes_to_config()
    |> noreply()
  end

  def handle_cast({:log_lock_complete, _id, _info}, t),
    do: noreply(t)

  defp now, do: DateTime.utc_now()

  defp noreply(t), do: {:noreply, t}
  defp reply(t, result), do: {:reply, result, t}

  defp store_changes_to_config(t)
       when t.config.transaction_system_layout.id != t.last_transaction_layout_id do
    :ok = Coordinator.write_config(t.coordinator, t.config)
    put_in(t.last_transaction_layout_id, t.config.transaction_system_layout.id)
  end

  defp store_changes_to_config(t), do: t
end
