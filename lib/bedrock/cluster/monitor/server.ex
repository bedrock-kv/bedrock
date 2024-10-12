defmodule Bedrock.Cluster.Monitor.Server do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator

  use GenServer
  use Bedrock.Internal.TimerManagement, type: State.t()

  import Bedrock.Cluster.Monitor.Advertising,
    only: [
      advertise_capabilities: 1,
      advertise_worker_to_cluster_controller: 2,
      publish_cluster_controller_replaced_to_pubsub: 1
    ]

  import Bedrock.Cluster.Monitor.Discovery,
    only: [
      change_coordinator: 2,
      change_cluster_controller: 2,
      find_a_live_coordinator: 1,
      maybe_set_ping_timer: 1
    ]

  import Bedrock.Cluster.Monitor.Telemetry,
    only: [
      emit_cluster_controller_replaced: 1
    ]

  require Logger

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    descriptor = opts[:descriptor] || raise "Missing :descriptor option"
    path_to_descriptor = opts[:path_to_descriptor] || raise "Missing :path_to_descriptor option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    capabilities = opts[:capabilities] || raise "Missing :capabilities option"
    mode = opts[:mode] || :active

    %{
      id: otp_name,
      start: {
        GenServer,
        :start_link,
        [
          __MODULE__,
          {cluster, path_to_descriptor, descriptor, mode, capabilities},
          [name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @doc false
  @impl GenServer
  def init({cluster, path_to_descriptor, descriptor, mode, capabilities}) do
    %State{
      node: Node.self(),
      cluster: cluster,
      descriptor: descriptor,
      path_to_descriptor: path_to_descriptor,
      coordinator: :unavailable,
      controller: :unavailable,
      mode: mode,
      capabilities: capabilities
    }
    |> then(&{:ok, &1, {:continue, :find_a_live_coordinator}})
  end

  @doc false
  @impl GenServer
  def handle_continue(:find_a_live_coordinator, t) do
    find_a_live_coordinator(t)
    |> case do
      {:ok, coordinator} ->
        t
        |> cancel_timer()
        |> change_coordinator(coordinator)
        |> noreply(:find_current_cluster_controller)

      {:error, :unavailable} ->
        t
        |> cancel_timer()
        |> set_timer(
          :find_a_live_coordinator,
          t.cluster.monitor_ping_timeout_in_ms()
        )
        |> change_coordinator(:unavailable)
        |> noreply()
    end
  end

  def handle_continue(:find_current_cluster_controller, t) do
    t.coordinator
    |> Coordinator.fetch_controller(100)
    |> case do
      {:ok, controller} ->
        t
        |> cancel_timer()
        |> change_cluster_controller(controller)
        |> noreply()

      {:error, :unavailable} ->
        t
        |> cancel_timer()
        |> change_cluster_controller(:unavailable)
        |> set_timer(
          :find_current_cluster_controller,
          t.cluster.monitor_ping_timeout_in_ms()
        )
        |> noreply()
    end
  end

  def handle_continue(:send_pong_to_controller, t) do
    :ok = ClusterController.send_pong(t.controller, t.node)

    t
    |> cancel_timer()
    |> maybe_set_ping_timer()
    |> noreply()
  end

  @doc false
  @impl GenServer
  def handle_call(:fetch_coordinator, _from, %{coordinator: :unavailable} = t),
    do: t |> reply({:error, :unavailable})

  def handle_call(:fetch_coordinator, _from, t),
    do: t |> reply({:ok, t.coordinator})

  def handle_call(:fetch_controller, _from, %{controller: :unavailable} = t),
    do: t |> reply({:error, :unavailable})

  def handle_call(:fetch_controller, _from, t),
    do: t |> reply({:ok, t.controller})

  def handle_call(:fetch_coordinator_nodes, _from, t),
    do: t |> reply({:ok, t.descriptor.coordinator_nodes})

  @impl GenServer
  def handle_info({:timeout, :find_a_live_coordinator}, t),
    do: t |> noreply(:find_a_live_coordinator)

  def handle_info({:timeout, :find_current_cluster_controller}, t),
    do: t |> noreply(:find_current_cluster_controller)

  def handle_info({:timeout, :ping}, t) do
    t
    |> change_cluster_controller(:unavailable)
    |> noreply(:find_current_cluster_controller)
  end

  def handle_info(:cluster_controller_replaced, t) do
    t
    |> emit_cluster_controller_replaced()
    |> publish_cluster_controller_replaced_to_pubsub()
    |> advertise_capabilities()
    |> case do
      {:ok, t} ->
        t |> noreply()

      {:error, :unavailable} ->
        put_in(t.controller, :unavailable)
        |> noreply(:find_current_cluster_controller)

      {:error, :nodes_must_be_added_by_an_administrator} ->
        # Logger.error("This node must be added to the cluster by an administrator")
        t |> noreply()
    end

    # Logger.debug(
    #   "Bedrock [#{t.cluster.name()}]: Controller changed to #{inspect(if is_pid(t.controller), do: node(t.controller), else: t.controller)}"
    # )
  end

  @doc false
  @impl GenServer
  def handle_cast({:ping, cluster_controller, _epoch}, t) do
    t
    |> change_cluster_controller(cluster_controller)
    |> noreply(:send_pong_to_controller)
  end

  def handle_cast({:advertise_worker, worker_pid}, t) do
    t
    |> advertise_worker_to_cluster_controller(worker_pid)
    |> noreply()
  end

  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue), do: {:noreply, t, {:continue, continue}}
  defp reply(t, reply), do: {:reply, reply, t}
end
