defmodule Bedrock.Cluster.Monitor.Server do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.ControlPlane.Coordinator

  use GenServer
  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Monitor.State,
    only: [
      put_controller: 2
    ]

  import Bedrock.Cluster.Monitor.Advertising,
    only: [
      advertise_capabilities: 1,
      advertise_worker_to_cluster_controller: 2,
      publish_cluster_controller_replaced_to_pubsub: 1
    ]

  import Bedrock.Cluster.Monitor.Discovery,
    only: [
      change_coordinator: 2,
      find_a_live_coordinator: 1
    ]

  import Bedrock.Cluster.Monitor.PingPong,
    only: [
      ping_controller: 1,
      pong_missed: 1,
      pong_received: 1,
      reset_ping_timer: 1
    ]

  import Bedrock.Cluster.Monitor.Telemetry,
    only: [
      trace_searching_for_controller: 1,
      trace_searching_for_coordinator: 1,
      trace_found_coordinator: 2,
      trace_found_controller: 2,
      trace_lost_controller: 1,
      trace_missed_pong: 2
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
    trace_searching_for_coordinator(t.cluster)

    t
    |> find_a_live_coordinator()
    |> case do
      {:ok, coordinator} ->
        t
        |> found_coordinator(coordinator)
        |> noreply(:find_current_controller)

      {:error, :unavailable} ->
        t
        |> continue_search_for_coordinator()
        |> noreply()
    end
  end

  def handle_continue(:find_current_controller, t) do
    trace_searching_for_controller(t.cluster)

    t.coordinator
    |> Coordinator.fetch_controller(100)
    |> case do
      {:ok, controller} when is_pid(controller) ->
        t
        |> found_controller(controller)
        |> noreply()

      {:error, reason} when reason in [:timeout, :unavailable] ->
        t
        |> continue_search_for_controller()
        |> noreply()
    end
  end

  def found_coordinator(t, coordinator) do
    trace_found_coordinator(t.cluster, coordinator)

    t
    |> cancel_timer(:find_a_live_coordinator)
    |> change_coordinator(coordinator)
  end

  def continue_search_for_coordinator(t) do
    t
    |> cancel_timer(:find_a_live_coordinator)
    |> set_timer(:find_a_live_coordinator, t.cluster.monitor_ping_timeout_in_ms())
    |> change_coordinator(:unavailable)
  end

  def found_controller(t, controller) do
    trace_found_controller(t.cluster, controller)

    t
    |> cancel_timer(:find_current_controller)
    |> change_controller(controller)
  end

  def continue_search_for_controller(t) do
    t
    |> change_controller(:unavailable)
    |> cancel_timer(:find_current_controller)
    |> set_timer(:find_current_controller, t.cluster.monitor_ping_timeout_in_ms())
  end

  def change_controller(t, controller) when t.controller == controller, do: t

  def change_controller(t, controller) do
    t
    |> put_controller(controller)
    |> publish_cluster_controller_replaced_to_pubsub()
    |> case do
      %{controller: :unavailable} ->
        t

      t ->
        t
        |> reset_ping_timer()
        |> ping_controller()
        |> advertise_capabilities()
        |> case do
          {:error, {:relieved_by, {_new_epoch, new_controller}}} ->
            t |> change_controller(new_controller)

          {:error, :unavailable} ->
            t |> change_controller(:unavailable)

          {:ok, t} ->
            t

          {:error, _reason} ->
            t
        end
    end
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

  def handle_info({:timeout, :find_current_controller}, t),
    do: t |> noreply(:find_current_controller)

  def handle_info({:timeout, :ping}, t) when t.missed_pongs > 3 do
    trace_lost_controller(t.cluster)

    t
    |> cancel_timer(:ping)
    |> change_controller(:unavailable)
    |> noreply(:find_current_controller)
  end

  def handle_info({:timeout, :ping}, t) do
    trace_missed_pong(t.cluster, t.missed_pongs)

    t
    |> pong_missed()
    |> reset_ping_timer()
    |> ping_controller()
    |> noreply()
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, t) do
    if pid == t.coordinator || pid == {t.cluster.otp_name(:coordinator), t.node} do
      t
      |> change_coordinator(:unavailable)
      |> noreply(:find_a_live_coordinator)
    else
      t |> noreply()
    end
  end

  @doc false
  @impl GenServer

  def handle_cast({:pong, {_epoch, controller}}, t) when controller == t.controller do
    t
    |> pong_received()
    |> noreply()
  end

  def handle_cast({:pong, {_epoch, controller}}, t) do
    t
    |> pong_received()
    |> cancel_all_timers()
    |> change_controller(controller)
    |> noreply()
  end

  def handle_cast({:advertise_worker, worker_pid}, t),
    do: t |> advertise_worker_to_cluster_controller(worker_pid) |> noreply()

  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue), do: {:noreply, t, {:continue, continue}}
  defp reply(t, reply), do: {:reply, reply, t}
end
