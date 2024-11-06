defmodule Bedrock.Cluster.Gateway.Server do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Coordinator

  use GenServer
  use Bedrock.Internal.TimerManagement
  import Bedrock.Internal.GenServer.Replies

  import Bedrock.Cluster.Gateway.State,
    only: [
      put_director: 2
    ]

  import Bedrock.Cluster.Gateway.Advertising,
    only: [
      advertise_capabilities: 1,
      advertise_worker_to_director: 2,
      publish_director_replaced_to_pubsub: 1
    ]

  import Bedrock.Cluster.Gateway.Discovery,
    only: [
      change_coordinator: 2,
      find_a_live_coordinator: 1
    ]

  import Bedrock.Cluster.Gateway.PingPong,
    only: [
      ping_director: 1,
      pong_missed: 1,
      pong_received: 1,
      reset_ping_timer: 1
    ]

  import Bedrock.Cluster.Gateway.Telemetry

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
    trace_started(cluster)

    %State{
      node: Node.self(),
      cluster: cluster,
      descriptor: descriptor,
      path_to_descriptor: path_to_descriptor,
      coordinator: :unavailable,
      director: :unavailable,
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
        |> noreply(continue: :find_current_director)

      {:error, :unavailable} ->
        t
        |> continue_search_for_coordinator()
        |> noreply()
    end
  end

  def handle_continue(:find_current_director, t) do
    trace_searching_for_director(t.cluster)

    t.coordinator
    |> Coordinator.fetch_director(100)
    |> case do
      {:ok, director} when is_pid(director) ->
        t
        |> found_director(director)
        |> noreply()

      {:error, reason} when reason in [:timeout, :unavailable] ->
        t
        |> continue_search_for_director()
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
    |> set_timer(:find_a_live_coordinator, t.cluster.gateway_ping_timeout_in_ms())
    |> change_coordinator(:unavailable)
  end

  def found_director(t, director) do
    trace_found_director(t.cluster, director)

    t
    |> cancel_timer(:find_current_director)
    |> change_director(director)
  end

  def continue_search_for_director(t) do
    t
    |> change_director(:unavailable)
    |> cancel_timer(:find_current_director)
    |> set_timer(:find_current_director, t.cluster.gateway_ping_timeout_in_ms())
  end

  def change_director(t, director) when t.director == director, do: t

  def change_director(t, director) do
    t
    |> put_director(director)
    |> publish_director_replaced_to_pubsub()
    |> case do
      %{director: :unavailable} ->
        t

      t ->
        t
        |> reset_ping_timer()
        |> ping_director()
        |> advertise_capabilities()
        |> case do
          {:error, {:relieved_by, {_new_epoch, new_director}}} ->
            t |> change_director(new_director)

          {:error, :unavailable} ->
            t |> change_director(:unavailable)

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

  def handle_call(:fetch_director, _from, %{director: :unavailable} = t),
    do: t |> reply({:error, :unavailable})

  def handle_call(:fetch_director, _from, t),
    do: t |> reply({:ok, t.director})

  def handle_call(:fetch_coordinator_nodes, _from, t),
    do: t |> reply({:ok, t.descriptor.coordinator_nodes})

  @impl GenServer
  def handle_info({:timeout, :find_a_live_coordinator}, t),
    do: t |> noreply(continue: :find_a_live_coordinator)

  def handle_info({:timeout, :find_current_director}, t),
    do: t |> noreply(continue: :find_current_director)

  def handle_info({:timeout, :ping}, t) when t.missed_pongs > 3 do
    trace_lost_director(t.cluster)

    t
    |> cancel_timer(:ping)
    |> change_director(:unavailable)
    |> noreply(continue: :find_current_director)
  end

  def handle_info({:timeout, :ping}, t) do
    trace_missed_pong(t.cluster, t.missed_pongs)

    t
    |> pong_missed()
    |> reset_ping_timer()
    |> ping_director()
    |> noreply()
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, t) do
    if pid == t.coordinator || pid == {t.cluster.otp_name(:coordinator), t.node} do
      t
      |> change_coordinator(:unavailable)
      |> noreply(continue: :find_a_live_coordinator)
    else
      t |> noreply()
    end
  end

  @doc false
  @impl GenServer

  def handle_cast({:pong, {_epoch, director}}, t) when director == t.director do
    t
    |> pong_received()
    |> noreply()
  end

  def handle_cast({:pong, {_epoch, director}}, t) do
    t
    |> pong_received()
    |> cancel_all_timers()
    |> change_director(director)
    |> noreply()
  end

  def handle_cast({:advertise_worker, worker_pid}, t),
    do: t |> advertise_worker_to_director(worker_pid) |> noreply()
end
