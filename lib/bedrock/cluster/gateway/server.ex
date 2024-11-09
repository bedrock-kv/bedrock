defmodule Bedrock.Cluster.Gateway.Server do
  alias Bedrock.Cluster.Gateway.State

  use GenServer
  use Bedrock.Internal.TimerManagement

  import Bedrock.Internal.GenServer.Replies

  import Bedrock.Cluster.Gateway.Calls

  import Bedrock.Cluster.Gateway.Discovery,
    only: [
      change_coordinator: 2,
      find_a_live_coordinator: 1,
      find_current_director: 1
    ]

  import Bedrock.Cluster.Gateway.Telemetry

  import Bedrock.Cluster.Gateway.DirectorRelations,
    only: [
      advertise_worker_to_director: 2,
      pong_received_from_director: 2,
      pong_was_not_received: 1
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
  @impl true
  def init({cluster, path_to_descriptor, descriptor, mode, capabilities}) do
    trace_started(cluster)

    %State{
      node: Node.self(),
      cluster: cluster,
      descriptor: descriptor,
      path_to_descriptor: path_to_descriptor,
      coordinator: :unavailable,
      director: :unavailable,
      tranasction_system_layout: nil,
      mode: mode,
      capabilities: capabilities
    }
    |> then(&{:ok, &1, {:continue, :find_a_live_coordinator}})
  end

  @doc false
  @impl true
  def handle_continue(:find_a_live_coordinator, t) do
    t
    |> find_a_live_coordinator()
    |> case do
      {t, :ok} -> t |> noreply(continue: :find_current_director)
      {t, {:error, :unavailable}} -> t |> noreply()
    end
  end

  def handle_continue(:find_current_director, t) do
    t
    |> find_current_director()
    |> then(&noreply(&1))
  end

  @doc false
  @impl true
  def handle_call({:begin_transaction, opts}, _, t) do
    t
    |> begin_transaction(opts)
    |> then(&reply(t, &1))
  end

  def handle_call({:renew_read_version_lease, read_version}, _, t) do
    t
    |> renew_read_version_lease(read_version)
    |> then(fn {t, result} -> t |> reply(result) end)
  end

  def handle_call(:fetch_coordinator, _, t) do
    t
    |> fetch_coordinator()
    |> then(&reply(t, &1))
  end

  def handle_call(:fetch_director, _, t) do
    t
    |> fetch_director()
    |> then(&reply(t, &1))
  end

  def handle_call(:fetch_coordinator_nodes, _, t) do
    t
    |> fetch_coordinator_nodes()
    |> then(&reply(t, &1))
  end

  @doc false
  @impl true
  def handle_info({:timeout, :find_a_live_coordinator}, t),
    do: t |> noreply(continue: :find_a_live_coordinator)

  def handle_info({:timeout, :find_current_director}, t),
    do: t |> noreply(continue: :find_current_director)

  def handle_info({:timeout, :ping}, t) do
    t
    |> pong_was_not_received()
    |> noreply()
  end

  def handle_info({:DOWN, _ref, :process, name, _reason}, t) do
    if name == t.coordinator || elem(name, 0) == t.cluster.otp_name(:coordinator) do
      t
      |> change_coordinator(:unavailable)
      |> noreply(continue: :find_a_live_coordinator)
    else
      t |> noreply()
    end
  end

  @doc false
  @impl true
  def handle_cast({:pong, {_epoch, director}}, t) do
    t
    |> pong_received_from_director(director)
    |> noreply()
  end

  def handle_cast({:advertise_worker, worker_pid}, t),
    do: t |> advertise_worker_to_director(worker_pid) |> noreply()
end
