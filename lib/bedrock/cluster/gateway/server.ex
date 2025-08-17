defmodule Bedrock.Cluster.Gateway.Server do
  @moduledoc false

  use GenServer
  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Gateway.Calls

  import Bedrock.Cluster.Gateway.Discovery,
    only: [
      change_coordinator: 2,
      find_a_live_coordinator: 1
    ]

  import Bedrock.Cluster.Gateway.Telemetry

  import Bedrock.Cluster.Gateway.WorkerAdvertisement,
    only: [
      advertise_worker_with_leader_check: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Gateway.State

  require Logger

  @spec child_spec(
          opts :: [
            cluster: module(),
            descriptor: Descriptor.t(),
            path_to_descriptor: Path.t(),
            otp_name: atom(),
            capabilities: [atom()],
            mode: :active | :passive
          ]
        ) :: Supervisor.child_spec()
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
  @spec init({
          cluster :: module(),
          path_to_descriptor :: Path.t(),
          descriptor :: Descriptor.t(),
          mode :: :active | :passive,
          capabilities :: [atom()]
        }) :: {:ok, State.t(), {:continue, :find_a_live_coordinator}}
  def init({cluster, path_to_descriptor, descriptor, mode, capabilities}) do
    trace_started(cluster)

    then(
      %State{
        node: Node.self(),
        cluster: cluster,
        descriptor: descriptor,
        path_to_descriptor: path_to_descriptor,
        known_coordinator: :unavailable,
        transaction_system_layout: nil,
        mode: mode,
        capabilities: capabilities
      },
      &{:ok, &1, {:continue, :find_a_live_coordinator}}
    )
  end

  @doc false
  @impl true
  @spec handle_continue(:find_a_live_coordinator, State.t()) :: {:noreply, State.t()}
  def handle_continue(:find_a_live_coordinator, t) do
    t
    |> find_a_live_coordinator()
    |> case do
      {t, :ok} -> noreply(t)
      {t, {:error, :unavailable}} -> noreply(t)
    end
  end

  @doc false
  @impl true
  @spec handle_call({:begin_transaction, keyword()}, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call({:begin_transaction, opts}, _, t) do
    {updated_state, result} = begin_transaction(t, opts)
    reply(updated_state, result)
  end

  @spec handle_call({:renew_read_version_lease, term()}, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call({:renew_read_version_lease, read_version}, _, t) do
    t
    |> renew_read_version_lease(read_version)
    |> then(fn {t, result} -> reply(t, result) end)
  end

  @spec handle_call(:get_known_coordinator, GenServer.from(), State.t()) ::
          {:reply, {:ok, term()} | {:error, :unavailable}, State.t()}
  def handle_call(:get_known_coordinator, _, t) do
    case t.known_coordinator do
      :unavailable -> reply(t, {:error, :unavailable})
      coordinator -> reply(t, {:ok, coordinator})
    end
  end

  @spec handle_call(:get_descriptor, GenServer.from(), State.t()) ::
          {:reply, {:ok, Descriptor.t()}, State.t()}
  def handle_call(:get_descriptor, _, t) do
    reply(t, {:ok, t.descriptor})
  end

  @doc false
  @impl true
  @spec handle_info({:timeout, :find_a_live_coordinator}, State.t()) ::
          {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:timeout, :find_a_live_coordinator}, t), do: noreply(t, continue: :find_a_live_coordinator)

  @spec handle_info({:tsl_updated, term()}, State.t()) :: {:noreply, State.t()}
  def handle_info({:tsl_updated, new_tsl}, t) do
    # Update cached TSL when coordinator broadcasts updates
    updated_state = %{t | transaction_system_layout: new_tsl}
    noreply(updated_state)
  end

  @spec handle_info({:DOWN, reference(), :process, term(), term()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:DOWN, _ref, :process, name, _reason}, t) do
    coordinator_matches =
      case t.known_coordinator do
        coordinator_ref when coordinator_ref != :unavailable ->
          name == coordinator_ref ||
            (is_tuple(name) and elem(name, 0) == t.cluster.otp_name(:coordinator))

        :unavailable ->
          is_tuple(name) and elem(name, 0) == t.cluster.otp_name(:coordinator)
      end

    if coordinator_matches do
      t
      |> change_coordinator(:unavailable)
      |> noreply(continue: :find_a_live_coordinator)
    else
      noreply(t)
    end
  end

  @doc false
  @impl true
  @spec handle_cast({:advertise_worker, pid()}, State.t()) :: {:noreply, State.t()}
  def handle_cast({:advertise_worker, worker_pid}, t),
    do: t |> advertise_worker_with_leader_check(worker_pid) |> noreply()
end
