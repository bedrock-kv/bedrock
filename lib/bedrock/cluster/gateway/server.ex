defmodule Bedrock.Cluster.Gateway.Server do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.Descriptor

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

    storage_table = :ets.new(:storage, [:ordered_set, :protected, read_concurrency: true])

    %State{
      node: Node.self(),
      cluster: cluster,
      descriptor: descriptor,
      path_to_descriptor: path_to_descriptor,
      coordinator: :unavailable,
      director: :unavailable,
      transaction_system_layout: nil,
      storage_table: storage_table,
      mode: mode,
      capabilities: capabilities
    }
    |> then(&{:ok, &1, {:continue, :find_a_live_coordinator}})
  end

  @doc false
  @impl true
  @spec handle_continue(:find_a_live_coordinator, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :find_current_director}}
  def handle_continue(:find_a_live_coordinator, t) do
    t
    |> find_a_live_coordinator()
    |> case do
      {t, :ok} -> t |> noreply(continue: :find_current_director)
      {t, {:error, :unavailable}} -> t |> noreply()
    end
  end

  @spec handle_continue(:find_current_director, State.t()) :: {:noreply, State.t()}
  def handle_continue(:find_current_director, t) do
    t
    |> find_current_director()
    |> then(&noreply(&1))
  end

  @doc false
  @impl true
  @spec handle_call({:begin_transaction, keyword()}, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call({:begin_transaction, opts}, _, t) do
    t
    |> begin_transaction(opts)
    |> then(&reply(t, &1))
  end

  @spec handle_call(:next_read_version, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call(:next_read_version, _, t) do
    t
    |> next_read_version()
    |> then(&reply(t, &1))
  end

  @spec handle_call({:renew_read_version_lease, term()}, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call({:renew_read_version_lease, read_version}, _, t) do
    t
    |> renew_read_version_lease(read_version)
    |> then(fn {t, result} -> t |> reply(result) end)
  end

  @spec handle_call(:fetch_coordinator, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call(:fetch_coordinator, _, t) do
    t
    |> fetch_coordinator()
    |> then(&reply(t, &1))
  end

  @spec handle_call(:fetch_director, GenServer.from(), State.t()) :: {:reply, term(), State.t()}
  def handle_call(:fetch_director, _, t) do
    t
    |> fetch_director()
    |> then(&reply(t, &1))
  end

  @spec handle_call(:fetch_commit_proxy, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call(:fetch_commit_proxy, _, t) do
    t
    |> fetch_commit_proxy()
    |> then(&reply(t, &1))
  end

  @spec handle_call(:fetch_coordinator_nodes, GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
  def handle_call(:fetch_coordinator_nodes, _, t) do
    t
    |> fetch_coordinator_nodes()
    |> then(&reply(t, &1))
  end

  @doc false
  @impl true
  @spec handle_info({:timeout, :find_a_live_coordinator}, State.t()) ::
          {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:timeout, :find_a_live_coordinator}, t),
    do: t |> noreply(continue: :find_a_live_coordinator)

  @spec handle_info({:timeout, :find_current_director}, State.t()) ::
          {:noreply, State.t(), {:continue, :find_current_director}}
  def handle_info({:timeout, :find_current_director}, t),
    do: t |> noreply(continue: :find_current_director)

  @spec handle_info({:timeout, :ping}, State.t()) :: {:noreply, State.t()}
  def handle_info({:timeout, :ping}, t) do
    t
    |> pong_was_not_received()
    |> noreply()
  end

  @spec handle_info({:DOWN, reference(), :process, term(), term()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:DOWN, _ref, :process, name, _reason}, t) do
    if name == t.coordinator ||
         (is_tuple(name) and elem(name, 0) == t.cluster.otp_name(:coordinator)) do
      t
      |> change_coordinator(:unavailable)
      |> noreply(continue: :find_a_live_coordinator)
    else
      t |> noreply()
    end
  end

  @doc false
  @impl true
  @spec handle_cast({:pong, {non_neg_integer(), pid()}}, State.t()) :: {:noreply, State.t()}
  def handle_cast({:pong, {epoch, director}}, t) do
    t
    |> pong_received_from_director({director, epoch})
    |> noreply()
  end

  @spec handle_cast({:advertise_worker, pid()}, State.t()) :: {:noreply, State.t()}
  def handle_cast({:advertise_worker, worker_pid}, t),
    do: t |> advertise_worker_to_director(worker_pid) |> noreply()
end
