defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable state.
  """
  use GenServer
  require Logger

  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.ClusterController.ServiceDirectory
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Log

  @type t :: GenServer.server()
  @type timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec send_pong(cluster_controller :: t(), from_node :: node()) :: :ok
  def send_pong(cluster_controller, from_node),
    do: GenServer.cast(cluster_controller, {:pong, from_node})

  @spec report_new_worker(cluster_controller :: t(), node(), keyword()) :: :ok
  def report_new_worker(cluster_controller, node, worker_info),
    do: GenServer.cast(cluster_controller, {:new_worker, node, worker_info})

  @spec request_to_rejoin(cluster_controller :: t(), node(), [atom()], [keyword()]) ::
          :ok | {:error, :unavailable | :nodes_must_be_added_by_an_administrator}
  @spec request_to_rejoin(
          cluster_controller :: t(),
          node(),
          [atom()],
          [keyword()],
          timeout_in_ms()
        ) ::
          :ok | {:error, :unavailable | :nodes_must_be_added_by_an_administrator}
  def request_to_rejoin(
        cluster_controller,
        node,
        advertised_services,
        services,
        timeout_in_ms \\ 5_000
      ) do
    GenServer.call(
      cluster_controller,
      {:request_to_rejoin, node, advertised_services, services},
      timeout_in_ms
    )
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec report_transaction_log_lock_complete(
          cluster_controller :: t(),
          Log.id(),
          info :: [
            last_tx_id: Transaction.version(),
            minimum_durable_tx_id: Transaction.version()
          ]
        ) :: :ok
  def report_transaction_log_lock_complete(controller, id, info),
    do:
      GenServer.cast(
        controller,
        {:transaction_log_lock_complete, id, info}
      )

  @spec get_transaction_system_layout(cluster_controller :: t()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :initializing | :unavailable}
  @spec get_transaction_system_layout(cluster_controller :: t(), timeout_in_ms()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :initializing | :unavailable}
  def get_transaction_system_layout(cluster_controller, timeout_in_ms \\ 5_000) do
    GenServer.call(cluster_controller, :get_transaction_system_layout, timeout_in_ms)
    |> case do
      nil -> {:error, :initializing}
      layout -> {:ok, layout}
    end
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

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

  defmodule Data do
    @type t :: %__MODULE__{
            epoch: Bedrock.epoch(),
            otp_name: atom(),
            cluster: module(),
            config: Config.t() | nil,
            coordinator: pid(),
            node_tracking: NodeTracking.t(),
            service_directory: ServiceDirectory.t(),
            timer_ref: reference() | nil,
            transaction_system_layout: TransactionSystemLayout.t() | nil,
            events: [term()]
          }
    defstruct epoch: nil,
              otp_name: nil,
              cluster: nil,
              config: nil,
              coordinator: nil,
              node_tracking: nil,
              service_directory: nil,
              timer_ref: nil,
              transaction_system_layout: nil,
              events: []

    def new(cluster, config, epoch, coordinator, otp_name) do
      %Data{
        epoch: epoch,
        cluster: cluster,
        config: config,
        otp_name: otp_name,
        coordinator: coordinator,
        node_tracking: NodeTracking.new(Config.nodes(config)),
        service_directory: ServiceDirectory.new(),
        events: []
      }
    end
  end

  defmodule Logic do
    @type service :: GenServer.name()
    @type timeout_in_ms :: Bedrock.timeout_in_ms()

    @spec handle_request_to_rejoin(Data.t(), node(), [atom()], []) ::
            {:ok, Data.t()} | {:error, :nodes_must_be_added_by_an_administrator}
    def handle_request_to_rejoin(t, node, advertised_services, services) do
      t =
        t
        |> maybe_add_node(node)
        |> update_node_last_seen_at(node)
        |> update_advertised_services(node, advertised_services)

      if NodeTracking.authorized?(t.node_tracking, node) do
        {:ok, services |> Enum.reduce(t, &add_event(&2, {:node_added_worker, node, &1}))}
      else
        {:error, :nodes_must_be_added_by_an_administrator}
      end
    end

    @spec notify_and_lock(Data.t()) :: Data.t()
    def notify_and_lock(t) do
      t
      |> ping_all_nodes()
      |> try_to_invite_old_sequencer()
      |> try_to_invite_old_data_distributor()
      |> try_to_lock_old_logs()
    end

    @spec ping_all_nodes(Data.t()) :: Data.t()
    def ping_all_nodes(t) do
      t.cluster.ping_nodes(Config.nodes(t.config), self(), t.epoch)

      t
      |> cancel_timer()
      |> set_timer(:ping_all_nodes, Config.ping_rate_in_ms(t.config))
    end

    @spec update_node_last_seen_at(Data.t(), node()) :: Data.t()
    def update_node_last_seen_at(t, node) do
      node_up = not NodeTracking.alive?(t.node_tracking, node)
      NodeTracking.update_last_seen_at(t.node_tracking, node, now())
      t |> maybe_add_event(node_up, {:node_up, node})
    end

    @spec determine_dead_nodes(Data.t()) :: Data.t()
    def determine_dead_nodes(t) do
      t.node_tracking
      |> NodeTracking.dying_nodes(now(), 3 * Config.ping_rate_in_ms(t.config))
      |> Enum.reduce(t, fn dying_node, t ->
        t.node_tracking |> NodeTracking.down(dying_node)
        t |> add_event({:node_down, dying_node})
      end)
    end

    @spec maybe_add_node(Data.t(), node()) :: Data.t()
    def maybe_add_node(t, node) do
      if not NodeTracking.exists?(t.node_tracking, node) do
        NodeTracking.add_node(
          t.node_tracking,
          node,
          Config.allow_volunteer_nodes_to_join?(t.config)
        )
      end

      t
    end

    @spec update_advertised_services(Data.t(), node(), [atom()]) :: Data.t()
    def update_advertised_services(t, node, advertised_services) do
      NodeTracking.update_advertised_services(t.node_tracking, node, advertised_services)
      t
    end

    @spec add_expected_service(Data.t(), GenServer.name(), atom()) :: Data.t()
    def add_expected_service(t, service, service_type) do
      t.service_directory |> ServiceDirectory.add_expected_service!(service, service_type)
      t
    end

    @spec maybe_add_event(Data.t(), boolean(), term()) :: Data.t()
    def maybe_add_event(t, false, _event), do: t
    def maybe_add_event(t, true, event), do: t |> add_event(event)

    @spec add_event(Data.t(), term()) :: Data.t()
    def add_event(t, event),
      do: %{t | events: [event | t.events]}

    @spec now() :: integer()
    def now, do: :erlang.monotonic_time(:millisecond)

    # Sequencer

    @spec try_to_invite_old_sequencer(Data.t()) :: Data.t()
    def try_to_invite_old_sequencer(t) do
      t.config.transaction_system_layout.sequencer
      |> send_rejoin_invitation_to_sequencer(t)
    end

    @spec send_rejoin_invitation_to_sequencer(sequencer :: pid() | nil, Data.t()) :: Data.t()
    def send_rejoin_invitation_to_sequencer(nil, t), do: t

    def send_rejoin_invitation_to_sequencer(sequencer, t) do
      Sequencer.invite_to_rejoin(sequencer, self(), t.epoch)
      t |> add_expected_service(sequencer, :sequencer)
    end

    # Data Distributor

    @spec try_to_invite_old_data_distributor(Data.t()) :: Data.t()
    def try_to_invite_old_data_distributor(t) do
      t.config.transaction_system_layout.data_distributor
      |> send_rejoin_invitation_to_data_distributor(t)
    end

    @spec send_rejoin_invitation_to_data_distributor(data_distributor :: pid() | nil, Data.t()) ::
            Data.t()
    def send_rejoin_invitation_to_data_distributor(nil, t), do: t

    def send_rejoin_invitation_to_data_distributor(data_distributor, t) do
      DataDistributor.invite_to_rejoin(data_distributor, self(), t.epoch)
      t |> add_expected_service(data_distributor, :data_distributor)
    end

    # Transaction Logs

    @spec try_to_lock_old_logs(Data.t()) :: Data.t()
    def try_to_lock_old_logs(t) do
      t.config
      |> Config.log_workers()
      |> Enum.reduce(t, fn log_worker, t ->
        :ok = Log.lock(log_worker, self(), t.epoch)
        t |> add_expected_service(log_worker, :log_worker)
      end)
    end

    @spec cancel_timer(Data.t()) :: Data.t()
    def cancel_timer(%{timer_ref: nil} = t), do: t

    def cancel_timer(%{timer_ref: timer_ref} = t) do
      Process.cancel_timer(timer_ref)
      %{t | timer_ref: nil}
    end

    @spec set_timer(Data.t(), name :: atom(), timeout_in_ms()) :: Data.t()
    def set_timer(%{timer_ref: nil} = t, name, timeout_in_ms),
      do: %{t | timer_ref: Process.send_after(self(), {:timeout, name}, timeout_in_ms)}
  end

  @impl GenServer
  def init({cluster, config, epoch, coordinator, otp_name}),
    do:
      {:ok, Data.new(cluster, config, epoch, coordinator, otp_name),
       {:continue, :notify_and_lock}}

  @impl GenServer
  def handle_info({:timeout, :ping_all_nodes}, t),
    do:
      {:noreply, t |> Logic.ping_all_nodes() |> Logic.determine_dead_nodes(),
       {:continue, :process_events}}

  @impl GenServer
  def handle_call(:get_transaction_system_layout, _from, t),
    do: {:reply, t.transaction_system_layout, t}

  def handle_call({:request_to_rejoin, node, advertised_services, services}, _from, t) do
    Logic.handle_request_to_rejoin(t, node, advertised_services, services)
    |> case do
      {:ok, t} -> {:reply, :ok, t, {:continue, :process_events}}
      {:error, _reason} = error -> {:reply, error, t}
    end
  end

  @impl GenServer
  def handle_cast({:pong, node}, t),
    do: {:noreply, t |> Logic.update_node_last_seen_at(node), {:continue, :process_events}}

  def handle_cast({:new_worker, node, worker_info}, t) do
    {:noreply, t |> Logic.add_event({:node_added_worker, node, worker_info}),
     {:continue, :process_events}}
  end

  def handle_cast({:transaction_log_lock_complete, _id, _info}, t) do
    # TODO
    {:noreply, t}
  end

  @impl GenServer
  def handle_continue(:notify_and_lock, t), do: {:noreply, t |> Logic.notify_and_lock()}

  def handle_continue(:process_events, %{events: []} = t), do: {:noreply, t}

  def handle_continue(:process_events, t),
    do:
      {:noreply, t.events |> Enum.reduce(%{t | events: []}, &handle_event(&1, &2)),
       {:continue, :process_events}}

  #

  def handle_event(event, t) do
    Logger.info(inspect(event))
    t
  end
end
