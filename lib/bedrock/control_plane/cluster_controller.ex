defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable t.
  """
  use GenServer

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.ClusterController.ServiceDirectory
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.Service.TransactionLogWorker

  @type service :: GenServer.name()
  @type timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec send_pong(service(), from_node :: node()) :: :ok
  def send_pong(controller, from_node),
    do: GenServer.cast(controller, {:pong, from_node})

  @spec notify_of_new_worker(service(), node(), keyword()) :: :ok
  def notify_of_new_worker(controller, node, worker_info),
    do: GenServer.cast(controller, {:new_worker, node, worker_info})

  @spec request_to_rejoin(service(), node(), [atom()], [keyword()]) ::
          :ok | {:error, :unavailable}
  @spec request_to_rejoin(service(), node(), [atom()], [keyword()], timeout_in_ms()) ::
          :ok | {:error, :unavailable}
  def request_to_rejoin(
        controller,
        node,
        advertised_services,
        services,
        timeout_in_ms \\ 5_000
      ) do
    GenServer.call(
      controller,
      {:request_to_rejoin, node, advertised_services, services},
      timeout_in_ms
    )
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec get_transaction_system_layout(service()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :initializing | :unavailable}
  @spec get_transaction_system_layout(service(), timeout_in_ms()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :initializing | :unavailable}
  def get_transaction_system_layout(controller, timeout_in_ms \\ 5_000) do
    GenServer.call(controller, :get_transaction_system_layout, timeout_in_ms)
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @type t :: %__MODULE__{
          epoch: Bedrock.epoch(),
          otp_name: atom(),
          cluster: Module.t(),
          config: Config.t(),
          sequencer: GenServer.name(),
          data_distributor: GenServer.name(),
          coordinator: pid(),
          node_tracking: NodeTracking.t(),
          transaction_system_layout: TransactionSystemLayout.t(),
          events: [term()]
        }
  defstruct [
    :epoch,
    :otp_name,
    :cluster,
    :config,
    :sequencer,
    :data_distributor,
    :coordinator,
    :node_tracking,
    :service_directory,
    :timer_ref,
    :transaction_system_layout,
    :events
  ]

  @doc false
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

  @doc false
  @impl GenServer
  def init({cluster, config, epoch, coordinator, otp_name}) do
    {:ok,
     %__MODULE__{
       epoch: epoch,
       cluster: cluster,
       config: config,
       otp_name: otp_name,
       coordinator: coordinator,
       node_tracking: NodeTracking.new(Config.nodes(config)),
       service_directory: ServiceDirectory.new(),
       events: []
     }, {:continue, :notify_and_lock}}
  end

  @impl GenServer
  def handle_continue(:notify_and_lock, t) do
    {:noreply,
     t
     |> ping_all_nodes()
     |> try_to_invite_old_sequencer()
     |> try_to_invite_old_data_distributor()
     |> try_to_lock_old_logs()}
  end

  def handle_continue(:process_events, %{events: []} = t), do: {:noreply, t}

  def handle_continue(:process_events, t) do
    {:noreply, t.events |> Enum.reduce(%{t | events: []}, &handle_event(&1, &2)),
     {:continue, :process_events}}
  end

  @impl GenServer
  def handle_info({:timeout, :ping_all_nodes}, t),
    do: {:noreply, t |> ping_all_nodes() |> determine_dead_nodes(), {:continue, :process_events}}

  @impl GenServer
  def handle_call(:get_transaction_system_layout, _from, t)
      when is_nil(t.transaction_system_layout),
      do: {:reply, {:error, :initializing}, t}

  def handle_call(:get_transaction_system_layout, _from, t),
    do: {:reply, {:ok, t.transaction_system_layout}, t}

  def handle_call({:request_to_rejoin, node, advertised_services, services}, _from, t) do
    handle_request_to_rejoin(t, node, advertised_services, services)
    |> case do
      {:ok, t} -> {:reply, :ok, t, {:continue, :process_events}}
      {:error, _reason} = error -> {:reply, error, t}
    end
  end

  @impl GenServer
  def handle_cast({:pong, node}, t),
    do: {:noreply, t |> update_node_last_seen_at(node), {:continue, :process_events}}

  def handle_cast({:new_worker, node, worker_info}, t) do
    {:noreply, t |> add_event({:node_added_worker, node, worker_info}),
     {:continue, :process_events}}
  end

  #

  @spec handle_request_to_rejoin(t(), node(), [atom()], []) ::
          {:ok, t()} | {:error, :nodes_must_be_added_by_an_administrator}
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

  def handle_event(event, t) do
    IO.inspect(event, label: "Event")
    t
  end

  @spec maybe_add_event(t(), boolean(), term()) :: t()
  def maybe_add_event(t, false, _event), do: t
  def maybe_add_event(t, true, event), do: t |> add_event(event)

  @spec add_event(t(), term()) :: t()
  def add_event(t, event),
    do: %{t | events: [event | t.events]}

  @spec now() :: integer()
  def now, do: :erlang.monotonic_time(:millisecond)

  @spec ping_all_nodes(t()) :: t()
  def ping_all_nodes(t) do
    t.cluster.ping_nodes(Config.nodes(t.config), self(), t.epoch)

    t
    |> cancel_timer()
    |> set_timer(:ping_all_nodes, Config.ping_rate_in_ms(t.config))
  end

  @spec update_node_last_seen_at(t(), node()) :: t()
  def update_node_last_seen_at(t, node) do
    node_up = not NodeTracking.alive?(t.node_tracking, node)
    NodeTracking.update_last_seen_at(t.node_tracking, node, now())
    t |> maybe_add_event(node_up, {:node_up, node})
  end

  @spec determine_dead_nodes(t()) :: t()
  def determine_dead_nodes(t) do
    t.node_tracking
    |> NodeTracking.dead_nodes(now(), 3 * Config.ping_rate_in_ms(t.config))
    |> Enum.reduce(t, fn dead_node, t ->
      t.node_tracking |> NodeTracking.down(dead_node)
      t |> add_event({:node_down, dead_node})
    end)
  end

  @spec maybe_add_node(t(), node()) :: t()
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

  @spec update_advertised_services(t(), node(), [atom()]) :: t()
  def update_advertised_services(t, node, advertised_services) do
    NodeTracking.update_advertised_services(t.node_tracking, node, advertised_services)
    t
  end

  # Sequencer

  @spec try_to_invite_old_sequencer(t()) :: t()
  def try_to_invite_old_sequencer(t) do
    t.config
    |> Config.sequencer()
    |> send_rejoin_invitation_to_sequencer(t)
  end

  @spec send_rejoin_invitation_to_sequencer(sequencer :: pid() | nil, t()) :: t()
  def send_rejoin_invitation_to_sequencer(nil, t), do: t

  def send_rejoin_invitation_to_sequencer(sequencer, t) do
    Sequencer.invite_to_rejoin(sequencer, self(), t.epoch)
    t |> add_expected_service(sequencer, :sequencer)
  end

  # Data Distributor

  @spec try_to_invite_old_data_distributor(t()) :: t()
  def try_to_invite_old_data_distributor(t) do
    t.config
    |> Config.data_distributor()
    |> send_rejoin_invitation_to_data_distributor(t)
  end

  @spec send_rejoin_invitation_to_data_distributor(data_distributor :: pid() | nil, t()) :: t()
  def send_rejoin_invitation_to_data_distributor(nil, t), do: t

  def send_rejoin_invitation_to_data_distributor(data_distributor, t) do
    DataDistributor.invite_to_rejoin(data_distributor, self(), t.epoch)
    t |> add_expected_service(data_distributor, :data_distributor)
  end

  # Transaction Logs

  @spec try_to_lock_old_logs(t()) :: t()
  def try_to_lock_old_logs(t) do
    t.config
    |> Config.log_workers()
    |> Enum.reduce(t, fn log_worker, t ->
      :ok = TransactionLogWorker.request_lock(log_worker, self(), t.epoch)
      t |> add_expected_service(log_worker, :log_worker)
    end)
  end

  # Internals

  @spec add_expected_service(t(), GenServer.name(), atom()) :: t()
  def add_expected_service(t, service, service_type) do
    t.service_directory |> ServiceDirectory.add_expected_service(service, service_type)
    t
  end

  @spec cancel_timer(t()) :: t()
  def cancel_timer(%{timer_ref: nil} = t), do: t

  def cancel_timer(%{timer_ref: timer_ref} = t) do
    Process.cancel_timer(timer_ref)
    %{t | timer_ref: nil}
  end

  @spec set_timer(t(), name :: atom(), timeout_in_ms()) :: t()
  def set_timer(%{timer_ref: nil} = t, name, timeout_in_ms),
    do: %{t | timer_ref: Process.send_after(self(), {:timeout, name}, timeout_in_ms)}
end
