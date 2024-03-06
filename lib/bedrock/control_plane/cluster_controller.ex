defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable t.
  """
  use GenServer

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

  @spec request_to_rejoin(service(), node(), [atom()], [keyword()], timeout_in_ms()) ::
          :ok | {:error, :unavailable}
  def request_to_rejoin(
        controller,
        node,
        advertised_services,
        running_services,
        timeout_in_ms \\ 5_000
      ) do
    GenServer.call(
      controller,
      {:request_to_rejoin, node, advertised_services, running_services},
      timeout_in_ms
    )
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec get_sequencer(service()) :: {:ok, pid()} | {:error, :unavailable}
  def get_sequencer(controller) do
    GenServer.call(controller, :get_sequencer)
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec get_data_distributor(service()) :: {:ok, pid()} | {:error, :unavailable}
  def get_data_distributor(controller) do
    GenServer.call(controller, :get_data_distributor)
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
          node_tracking: NodeTracking.t()
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
    :timer_ref
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

  @spec init({any(), Bedrock.ControlPlane.Config.t(), any(), any(), any()}) ::
          {:ok, Bedrock.ControlPlane.ClusterController.t(), {:continue, :recruiting}}
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
       service_directory: ServiceDirectory.new()
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

  def handle_continue(:track_rejoins, t) do
    {:noreply, t}
  end

  @impl GenServer
  def handle_info({:timeout, :ping_all_nodes}, t),
    do: {:noreply, t |> ping_all_nodes()}

  @impl GenServer
  def handle_call(:get_sequencer, _from, t),
    do: {:reply, {:ok, t.sequencer}, t}

  def handle_call(:get_data_distributor, _from, t),
    do: {:reply, {:ok, t.data_distributor}, t}

  def handle_call({:request_to_rejoin, node, advertised_services, running_services}, _from, t) do
    handle_request_to_rejoin(t, node, advertised_services, running_services)
    |> case do
      :ok -> {:reply, :ok, t, {:continue, :track_rejoins}}
      {:error, _reason} = error -> {:reply, error, t}
    end
  end

  @spec handle_request_to_rejoin(t(), node(), [atom()], []) ::
          :ok | {:error, :nodes_must_be_added_by_an_administrator}
  def handle_request_to_rejoin(t, node, advertised_services, running_services) do
    advertised_services = Enum.sort(advertised_services)
    now = :erlang.monotonic_time(:millisecond)

    IO.inspect(
      "advertised_services: #{inspect(advertised_services)}, running_services: #{inspect(running_services)}"
    )

    t.node_tracking
    |> NodeTracking.update_last_pong_received_at(node, now)
    |> NodeTracking.advertised_services(node)
    |> case do
      :unknown ->
        if Config.allow_volunteer_nodes_to_join?(t.config) do
          t.node_tracking |> NodeTracking.add_node(node, now, advertised_services)
          :ok
        else
          {:error, :nodes_must_be_added_by_an_administrator}
        end

      ^advertised_services ->
        :ok

      _existing_services ->
        t.node_tracking |> NodeTracking.update_advertised_services(node, advertised_services)
        :ok
    end
  end

  @impl GenServer
  def handle_cast({:pong, node}, t) do
    t.node_tracking
    |> NodeTracking.update_last_pong_received_at(node, :erlang.monotonic_time(:millisecond))

    {:noreply, t}
  end

  #

  @spec ping_all_nodes(t()) :: t()
  def ping_all_nodes(t) do
    t.cluster.ping_nodes(Config.nodes(t.config), self(), t.epoch)

    t
    |> cancel_timer()
    |> set_timer(:ping_all_nodes, Config.ping_rate_in_ms(t.config))
  end

  def recruit_missing_services(t) do
    t
  end

  # Sequencer

  @spec try_to_invite_old_sequencer(t()) :: t()
  def try_to_invite_old_sequencer(t) do
    t.config
    |> Config.sequencer()
    |> case do
      nil -> t
      sequencer -> t |> send_rejoin_invitation_to_sequencer(sequencer)
    end
  end

  @spec send_rejoin_invitation_to_sequencer(t(), sequencer :: GenServer.name()) :: t()
  def send_rejoin_invitation_to_sequencer(t, sequencer) do
    Sequencer.invite_to_rejoin(sequencer, self(), t.epoch)
    t |> add_expected_service(sequencer, :sequencer)
  end

  # Data Distributor

  @spec try_to_invite_old_data_distributor(t()) :: t()
  def try_to_invite_old_data_distributor(t) do
    t.config
    |> Config.data_distributor()
    |> case do
      nil -> t
      data_distributor -> t |> send_rejoin_invitation_to_data_distributor(data_distributor)
    end
  end

  @spec send_rejoin_invitation_to_data_distributor(t(), data_distributor :: GenServer.name()) ::
          t()
  def send_rejoin_invitation_to_data_distributor(t, data_distributor) do
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
