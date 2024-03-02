defmodule Bedrock.Cluster do
  use Supervisor
  use Bedrock, :types

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Descriptor

  require Logger

  @type version :: Bedrock.DataPlane.Transaction.version()
  @type transaction :: Bedrock.DataPlane.Transaction.t()
  @type storage_worker :: GenServer.name()
  @type transaction_log_worker :: GenServer.name()
  @type service :: :coordination | :transaction_log | :storage

  @doc false
  defmacro __using__(:types) do
    quote do
      @type version :: Bedrock.Cluster.version()
      @type transaction :: Bedrock.Cluster.transaction()
      @type storage_worker :: Bedrock.Cluster.storage_worker()
      @type transaction_log_worker :: Bedrock.Cluster.transaction_log_worker()
    end
  end

  @doc false
  defmacro __using__(opts) do
    otp_app = opts[:otp_app] || raise "Missing :otp_app option"
    name = opts[:name] || raise "Missing :name option"

    quote location: :keep do
      alias Bedrock.Cluster
      alias Bedrock.Cluster.Monitor

      @default_descriptor_file_name "bedrock.cluster"
      @default_coordinator_ping_timeout_in_ms 300
      @default_monitor_ping_timeout_in_ms 300

      @name unquote(name)
      @otp_name Cluster.otp_name(@name)

      @sup_otp_name Cluster.otp_name(@name, :sup)

      @controller_otp_name Cluster.otp_name(@name, :controller)
      @coordinator_otp_name Cluster.otp_name(@name, :coordinator)
      @data_distributor_otp_name Cluster.otp_name(@name, :data_distributor)
      @transaction_log_otp_name Cluster.otp_name(@name, :transaction_log)
      @monitor_otp_name Cluster.otp_name(@name, :monitor)
      @sequencer_otp_name Cluster.otp_name(@name, :sequencer)
      @service_advertiser_otp_name Cluster.otp_name(@name, :service_advertiser)
      @storage_system_otp_name Cluster.otp_name(@name, :storage_system)

      @doc """
      Get the name of the cluster.
      """
      @spec name() :: String.t()
      def name, do: @name

      ######################################################################
      # Configuration
      ######################################################################

      @doc """
      Get the configuration for this node of the cluster.
      """
      @spec config() :: Keyword.t()
      def config, do: Application.get_env(unquote(otp_app), __MODULE__, [])

      @doc """
      Get the services advertised to the cluster by this node.
      """
      @spec advertised_services() :: [Bedrock.Cluster.service()]
      def advertised_services, do: config() |> Keyword.get(:services, [])

      @doc """
      Get the path to the descriptor file. If the path is not set in the
      configuration, we default to a file named
      "#{@default_descriptor_file_name}" in the `priv` directory for the
      application.
      """
      @spec path_to_descriptor() :: Path.t()
      def path_to_descriptor do
        config()
        |> Keyword.get(
          :path_to_descriptor,
          Path.join(
            Application.app_dir(unquote(otp_app), "priv"),
            @default_descriptor_file_name
          )
        )
      end

      @doc """
      Get the timeout (in milliseconds) for pinging the coordinator.
      """
      @spec coordinator_ping_timeout_in_ms() :: non_neg_integer()
      def coordinator_ping_timeout_in_ms do
        config()
        |> Keyword.get(:coordinator_ping_timeout_in_ms, @default_coordinator_ping_timeout_in_ms)
      end

      @doc """
      Get the timeout (in milliseconds) for a monitor process waiting to
      receivea a ping from the currently active cluster controller. Once a
      monitor has successfully joined a cluster, it will wait for a ping from
      the controller. If it does not receive a ping within the timeout, it
      will attempt to find a new controller.
      """
      @spec monitor_ping_timeout_in_ms() :: non_neg_integer()
      def monitor_ping_timeout_in_ms do
        config()
        |> Keyword.get(:monitor_ping_timeout_in_ms, @default_monitor_ping_timeout_in_ms)
      end

      ######################################################################
      # OTP Names
      ######################################################################

      @doc """
      Get the OTP name for the cluster.
      """
      @spec otp_name() :: atom()
      def otp_name, do: @otp_name

      @doc """
      Get the OTP name for a component within the cluster. These names are
      limited in scope to the current node.
      """
      @spec otp_name(
              :sup
              | :controller
              | :coordinator
              | :data_distributor
              | :transaction_log
              | :monitor
              | :sequencer
              | :service_advertiser
              | :storage_system
            ) :: atom()
      def otp_name(:sup), do: @sup_otp_name
      def otp_name(:controller), do: @controller_otp_name
      def otp_name(:coordinator), do: @coordinator_otp_name
      def otp_name(:data_distributor), do: @data_distributor_otp_name
      def otp_name(:transaction_log), do: @transaction_log_otp_name
      def otp_name(:monitor), do: @monitor_otp_name
      def otp_name(:sequencer), do: @sequencer_otp_name
      def otp_name(:service_advertiser), do: @service_advertiser_otp_name
      def otp_name(:storage_system), do: @storage_system_otp_name
      def otp_name(component) when is_atom(component), do: Cluster.otp_name(@name, component)

      ######################################################################
      # Cluster Services
      ######################################################################

      @doc """
      Get the current controller for the cluster. If we can't find one, we
      return an error.
      """
      @spec controller() :: {:ok, GenServer.name()} | {:error, :unavailable}
      def controller, do: otp_name(:monitor) |> Monitor.get_controller()

      @doc """
      Get a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we return an error.
      """
      @spec coordinator() :: {:ok, GenServer.name()} | {:error, :unavailable}
      def coordinator, do: otp_name(:monitor) |> Monitor.get_coordinator()

      @doc """
      Get the nodes that are running coordinators for the cluster.
      """
      @spec coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
      def coordinator_nodes, do: otp_name(:monitor) |> Monitor.get_coordinator_nodes()

      @doc """
      Get a new instance of the `Bedrock.Client` configured for the cluster.
      """
      @spec client() :: {:ok, Bedrock.Client.t()} | {:error, :unavailable}
      def client do
        coordinator()
        |> case do
          {:ok, coordinator} -> Bedrock.Client.new(coordinator)
          {:error, _} = error -> error
        end
      end

      @doc false
      def child_spec(opts), do: Bedrock.Cluster.child_spec([{:cluster, __MODULE__} | opts])

      @doc false
      def ping_nodes(nodes, cluster_controller, epoch),
        do: otp_name(:monitor) |> Monitor.ping_nodes(nodes, cluster_controller, epoch)
    end
  end

  @doc """
  Get the OTP name for the cluster with the given name.
  """
  @spec otp_name(cluster_name :: binary()) :: atom()
  def otp_name(cluster_name) when is_binary(cluster_name), do: :"bedrock_#{cluster_name}"

  @doc """
  Get the OTP name for a component within the cluster with the given name.
  """
  @spec otp_name(cluster_name :: binary(), service :: atom()) :: atom()
  def otp_name(cluster_name, service) when is_binary(cluster_name),
    do: :"#{otp_name(cluster_name)}_#{service}"

  @doc false
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"

    cluster_name = cluster.name()

    path_to_descriptor = opts[:path_to_descriptor] || cluster.path_to_descriptor()

    descriptor =
      path_to_descriptor
      |> Descriptor.read_from_file()
      |> case do
        {:ok, descriptor} ->
          if cluster_name != descriptor.cluster_name do
            raise """
            The cluster name in the descriptor file (#{descriptor.cluster_name}) does not match the cluster name (#{cluster_name}) in the configuration.
            """
          end

          if Node.self() == :nonode@nohost do
            raise "Bedrock: this node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"
          end

          descriptor

        {:error, _reason} ->
          Logger.warning("Bedrock: Creating a default single-cluster configuration")

          Descriptor.new(cluster_name, [Node.self()])
      end

    %{
      id: __MODULE__,
      start:
        {Supervisor, :start_link,
         [
           __MODULE__,
           {cluster, path_to_descriptor, descriptor},
           []
         ]},
      restart: :permanent,
      type: :supervisor
    }
  end

  @impl Supervisor
  def init({cluster, path_to_descriptor, descriptor}) do
    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)},
        {Bedrock.Cluster.PubSub, otp_name: cluster.otp_name(:pub_sub)},
        {Bedrock.Cluster.Monitor,
         cluster: cluster,
         descriptor: descriptor,
         path_to_descriptor: path_to_descriptor,
         name: cluster.otp_name(:monitor)}
        | children_for_services(cluster, cluster.advertised_services())
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp children_for_services(_cluster, []), do: []

  defp children_for_services(cluster, services) do
    [
      {Bedrock.Cluster.ServiceAdvertiser,
       [
         cluster: cluster,
         services: services,
         otp_name: cluster.otp_name(:service_advertiser)
       ]}
      | services
        |> Enum.map(fn service ->
          {module_for_service(service),
           [
             {:cluster, cluster}
             | cluster.config()
               |> Keyword.get(service, [])
           ]}
        end)
    ]
  end

  defp module_for_service(:coordination), do: Bedrock.ControlPlane.Coordinator
  defp module_for_service(:storage), do: Bedrock.Service.Storage
  defp module_for_service(:transaction_log), do: Bedrock.Service.TransactionLog
  defp module_for_service(service), do: raise("Unknown service: #{inspect(service)}")
end
