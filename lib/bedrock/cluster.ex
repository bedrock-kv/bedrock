defmodule Bedrock.Cluster do
  use Supervisor

  alias Bedrock.Cluster
  alias Bedrock.Client
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Transaction

  require Logger

  @type t :: module()
  @type name :: String.t()
  @type version :: Transaction.version()
  @type transaction :: Transaction.t()
  @type storage :: Storage.ref()
  @type log :: Log.ref()
  @type capability :: :coordination | :log | :storage

  @callback name() :: String.t()
  @callback fetch_config() :: {:ok, Config.t()} | {:error, :unavailable}
  @callback config!() :: Config.t()
  @callback node_config() :: Keyword.t()
  @callback capabilities() :: [Bedrock.Cluster.capability()]
  @callback path_to_descriptor() :: Path.t()
  @callback coordinator_ping_timeout_in_ms() :: non_neg_integer()
  @callback monitor_ping_timeout_in_ms() :: non_neg_integer()
  @callback otp_name() :: atom()
  @callback otp_name(service :: atom()) :: atom()
  @callback fetch_controller() :: {:ok, ClusterController.ref()} | {:error, :unavailable}
  @callback controller!() :: ClusterController.ref()
  @callback fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
  @callback coordinator!() :: Coordinator.ref()
  @callback fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
  @callback coordinator_nodes!() :: [node()]
  @callback client() :: {:ok, Bedrock.Client.t()} | {:error, :unavailable}

  @doc false
  defmacro __using__(opts) do
    otp_app = opts[:otp_app] || raise "Missing :otp_app option"
    name = opts[:name] || raise "Missing :name option"

    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote location: :keep do
      @behaviour Bedrock.Cluster
      alias Bedrock.Cluster
      alias Bedrock.Client
      alias Bedrock.Cluster.Monitor
      alias Bedrock.ControlPlane.Config

      @default_coordinator_ping_timeout_in_ms 300
      @default_monitor_ping_timeout_in_ms 300

      @name unquote(name)
      @otp_name Cluster.otp_name(@name)

      @sup_otp_name Cluster.otp_name(@name, :sup)

      @controller_otp_name Cluster.otp_name(@name, :controller)
      @coordinator_otp_name Cluster.otp_name(@name, :coordinator)
      @data_distributor_otp_name Cluster.otp_name(@name, :data_distributor)
      @log_otp_name Cluster.otp_name(@name, :log)
      @monitor_otp_name Cluster.otp_name(@name, :monitor)
      @sequencer_otp_name Cluster.otp_name(@name, :sequencer)
      @storage_otp_name Cluster.otp_name(@name, :storage)

      @doc """
      Get the name of the cluster.
      """
      @impl Bedrock.Cluster
      @spec name() :: Cluster.name()
      def name, do: @name

      ######################################################################
      # Configuration
      ######################################################################

      @doc """
      Fetch the configuration for the cluster.
      """
      @impl Bedrock.Cluster
      @spec fetch_config() :: {:ok, Config.t()} | {:error, :unavailable}
      def fetch_config, do: Cluster.fetch_config(__MODULE__)

      @doc """
      Fetch the configuration for the cluster.
      """
      @impl Bedrock.Cluster
      @spec config!() :: Config.t()
      def config!, do: Cluster.config!(__MODULE__)

      @doc """
      Get the configuration for this node of the cluster.
      """
      @impl Bedrock.Cluster
      @spec node_config() :: Keyword.t()
      def node_config, do: Application.get_env(unquote(otp_app), __MODULE__, [])

      @doc """
      Get the capability advertised to the cluster by this node.
      """
      @impl Bedrock.Cluster
      @spec capabilities() :: [Cluster.capability()]
      def capabilities, do: node_config() |> Keyword.get(:capabilities, [])

      @doc """
      Get the path to the descriptor file. If the path is not set in the
      configuration, we default to a file named
      "#{Cluster.default_descriptor_file_name()}" in the `priv` directory for the
      application.
      """
      @impl Bedrock.Cluster
      @spec path_to_descriptor() :: Path.t()
      def path_to_descriptor, do: Cluster.path_to_descriptor(__MODULE__, unquote(otp_app))

      @doc """
      Get the timeout (in milliseconds) for pinging the coordinator.
      """
      @impl Bedrock.Cluster
      @spec coordinator_ping_timeout_in_ms() :: non_neg_integer()
      def coordinator_ping_timeout_in_ms do
        node_config()
        |> Keyword.get(:coordinator_ping_timeout_in_ms, @default_coordinator_ping_timeout_in_ms)
      end

      @doc """
      Get the timeout (in milliseconds) for a monitor process waiting to
      receives a ping from the currently active cluster controller. Once a
      monitor has successfully joined a cluster, it will wait for a ping from
      the controller. If it does not receive a ping within the timeout, it
      will attempt to find a new controller.
      """
      @impl Bedrock.Cluster
      @spec monitor_ping_timeout_in_ms() :: non_neg_integer()
      def monitor_ping_timeout_in_ms do
        node_config()
        |> Keyword.get(:monitor_ping_timeout_in_ms, @default_monitor_ping_timeout_in_ms)
      end

      ######################################################################
      # OTP Names
      ######################################################################

      @doc """
      Get the OTP name for the cluster.
      """
      @impl Bedrock.Cluster
      @spec otp_name() :: atom()
      def otp_name, do: @otp_name

      @doc """
      Get the OTP name for a component within the cluster. These names are
      limited in scope to the current node.
      """
      @impl Bedrock.Cluster
      @spec otp_name(
              :sup
              | :controller
              | :coordinator
              | :data_distributor
              | :monitor
              | :sequencer
              | :storage
              | :log
            ) :: atom()
      def otp_name(:sup), do: @sup_otp_name
      def otp_name(:controller), do: @controller_otp_name
      def otp_name(:coordinator), do: @coordinator_otp_name
      def otp_name(:data_distributor), do: @data_distributor_otp_name
      def otp_name(:log), do: @log_otp_name
      def otp_name(:monitor), do: @monitor_otp_name
      def otp_name(:sequencer), do: @sequencer_otp_name
      def otp_name(:storage), do: @storage_otp_name
      def otp_name(component) when is_atom(component), do: Cluster.otp_name(@name, component)

      ######################################################################
      # Cluster Services
      ######################################################################

      @doc """
      Fetch the current controller for the cluster. If we can't find one, we
      return an error.
      """
      @impl Bedrock.Cluster
      @spec fetch_controller() :: {:ok, ClusterController.ref()} | {:error, :unavailable}
      def fetch_controller, do: otp_name(:monitor) |> Monitor.fetch_controller()

      @doc """
      Get the current controller for the cluster. If we can't find one, we
      raise an error.
      """
      @impl Bedrock.Cluster
      @spec controller!() :: ClusterController.ref()
      def controller!, do: Cluster.controller!(__MODULE__)

      @doc """
      Fetch a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we return an error.
      """
      @impl Bedrock.Cluster
      @spec fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
      def fetch_coordinator, do: otp_name(:monitor) |> Monitor.fetch_coordinator()

      @doc """
      Get a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we raise an error.
      """
      @impl Bedrock.Cluster
      @spec coordinator!() :: Coordinator.ref()
      def coordinator!, do: Cluster.coordinator!(__MODULE__)

      @doc """
      Fetch the nodes that are running coordinators for the cluster.
      """
      @impl Bedrock.Cluster
      @spec fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
      def fetch_coordinator_nodes, do: otp_name(:monitor) |> Monitor.fetch_coordinator_nodes()

      @doc """
      Get the nodes that are running coordinators for the cluster. If we can't
      find any, we raise an error.
      """
      @impl Bedrock.Cluster
      @spec coordinator_nodes!() :: [node()]
      def coordinator_nodes!, do: Cluster.coordinator_nodes!(__MODULE__)

      @doc """
      Get a new instance of the `Client` configured for the cluster.
      """
      @impl Bedrock.Cluster
      @spec client() :: {:ok, Client.t()} | {:error, :unavailable}
      def client, do: Cluster.client(__MODULE__)

      @doc false
      def child_spec(opts), do: Cluster.child_spec([{:cluster, __MODULE__} | opts])

      @doc false
      def ping_nodes(nodes, cluster_controller, epoch),
        do: otp_name(:monitor) |> Monitor.ping_nodes(nodes, cluster_controller, epoch)
    end
  end

  @doc false
  def default_descriptor_file_name, do: "bedrock.cluster"

  @doc """
  Get the OTP name for the cluster with the given name.
  """
  @spec otp_name(name()) :: atom()
  def otp_name(cluster_name) when is_binary(cluster_name), do: :"bedrock_#{cluster_name}"

  @doc """
  Get the OTP name for a component within the cluster with the given name.
  """
  @spec otp_name(name(), service :: atom()) :: atom()
  def otp_name(cluster_name, service) when is_binary(cluster_name),
    do: :"#{otp_name(cluster_name)}_#{service}"

  @doc false
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"

    path_to_descriptor = opts[:path_to_descriptor] || cluster.path_to_descriptor()

    descriptor =
      with :ok <- check_node_is_in_a_cluster(),
           {:ok, descriptor} <- path_to_descriptor |> Descriptor.read_from_file(),
           :ok <- check_descriptor_is_for_cluster(descriptor, cluster) do
        descriptor
      else
        {:error, :descriptor_not_for_this_cluster} ->
          Logger.warning(
            "Bedrock: The cluster name in the descriptor file does not match the cluster name (#{cluster.name()}) in the configuration."
          )

          nil

        {:error, :not_in_a_cluster} ->
          Logger.warning(
            "Bedrock: This node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"
          )

        {:error, _reason} ->
          nil
      end
      |> case do
        nil ->
          Logger.warning("Bedrock: Creating a default single-node configuration")
          Descriptor.new(cluster.name(), [Node.self()])

        descriptor ->
          descriptor
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

  @spec check_node_is_in_a_cluster() :: :ok | {:error, :not_in_a_cluster}
  defp check_node_is_in_a_cluster,
    do: if(Node.self() != :nonode@nohost, do: :ok, else: {:error, :not_in_a_cluster})

  @spec check_descriptor_is_for_cluster(Descriptor.t(), module()) ::
          :ok | {:error, :descriptor_not_for_this_cluster}
  defp check_descriptor_is_for_cluster(descriptor, cluster),
    do:
      if(descriptor.cluster_name == cluster.name(),
        do: :ok,
        else: {:error, :descriptor_not_for_this_cluster}
      )

  @doc false
  @impl Supervisor
  def init({cluster, path_to_descriptor, descriptor}) do
    capabilities = cluster.capabilities()

    :ok = Bedrock.Internal.Logging.start()

    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)},
        {Bedrock.Cluster.PubSub, otp_name: cluster.otp_name(:pub_sub)},
        {Bedrock.Cluster.Monitor,
         [
           cluster: cluster,
           descriptor: descriptor,
           path_to_descriptor: path_to_descriptor,
           otp_name: cluster.otp_name(:monitor),
           capabilities: capabilities,
           mode: mode_for_capabilities(capabilities)
         ]}
        | children_for_capabilities(cluster, capabilities)
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp mode_for_capabilities([]), do: :passive
  defp mode_for_capabilities(_), do: :active

  defp children_for_capabilities(_cluster, []), do: []

  defp children_for_capabilities(cluster, capabilities) do
    capabilities
    |> Enum.map(fn capability ->
      {module_for_capability(capability),
       [
         {:cluster, cluster}
         | cluster.node_config()
           |> Keyword.get(capability, [])
       ]}
    end)
  end

  defp module_for_capability(:coordination), do: Bedrock.ControlPlane.Coordinator
  defp module_for_capability(:storage), do: Bedrock.Service.StorageController
  defp module_for_capability(:log), do: Bedrock.Service.LogController
  defp module_for_capability(capability), do: raise("Unknown capability: #{inspect(capability)}")

  @spec fetch_config(module()) :: {:ok, Config.t()} | {:error, :unavailable}
  def fetch_config(module) do
    with {:ok, coordinator} <- module.fetch_coordinator() do
      Coordinator.fetch_config(coordinator)
    end
  end

  @spec config!(module()) :: Config.t()
  def config!(module) do
    fetch_config(module)
    |> case do
      {:ok, config} -> config
      {:error, _} -> raise "No configuration available"
    end
  end

  @spec controller!(module()) :: ClusterController.ref()
  def controller!(module) do
    module.fetch_controller()
    |> case do
      {:ok, controller} -> controller
      {:error, _} -> raise "No controller available"
    end
  end

  @spec coordinator!(module()) :: Coordinator.ref()
  def coordinator!(module) do
    module.fetch_coordinator()
    |> case do
      {:ok, coordinator} -> coordinator
      {:error, _} -> raise "No coordinator available"
    end
  end

  @spec coordinator_nodes!(module()) :: [node()]
  def coordinator_nodes!(module) do
    module.fetch_coordinator_nodes()
    |> case do
      {:ok, coordinator_nodes} -> coordinator_nodes
      {:error, _} -> raise "No coordinator nodes available"
    end
  end

  @spec path_to_descriptor(module(), otp_app :: atom()) :: Path.t()
  def path_to_descriptor(module, otp_app) do
    module.node_config()
    |> Keyword.get(
      :path_to_descriptor,
      Path.join(
        Application.app_dir(otp_app, "priv"),
        default_descriptor_file_name()
      )
    )
  end

  @spec client(module()) :: {:ok, Client.t()} | {:error, :unavailable}
  def client(module) do
    with {:ok, coordinator} <- module.coordinator() do
      Client.new(coordinator)
    end
  end
end
