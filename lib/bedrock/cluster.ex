defmodule Bedrock.Cluster do
  use GenServer
  use Bedrock, :types

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Coordinator

  @type version :: Bedrock.DataPlane.Transaction.version()
  @type transaction :: Bedrock.DataPlane.Transaction.t()
  @type storage_engine :: GenServer.name()
  @type log_system_engine :: GenServer.name()

  @coordinator_ping_timeout_in_ms 300

  require Logger

  @type t :: %__MODULE__{
          cluster: Module.t(),
          descriptor: Descriptor.t(),
          coordinator: pid() | nil,
          coordinator_ref: reference() | nil
        }

  defstruct cluster: nil,
            descriptor: nil,
            coordinator: nil,
            coordinator_ref: nil

  defmacro __using__(:types) do
    quote do
      @type version :: Bedrock.Cluster.version()
      @type transaction :: Bedrock.Cluster.transaction()
      @type storage_engine :: Bedrock.Cluster.storage_engine()
      @type log_system_engine :: Bedrock.Cluster.log_system_engine()
    end
  end

  defmacro __using__(opts) do
    otp_app = opts[:otp_app] || raise "Missing :otp_app option"
    name = opts[:name] || raise "Missing :name option"

    quote do
      alias Bedrock.Cluster

      @descriptor_file_name "bedrock.cluster"
      @name unquote(name)
      @otp_name Cluster.otp_name(@name)

      @sup_otp_name Cluster.otp_name(@name, :sup)

      @controller_otp_name Cluster.otp_name(@name, :controller)
      @coordinator_otp_name Cluster.otp_name(@name, :coordinator)
      @data_distributor_otp_name Cluster.otp_name(@name, :data_distributor)
      @log_system_otp_name Cluster.otp_name(@name, :log_system)
      @sequencer_otp_name Cluster.otp_name(@name, :sequencer)
      @storage_system_otp_name Cluster.otp_name(@name, :storage_system)
      @worker_otp_name Cluster.otp_name(@name, :worker)

      @doc """
      Get the name of the cluster.
      """
      @spec name() :: String.t()
      def name, do: @name

      @doc """
      Get the path to the descriptor file. If the path is not set in the
      configuration, we default to a file named "#{@descriptor_file_name}" in
      the `priv` directory for the application.
      """
      @spec path_to_descriptor() :: Path.t()
      def path_to_descriptor do
        config()[:path_to_descriptor] ||
          Path.join(
            Application.app_dir(unquote(otp_app), "priv"),
            @descriptor_file_name
          )
      end

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
              | :log_system
              | :sequencer
              | :storage_system
              | :worker
            ) :: atom()
      def otp_name(:sup), do: @sup_otp_name
      def otp_name(:controller), do: @controller_otp_name
      def otp_name(:coordinator), do: @coordinator_otp_name
      def otp_name(:data_distributor), do: @data_distributor_otp_name
      def otp_name(:log_system), do: @log_system_otp_name
      def otp_name(:sequencer), do: @sequencer_otp_name
      def otp_name(:storage_system), do: @storage_system_otp_name
      def otp_name(:worker), do: @worker_otp_name
      def otp_name(component) when is_atom(component), do: Cluster.otp_name(@name, component)

      @doc """
      Get the current controller for the cluster. If we can't find one, we
      return an error.
      """
      @spec controller() :: {:ok, GenServer.name()} | {:error, :unavailable}
      def controller, do: Cluster.get_controller(__MODULE__)

      @doc """
      Get a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we return an error.
      """
      @spec coordinator() :: {:ok, GenServer.name()} | {:error, :unavailable}
      def coordinator, do: Cluster.get_coordinator(__MODULE__)

      @doc """
      Get the nodes that are running coordinators for the cluster.
      """
      @spec coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
      def coordinator_nodes, do: Cluster.get_coordinator_nodes(__MODULE__)

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

      @spec config() :: Keyword.t()
      def config, do: Application.get_env(unquote(otp_app), __MODULE__, [])
    end
  end

  @doc """
  Get a coordinator for the given cluster.
  """
  @spec get_coordinator(cluster :: Module.t()) :: {:ok, pid()} | {:error, :unavailable}
  def get_coordinator(cluster) do
    Process.whereis(cluster.otp_name(:coordinator))
    |> case do
      nil -> GenServer.call(cluster.otp_name(), :get_coordinator)
      pid -> {:ok, pid}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the current controller for the cluster. If we can't find one, we
  return an error.
  """
  @spec get_controller(cluster :: Module.t()) :: {:ok, pid()} | {:error, :unavailable}
  def get_controller(cluster) do
    Process.whereis(cluster.otp_name(:controller))
    |> case do
      nil ->
        with {:ok, coordinator} <- cluster.coordinator() do
          Coordinator.get_controller(coordinator)
        end

      pid ->
        {:ok, pid}
    end
  end

  @doc """
  Get the nodes that are running coordinators for the given cluster.
  """
  @spec get_coordinator_nodes(cluster :: Module.t()) :: {:ok, [node()]} | {:error, :unavailable}
  def get_coordinator_nodes(cluster) do
    GenServer.call(cluster.otp_name(), :get_coordinator_nodes)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the OTP name for the cluster with the given name.
  """
  @spec otp_name(cluster_name :: binary()) :: atom()
  def otp_name(cluster_name) when is_binary(cluster_name), do: :"bedrock_#{cluster_name}"

  @doc """
  Get the OTP name for a service within the cluster with the given name.
  """
  @spec otp_name(
          cluster_name :: binary(),
          service ::
            :sup
            | :controller
            | :coordinator
            | :data_distributor
            | :log_system
            | :sequencer
            | :storage_system
            | :worker
        ) :: atom()
  def otp_name(cluster_name, service) when is_binary(cluster_name),
    do: :"#{otp_name(cluster_name)}_#{service}"

  @doc """
  Return an appropriately configured child specification for a cluster.
  """
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    logger = Module.concat(cluster, Logger)

    cluster_name = cluster.name()

    path_to_descriptor = opts[:path_to_descriptor] || cluster.path_to_descriptor()

    descriptor =
      path_to_descriptor
      |> Descriptor.read_from_file()
      |> case do
        {:ok, descriptor} ->
          if Node.self() == :nonode@nohost do
            raise "Bedrock: this node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"
          end

          descriptor

        {:error, _reason} ->
          logger.warning("Bedrock: Creating a default single-cluster configuration")

          Descriptor.new(cluster_name, [Node.self()])
      end

    if cluster_name != descriptor.cluster_name do
      raise """
      The cluster name in the descriptor file (#{descriptor.cluster_name}) does not match the cluster name (#{cluster_name}) in the configuration.
      """
    end

    otp_name = cluster.otp_name()

    %{
      id: otp_name,
      start: {
        GenServer,
        :start_link,
        [__MODULE__, {cluster, descriptor}, [name: otp_name]]
      },
      restart: :permanent
    }
  end

  @impl GenServer
  def init({cluster, descriptor}),
    do: {:ok, %__MODULE__{cluster: cluster, descriptor: descriptor}}

  @impl GenServer
  def handle_call(:get_coordinator, _from, %{coordinator: nil} = t) do
    find_a_live_coordinator(t)
    |> case do
      {:ok, coordinator} ->
        coordinator_ref = Process.monitor(coordinator)

        {:reply, {:ok, coordinator},
         %{t | coordinator: coordinator, coordinator_ref: coordinator_ref}}

      {:error, _reason} ->
        {:reply, {:error, :unavailable}, t}
    end
  end

  def handle_call(:get_coordinator, _from, t),
    do: {:reply, {:ok, t.coordinator}, t}

  def handle_call(:get_coordinator_nodes, _from, t),
    do: {:reply, {:ok, t.descriptor.coordinator_nodes}, t}

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _process, _reason}, t)
      when t.coordinator_ref == ref,
      do: {:noreply, %{t | coordinator: nil, coordinator_ref: nil}}

  # Find a live coordinator. We make a ping call to all of the nodes that we
  # know about and return the first one that responds. If none respond, we
  # return an error.
  #
  @doc false
  @spec find_a_live_coordinator(t()) :: {:ok, {atom(), node()}} | {:error, :unavailable}
  def find_a_live_coordinator(t) do
    coordinator_otp_name = t.cluster.otp_name(:coordinator)

    GenServer.multi_call(
      t.descriptor.coordinator_nodes,
      coordinator_otp_name,
      :ping,
      @coordinator_ping_timeout_in_ms
    )
    |> case do
      {[], _failures} ->
        {:error, :unavailable}

      {[{first_node, :pong} | _other_coordinators], _failures} ->
        {:ok, {coordinator_otp_name, first_node}}
    end
  end
end
