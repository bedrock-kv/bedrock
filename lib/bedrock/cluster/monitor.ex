defmodule Bedrock.Cluster.Monitor do
  use GenServer

  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Cluster.PubSub

  require Logger

  @type coordinator :: GenServer.name()

  @type t :: %__MODULE__{
          cluster: Module.t(),
          descriptor: Descriptor.t(),
          coordinator: coordinator() | :unavailable
        }

  defstruct ~w[
    cluster
    descriptor
    coordinator
  ]a

  @doc """
  Get a coordinator for the given cluster. First, we check the current node
  to see if we're running an instance locally, if not, we ask the running
  instance of the cluster monitor to find one for us.
  """
  @spec get_coordinator(cluster :: Module.t()) :: {:ok, coordinator()} | {:error, :unavailable}
  def get_coordinator(cluster) do
    cluster.otp_name(:coordinator)
    |> Process.whereis()
    |> case do
      nil -> GenServer.call(cluster.otp_name(:monitor), :get_coordinator)
      pid -> {:ok, pid}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the current controller for the cluster.
  """
  @spec get_controller(cluster :: Module.t()) :: {:ok, coordinator()} | {:error, :unavailable}
  def get_controller(cluster) do
    cluster
    |> get_coordinator()
    |> case do
      {:ok, coordinator} -> Coordinator.get_controller(coordinator)
      {:error, _reason} = reason -> reason
    end
  end

  @doc """
  Get the nodes that are running coordinators for the given cluster.
  """
  @spec get_coordinator_nodes(cluster :: Module.t()) :: {:ok, [node()]} | {:error, :unavailable}
  def get_coordinator_nodes(cluster) do
    cluster.otp_name(:monitor)
    |> GenServer.call(:get_coordinator_nodes)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Return an appropriately configured child specification for a cluster.
  """
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    descriptor = opts[:descriptor] || raise "Missing :descriptor option"
    otp_name = cluster.otp_name(:monitor)

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

  # GenServer

  @impl GenServer
  def init({cluster, descriptor}) do
    t = %__MODULE__{
      cluster: cluster,
      descriptor: descriptor,
      coordinator: :unavailable
    }

    {:ok, t, {:continue, :find_a_live_coordinator}}
  end

  @impl GenServer
  def handle_continue(:find_a_live_coordinator, t) do
    find_a_live_coordinator(t)
    |> case do
      {:ok, coordinator} ->
        {:reply, t |> change_coordinator(coordinator)}

      {:error, :unavailable} ->
        Process.send_after(
          self(),
          :try_to_find_a_live_coordinator,
          t.cluster.coordinator_ping_timeout_in_ms()
        )

        {:noreply, t |> change_coordinator(:unavailable)}
    end
  end

  @impl GenServer
  def handle_call(:get_coordinator, _from, %{coordinator: :unavailable} = t),
    do: {:reply, {:error, :unavailable}, t}

  def handle_call(:get_coordinator, _from, t),
    do: {:reply, {:ok, t.coordinator}, t}

  def handle_call(:get_coordinator_nodes, _from, t),
    do: {:reply, {:ok, t.descriptor.coordinator_nodes}, t}

  @impl GenServer
  def handle_info(:try_to_find_a_live_coordinator, %{coordinator: :unavailable} = t),
    do: {:noreply, t, {:continue, :find_a_live_coordinator}}

  def handle_info(:try_to_find_a_live_coordinator, t),
    do: {:noreply, t}

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
      t.cluster.coordinator_ping_timeout_in_ms()
    )
    |> case do
      {[], _failures} ->
        {:error, :unavailable}

      {[{first_node, :pong} | _other_coordinators], _failures} ->
        {:ok, {coordinator_otp_name, first_node}}
    end
  end

  @spec change_coordinator(t(), coordinator :: coordinator() | :unavailable) :: t()
  def change_coordinator(t, coordinator) when t.coordinator == coordinator, do: t

  def change_coordinator(t, coordinator) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator})
    %{t | coordinator: coordinator}
  end
end
