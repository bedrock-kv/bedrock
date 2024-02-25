defmodule Bedrock.Cluster.Monitor do
  use GenServer

  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Cluster.PubSub

  require Logger

  @type monitor :: GenServer.name()
  @type controller :: GenServer.name()

  @type t :: %__MODULE__{
          cluster: Module.t(),
          descriptor: Descriptor.t(),
          coordinator: Coordinator.name() | :unavailable,
          controller: ClusterController.name() | :unavailable
        }

  defstruct ~w[
    cluster
    descriptor
    coordinator
    controller
    timer_ref
  ]a

  @doc """
  Get a coordinator for the cluster. We ask the running instance of the cluster
  monitor to find one for us.
  """
  @spec get_coordinator(monitor()) :: {:ok, Coordinator.name()} | {:error, :unavailable}
  def get_coordinator(monitor) do
    monitor |> GenServer.call(:get_coordinator)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the current controller for the cluster.
  """
  @spec get_controller(monitor()) :: {:ok, ClusterController.name()} | {:error, :unavailable}
  def get_controller(monitor) do
    monitor |> GenServer.call(:get_controller)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Get the nodes that are running coordinators for the given cluster.
  """
  @spec get_coordinator_nodes(monitor()) :: {:ok, [node()]} | {:error, :unavailable}
  def get_coordinator_nodes(monitor) do
    monitor |> GenServer.call(:get_coordinator_nodes)
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
      coordinator: :unavailable,
      controller: :unavailable
    }

    {:ok, t, {:continue, :find_a_live_coordinator}}
  end

  @impl GenServer
  def handle_continue(:find_a_live_coordinator, t) do
    find_a_live_coordinator(t)
    |> case do
      {:ok, coordinator} ->
        {:noreply,
         t
         |> cancel_timer()
         |> change_coordinator(coordinator), {:continue, :find_current_cluster_controller}}

      {:error, :unavailable} ->
        {:noreply,
         t
         |> cancel_timer()
         |> change_coordinator(:unavailable)
         |> set_timer(:try_to_find_a_live_coordinator)}
    end
  end

  def handle_continue(:find_current_cluster_controller, t) do
    t.coordinator
    |> Coordinator.get_controller(100)
    |> case do
      {:ok, controller} ->
        {:noreply,
         t
         |> cancel_timer()
         |> change_cluster_controller(controller)}

      {:error, :unavailable} ->
        {:noreply,
         t
         |> cancel_timer()
         |> change_cluster_controller(:unavailable)
         |> set_timer(:try_to_find_current_cluster_controller)}
    end
  end

  @impl GenServer
  def handle_call(:get_coordinator, _from, %{coordinator: :unavailable} = t),
    do: {:reply, {:error, :unavailable}, t}

  def handle_call(:get_coordinator, _from, t),
    do: {:reply, {:ok, t.coordinator}, t}

  def handle_call(:get_controller, _from, %{controller: :unavailable} = t),
    do: {:reply, {:error, :unavailable}, t}

  def handle_call(:get_controller, _from, t),
    do: {:reply, {:ok, t.controller}, t}

  def handle_call(:get_coordinator_nodes, _from, t),
    do: {:reply, {:ok, t.descriptor.coordinator_nodes}, t}

  @impl GenServer
  def handle_info(:try_to_find_a_live_coordinator, %{coordinator: :unavailable} = t),
    do: {:noreply, t, {:continue, :find_a_live_coordinator}}

  def handle_info(:try_to_find_a_live_coordinator, t),
    do: {:noreply, t}

  def handle_info(:try_to_find_current_cluster_controller, %{controller: :unavailable} = t),
    do: {:noreply, t, {:continue, :find_current_cluster_controller}}

  def handle_info(:try_to_find_current_cluster_controller, t),
    do: {:noreply, t}

  @impl GenServer
  def handle_cast({:cluster_controller_replaced, :unavailable}, t) do
    {:noreply,
     t
     |> cancel_timer()
     |> change_cluster_controller(:unavailable), {:continue, :find_current_cluster_controller}}
  end

  def handle_cast({:cluster_controller_replaced, cluster_controller}, t) do
    {:noreply,
     t
     |> cancel_timer()
     |> change_cluster_controller(cluster_controller)}
  end

  # Internals

  @doc """
  Find a live coordinator. We make a ping call to all of the nodes that we know
  about and return the first one that responds. If none respond, we return an
  error.
  """
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

  @doc """
  Change the coordinator. If the coordinator is the same as the one we already
  have we do nothing, otherwise we publish a message to a topic to let everyone
  on this node know that the coordinator has changed.
  """
  @spec change_coordinator(t(), Coordinator.name() | :unavailable) :: t()
  def change_coordinator(t, coordinator) when t.coordinator == coordinator, do: t

  def change_coordinator(t, coordinator) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator})
    %{t | coordinator: coordinator}
  end

  @doc """
  Change the cluster controller. If the controller is the same as the one we
  already have we do nothing, otherwise we publish a message to a topic to let
  everyone on this node know that the controller has changed.
  """
  @spec change_cluster_controller(t(), ClusterController.name() | :unavailable) :: t()
  def change_cluster_controller(t, controller) when t.controller == controller, do: t

  def change_cluster_controller(t, controller)
      when controller == :unavailable or is_tuple(controller) do
    Logger.debug("Bedrock [#{t.cluster.name()}]: Controller changed to #{inspect(controller)}")

    PubSub.publish(
      t.cluster,
      :cluster_controller_replaced,
      {:cluster_controller_replaced, controller}
    )

    %{t | controller: controller}
  end

  @spec cancel_timer(t()) :: t()
  def cancel_timer(%{timer_ref: nil} = t), do: t

  def cancel_timer(%{timer_ref: timer_ref} = t) do
    Process.cancel_timer(timer_ref)
    %{t | timer_ref: nil}
  end

  @spec set_timer(t(), timer_name :: atom()) :: t()
  def set_timer(%{timer_ref: nil} = t, timer_name) do
    %{
      t
      | timer_ref:
          Process.send_after(
            self(),
            timer_name,
            t.cluster.coordinator_ping_timeout_in_ms()
          )
    }
  end
end
