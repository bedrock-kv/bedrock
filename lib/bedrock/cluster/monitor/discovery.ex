defmodule Bedrock.Cluster.Monitor.Discovery do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator

  use Bedrock.Internal.TimerManagement, type: State.t()

  import Bedrock.Cluster.Monitor.State,
    only: [
      set_coordinator: 2
    ]

  require Logger

  @doc """
  Find a live coordinator. We make a ping call to all of the nodes that we know
  about and return the first one that responds. If none respond, we return an
  error.
  """
  @spec find_a_live_coordinator(State.t()) :: {:ok, {atom(), node()}} | {:error, :unavailable}
  def find_a_live_coordinator(t) do
    coordinator_otp_name = t.cluster.otp_name(:coordinator)

    if t.node in t.descriptor.coordinator_nodes do
      {:ok, coordinator_otp_name}
    else
      GenServer.multi_call(
        t.descriptor.coordinator_nodes,
        coordinator_otp_name,
        :ping,
        t.cluster.coordinator_ping_timeout_in_ms()
      )
      |> case do
        {[], _failures} ->
          {:error, :unavailable}

        {[{_first_node, {:pong, coordinator_pid}} | _other_coordinators], _failures} ->
          {:ok, coordinator_pid}
      end
    end
  end

  @doc """
  Change the coordinator. If the coordinator is the same as the one we already
  have we do nothing, otherwise we publish a message to a topic to let everyone
  on this node know that the coordinator has changed.
  """

  @spec change_coordinator(State.t(), Coordinator.ref() | :unavailable) :: State.t()
  def change_coordinator(t, coordinator) when t.coordinator == coordinator, do: t

  def change_coordinator(t, :unavailable), do: t |> set_coordinator(:unavailable)

  def change_coordinator(t, coordinator) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator})

    t
    |> set_coordinator(coordinator)
    |> start_monitoring_coordinator()
  end

  def start_monitoring_coordinator(t) do
    Process.monitor(t.coordinator)
    t
  end

  @doc """
  Change the cluster controller. If the controller is the same as the one we
  already have we do nothing, otherwise we publish a message to a topic to let
  everyone on this node know that the controller has changed.
  """
  @spec change_cluster_controller(State.t(), ClusterController.ref() | :unavailable) :: State.t()
  def change_cluster_controller(t, controller) when t.controller == controller,
    do: t |> cancel_timer() |> maybe_set_ping_timer()

  def change_cluster_controller(t, controller) do
    put_in(t.controller, controller)
    |> cancel_timer()
    |> maybe_set_ping_timer()
    |> notify_cluster_controller_changed()
  end

  def notify_cluster_controller_changed(t) do
    send(self(), :cluster_controller_replaced)
    t
  end

  def maybe_set_ping_timer(%{controller: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.monitor_ping_timeout_in_ms())
end
