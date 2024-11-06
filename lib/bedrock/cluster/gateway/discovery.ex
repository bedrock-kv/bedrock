defmodule Bedrock.Cluster.Gateway.Discovery do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Coordinator

  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Gateway.State,
    only: [
      put_coordinator: 2
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

  def change_coordinator(t, :unavailable), do: t |> put_coordinator(:unavailable)

  def change_coordinator(t, coordinator) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator})

    t
    |> put_coordinator(coordinator)
    |> start_monitoring_coordinator()
  end

  def start_monitoring_coordinator(t) do
    Process.monitor(t.coordinator)
    t
  end

  def maybe_set_ping_timer(%{director: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.gateway_ping_timeout_in_ms())
end
