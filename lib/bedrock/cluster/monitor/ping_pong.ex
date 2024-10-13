defmodule Bedrock.Cluster.Monitor.PingPong do
  alias Bedrock.Cluster.Monitor.State
  alias Bedrock.ControlPlane.ClusterController

  use Bedrock.Internal.TimerManagement, type: State.t()

  def ping_cluster_controller_if_available(t) when t.controller != :unavailable do
    :ok = ClusterController.send_ping(t.controller)
    t
  end

  def ping_cluster_controller_if_available(t), do: t

  def pong_received(t) do
    t
    |> reset_missed_pongs()
    |> cancel_timer()
    |> maybe_set_ping_timer()
  end

  def pong_missed(t), do: update_in(t.missed_pongs, &(&1 + 1))

  def reset_missed_pongs(t), do: put_in(t.missed_pongs, 0)

  def maybe_set_ping_timer(%{controller: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.monitor_ping_timeout_in_ms())
end
