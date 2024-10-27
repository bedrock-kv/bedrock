defmodule Bedrock.Cluster.Monitor.PingPong do
  alias Bedrock.ControlPlane.ClusterController

  import Bedrock.Cluster.Monitor.State,
    only: [
      put_missed_pongs: 2,
      update_missed_pongs: 2
    ]

  use Bedrock.Internal.TimerManagement

  def ping_cluster_controller_if_available(t) when t.controller != :unavailable do
    :ok = ClusterController.send_ping(t.controller)
    t
  end

  def ping_cluster_controller_if_available(t), do: t

  def pong_received(t) do
    t
    |> reset_missed_pongs()
    |> cancel_timer(:ping)
    |> maybe_set_ping_timer()
  end

  def pong_missed(t), do: t |> update_missed_pongs(&(&1 + 1))

  def reset_missed_pongs(t), do: t |> put_missed_pongs(0)

  def maybe_set_ping_timer(%{controller: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.monitor_ping_timeout_in_ms())
end
