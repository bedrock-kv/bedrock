defmodule Bedrock.Cluster.Monitor.PingPong do
  alias Bedrock.ControlPlane.Director

  import Bedrock.Cluster.Monitor.State,
    only: [
      put_missed_pongs: 2,
      update_missed_pongs: 2
    ]

  use Bedrock.Internal.TimerManagement

  def ping_director(t) when t.director != :unavailable do
    :ok = Director.send_ping(t.director)
    t
  end

  def pong_received(t), do: t |> reset_missed_pongs()

  def pong_missed(t), do: t |> update_missed_pongs(&(&1 + 1))

  def reset_missed_pongs(t), do: t |> put_missed_pongs(0)

  def reset_ping_timer(t) do
    t
    |> cancel_timer(:ping)
    |> maybe_set_ping_timer()
  end

  def maybe_set_ping_timer(%{director: :unavailable} = t), do: t

  def maybe_set_ping_timer(t),
    do: t |> set_timer(:ping, t.cluster.monitor_ping_timeout_in_ms())
end
