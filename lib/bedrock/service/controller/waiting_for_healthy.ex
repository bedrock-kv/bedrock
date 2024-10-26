defmodule Bedrock.Service.Controller.WaitingForHealthy do
  alias Bedrock.Service.Controller.State

  @spec add_pid_to_waiting_for_healthy(State.t(), pid()) :: State.t()
  def add_pid_to_waiting_for_healthy(t, pid),
    do: %{t | waiting_for_healthy: [pid | t.waiting_for_healthy]}

  @spec notify_waiting_for_healthy(State.t()) :: State.t()
  def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
      when waiting_for_healthy != [] do
    Enum.each(t.waiting_for_healthy, fn from -> GenServer.reply(from, :ok) end)

    %{t | waiting_for_healthy: []}
  end

  def notify_waiting_for_healthy(t), do: t
end
