defmodule Bedrock.ControlPlane.Coordinator.RaftAdapter do
  @moduledoc false
  @behaviour Bedrock.Raft.Interface

  defp determine_timeout(min_ms, max_ms) when min_ms == max_ms, do: min_ms
  defp determine_timeout(min_ms, max_ms) when min_ms > max_ms, do: raise("invalid_timeout")
  defp determine_timeout(min_ms, max_ms), do: min_ms + :rand.uniform(max_ms - min_ms)

  @impl true
  def heartbeat_ms, do: 50

  @impl true
  def timestamp_in_ms, do: :erlang.monotonic_time(:millisecond)

  @impl true
  def ignored_event(_event, _from), do: :ok

  @impl true
  def leadership_changed(leadership),
    do: send(self(), {:raft, :leadership_changed, leadership})

  @impl true
  def send_event(to, event) do
    send(self(), {:raft, :send_rpc, event, to})
    :ok
  end

  @impl true
  def timer(:heartbeat), do: set_timer(:heartbeat, heartbeat_ms(), 0)
  def timer(:election), do: set_timer(:election, 150, 50)

  defp set_timer(name, min_ms, jitter) do
    determine_timeout(min_ms, min_ms + jitter)
    |> :timer.send_after({:raft, :timer, name})
    |> case do
      {:ok, ref} ->
        fn -> :timer.cancel(ref) end

      {:error, _} ->
        raise "Bedrock: failed to start timer for raft #{inspect(name)}"

        fn -> :ok end
    end
  end

  @impl true
  def consensus_reached(log, transaction_id) do
    send(self(), {:raft, :consensus_reached, log, transaction_id})
    :ok
  end
end
