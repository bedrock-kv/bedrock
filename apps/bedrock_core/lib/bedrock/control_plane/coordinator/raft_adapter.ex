defmodule Bedrock.ControlPlane.Coordinator.RaftAdapter do
  @moduledoc false
  @behaviour Bedrock.Raft.Interface

  require Logger

  @spec determine_timeout(pos_integer(), pos_integer()) :: pos_integer()
  defp determine_timeout(min_ms, max_ms) when min_ms == max_ms, do: min_ms
  defp determine_timeout(min_ms, max_ms) when min_ms > max_ms, do: raise("invalid_timeout")
  defp determine_timeout(min_ms, max_ms), do: min_ms + :rand.uniform(max_ms - min_ms)

  @impl true
  def heartbeat_ms, do: 100

  @impl true
  def timestamp_in_ms, do: :erlang.monotonic_time(:millisecond)

  @impl true
  def ignored_event(_event, _from), do: :ok

  @impl true
  def leadership_changed(leadership), do: send(self(), {:raft, :leadership_changed, leadership})

  @impl true
  def send_event(to, event) do
    send(self(), {:raft, :send_rpc, event, to})
    :ok
  end

  @impl true
  def timer(:heartbeat), do: set_timer(:heartbeat, heartbeat_ms(), 0)
  def timer(:election), do: set_timer(:election, 250, 100)

  @spec set_timer(atom(), pos_integer(), non_neg_integer()) :: (-> :ok | {:error, :badarg})
  defp set_timer(name, min_ms, jitter) do
    min_ms
    |> determine_timeout(min_ms + jitter)
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
  def consensus_reached(log, transaction_id, consistency) do
    send(self(), {:raft, :consensus_reached, log, transaction_id, consistency})
    :ok
  end

  @impl true
  def quorum_lost(_active_followers, _total_followers, _term), do: :step_down
end
