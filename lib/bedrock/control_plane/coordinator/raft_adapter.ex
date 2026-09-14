defmodule Bedrock.ControlPlane.Coordinator.RaftAdapter do
  @moduledoc false
  @behaviour Bedrock.Raft.Interface

  @spec determine_timeout(pos_integer(), pos_integer()) :: pos_integer()
  defp determine_timeout(min_ms, max_ms) when min_ms == max_ms, do: min_ms
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
  def timer(:heartbeat), do: set_timer(:heartbeat, heartbeat_ms(), heartbeat_ms())

  # A follower waits as long as a leader waits to hear from its followers
  # before giving up leadership (bedrock_raft's quorum check spans five
  # heartbeats), randomized over (T, 2T] to break split votes. That outlasts
  # the 2 * heartbeat_ms a healthy follower may go between AppendEntries.
  def timer(:election), do: set_timer(:election, 5 * heartbeat_ms(), 10 * heartbeat_ms())

  @spec set_timer(atom(), pos_integer(), pos_integer()) :: (-> :ok)
  defp set_timer(name, min_ms, max_ms) do
    ref = Process.send_after(self(), {:raft, :timer, name}, determine_timeout(min_ms, max_ms))
    fn -> cancel_timer(ref, name) end
  end

  # A timer that already fired has queued its message. Cancelling must
  # discard it too, or the stale timeout is handled after the event that
  # superseded it (an election despite a heartbeat). The protocol cancels a
  # timer before arming its replacement, so any queued message of this name
  # is stale.
  @spec cancel_timer(reference(), atom()) :: :ok
  defp cancel_timer(ref, name) do
    Process.cancel_timer(ref) || flush_timer(name)
    :ok
  end

  @spec flush_timer(atom()) :: :ok
  defp flush_timer(name) do
    receive do
      {:raft, :timer, ^name} -> :ok
    after
      0 -> :ok
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
