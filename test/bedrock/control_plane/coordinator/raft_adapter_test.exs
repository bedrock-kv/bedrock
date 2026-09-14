defmodule Bedrock.ControlPlane.Coordinator.RaftAdapterTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.RaftAdapter

  describe "heartbeat_ms/0" do
    test "returns heartbeat interval in milliseconds" do
      assert RaftAdapter.heartbeat_ms() == 100
    end
  end

  describe "timestamp_in_ms/0" do
    test "returns current monotonic time in milliseconds" do
      timestamp = RaftAdapter.timestamp_in_ms()

      assert is_integer(timestamp)
      # Monotonic time can be negative depending on system start
    end
  end

  describe "ignored_event/2" do
    test "returns :ok for any event" do
      assert :ok = RaftAdapter.ignored_event(:some_event, self())
      assert :ok = RaftAdapter.ignored_event({:other, :event}, :from)
    end
  end

  describe "leadership_changed/1" do
    test "sends leadership changed message to self" do
      RaftAdapter.leadership_changed(:leader)

      assert_received {:raft, :leadership_changed, :leader}
    end

    test "sends follower leadership message" do
      RaftAdapter.leadership_changed(:follower)

      assert_received {:raft, :leadership_changed, :follower}
    end
  end

  describe "send_event/2" do
    test "sends RPC event message to self" do
      event = {:request_vote, 1}
      to = :node1@localhost

      assert :ok = RaftAdapter.send_event(to, event)
      assert_received {:raft, :send_rpc, ^event, ^to}
    end
  end

  describe "timer/1" do
    test "creates heartbeat timer" do
      cancel_fn = RaftAdapter.timer(:heartbeat)

      assert is_function(cancel_fn, 0)
      assert :ok = cancel_fn.()
    end

    test "creates election timer with jitter" do
      cancel_fn = RaftAdapter.timer(:election)

      assert is_function(cancel_fn, 0)
      assert :ok = cancel_fn.()
    end

    # A coordinator busy for longer than a timer's interval finds the fired
    # timer's message already queued when the protocol resets that timer
    # (e.g. on AppendEntries). Delivered afterwards, the stale election
    # timeout would start an election despite the leader's heartbeat.
    test "cancelling a timer that already fired discards its queued message" do
      cancel_fn = RaftAdapter.timer(:heartbeat)
      Process.send_after(self(), :heartbeat_interval_elapsed, 2 * RaftAdapter.heartbeat_ms())
      assert_receive :heartbeat_interval_elapsed, 1_000
      {:messages, messages} = Process.info(self(), :messages)
      assert {:raft, :timer, :heartbeat} in messages

      cancel_fn.()

      refute_received {:raft, :timer, :heartbeat}
    end
  end

  describe "consensus_reached/3" do
    test "sends consensus reached message to self" do
      log = [:entry1, :entry2]
      transaction_id = "tx_123"
      consistency = :strong

      assert :ok = RaftAdapter.consensus_reached(log, transaction_id, consistency)
      assert_received {:raft, :consensus_reached, ^log, ^transaction_id, ^consistency}
    end
  end

  describe "quorum_lost/3" do
    test "returns :step_down" do
      assert :step_down = RaftAdapter.quorum_lost(1, 3, 5)
      assert :step_down = RaftAdapter.quorum_lost(0, 5, 10)
    end
  end
end
