defmodule Bedrock.ControlPlane.Coordinator.ColdBootTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTracker
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog

  defmodule TestCluster do
    @moduledoc false
    def name, do: "test_cluster"
    def otp_name(:coordinator), do: :test_coordinator
    def otp_name(:sup), do: :test_sup
    def otp_name(component), do: :"test_#{component}"
  end

  describe "cold boot leadership election" do
    test "leader election completes when there are no transactions to reach consensus on" do
      # Simulate a fresh coordinator state with an empty Raft log
      my_node = Node.self()
      raft_log = InMemoryLog.new(:tuple)

      raft =
        Raft.new(
          my_node,
          # No other nodes - single node cluster
          [],
          raft_log,
          RaftAdapter
        )

      state = %State{
        cluster: TestCluster,
        my_node: my_node,
        otp_name: :test_coordinator,
        supervisor_otp_name: :test_sup,
        raft: raft,
        leader_node: :undecided,
        leader_startup_state: :not_leader,
        epoch: 0,
        # No transactions waiting for consensus
        waiting_list: %{},
        config: nil,
        transaction_system_layout: nil,
        service_directory: %{},
        node_capabilities: %{},
        director: :unavailable,
        last_durable_txn_id: {0, 0},
        tsl_subscribers: MapSet.new()
      }

      # Simulate leadership election - this node becomes leader
      leadership_event = {:raft, :leadership_changed, {my_node, 1}}

      # Upon leadership election, the coordinator should immediately proceed to director startup
      # (no waiting for consensus - TSL is output of recovery, config loaded from object storage at init)

      # The director startup will fail because there's no supervisor in the test environment,
      # but we can verify that it ATTEMPTED to start the director
      result = catch_exit(Server.handle_info(leadership_event, state))

      # Verify it attempted director startup - the exit indicates it tried to call the supervisor
      assert match?({:noproc, _}, result) or match?({:EXIT, :noproc}, result) or match?({:normal, _}, result),
             "Expected director startup attempt, got: #{inspect(result)}"
    end

    test "leader election with existing committed transactions starts director immediately" do
      # With the new flow, the coordinator starts the director immediately on leadership
      # election - it no longer waits for consensus first

      my_node = Node.self()
      raft_log = InMemoryLog.new(:tuple)

      # Add a committed transaction to the log
      initial_id = Log.initial_transaction_id(raft_log)
      # Use a real coordinator command - register_services is simpler
      {:ok, raft_log} = Log.append_transactions(raft_log, initial_id, [{{0, 1}, {:register_services, %{services: []}}}])
      {:ok, raft_log} = Log.commit_up_to(raft_log, {0, 1})

      raft =
        Raft.new(
          my_node,
          [],
          raft_log,
          RaftAdapter
        )

      state = %State{
        cluster: TestCluster,
        my_node: my_node,
        otp_name: :test_coordinator,
        supervisor_otp_name: :test_sup,
        raft: raft,
        leader_node: :undecided,
        leader_startup_state: :not_leader,
        epoch: 0,
        # Transaction waiting for consensus
        waiting_list: %{{0, 1} => fn -> :ok end},
        config: nil,
        transaction_system_layout: nil,
        service_directory: %{},
        node_capabilities: %{},
        director: :unavailable,
        last_durable_txn_id: {0, 0},
        tsl_subscribers: MapSet.new()
      }

      # Simulate leadership election
      leadership_event = {:raft, :leadership_changed, {my_node, 1}}

      # Director startup will fail because no supervisor, but verify it attempted to start
      result = catch_exit(Server.handle_info(leadership_event, state))

      assert match?({:noproc, _}, result) or match?({:EXIT, :noproc}, result) or match?({:normal, _}, result),
             "Expected director startup attempt, got: #{inspect(result)}"
    end
  end

  describe "node capability registration during cold boot" do
    test "coordinator accepts node capability registration requests" do
      # This test verifies that the coordinator API accepts node capability registration
      # The full integration test of this feature would be in an integration test suite
      my_node = Node.self()
      raft_log = InMemoryLog.new(:tuple)

      raft = Raft.new(my_node, [], raft_log, RaftAdapter)

      state = %State{
        cluster: TestCluster,
        my_node: my_node,
        otp_name: :test_coordinator,
        supervisor_otp_name: :test_sup,
        raft: raft,
        # Not leader
        leader_node: :undecided,
        leader_startup_state: :not_leader,
        epoch: 1,
        waiting_list: %{},
        config: nil,
        transaction_system_layout: nil,
        service_directory: %{},
        node_capabilities: %{},
        director: :unavailable,
        last_durable_txn_id: {0, 0},
        tsl_subscribers: MapSet.new()
      }

      # Simulate a node registering its capabilities (coordination capability)
      capabilities = [:coordination]

      # Call register_node_resources when not leader - should forward to leader
      {:noreply, updated_state} =
        Server.handle_call(
          {:register_node_resources, self(), [], capabilities},
          {self(), make_ref()},
          state
        )

      # Verify the client PID was added as a TSL subscriber
      assert MapSet.member?(updated_state.tsl_subscribers, self())
    end
  end

  describe "consensus_reached behavior" do
    test "consensus_reached processes durable writes without starting director" do
      # Since director is started immediately on leadership change, consensus_reached
      # now only processes durable writes and checks for capability changes
      my_node = Node.self()
      raft_log = InMemoryLog.new(:tuple)

      initial_id = Log.initial_transaction_id(raft_log)
      # Use a real coordinator command - register_services is simpler
      {:ok, raft_log} = Log.append_transactions(raft_log, initial_id, [{{0, 1}, {:register_services, %{services: []}}}])
      {:ok, raft_log} = Log.commit_up_to(raft_log, {0, 1})

      raft = Raft.new(my_node, [], raft_log, RaftAdapter)

      # Simulate receiving an ack for the waiting transaction
      ack_received = :erlang.make_ref()

      # Initialize recovery_tracker with the current hash so no "change" is detected
      # This simulates the normal state where hash was set when leadership was acquired
      node_capabilities = %{}
      service_directory = %{}

      recovery_tracker =
        RecoveryCapabilityTracker.update_recovery_state_hash(
          RecoveryCapabilityTracker.new(),
          node_capabilities,
          service_directory
        )

      state = %State{
        cluster: TestCluster,
        my_node: my_node,
        otp_name: :test_coordinator,
        supervisor_otp_name: :test_sup,
        raft: raft,
        leader_node: my_node,
        # Already leader_ready (director started on leadership change)
        leader_startup_state: :leader_ready,
        epoch: 1,
        waiting_list: %{{0, 1} => fn _result -> send(self(), {:ack, ack_received}) end},
        config: nil,
        transaction_system_layout: nil,
        service_directory: service_directory,
        node_capabilities: node_capabilities,
        director: :unavailable,
        last_durable_txn_id: {0, 0},
        tsl_subscribers: MapSet.new(),
        recovery_tracker: recovery_tracker
      }

      # Simulate consensus_reached
      consensus_event = {:raft, :consensus_reached, raft_log, {0, 1}, :latest}

      # consensus_reached should process the durable write and clear the waiting_list
      {:noreply, updated_state} = Server.handle_info(consensus_event, state)

      # Verify the waiting_list was cleared (transaction was acknowledged)
      assert updated_state.waiting_list == %{}

      # Verify the ack was sent
      assert_received {:ack, ^ack_received}
    end
  end
end
