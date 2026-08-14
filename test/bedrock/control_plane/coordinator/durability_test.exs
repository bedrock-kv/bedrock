defmodule Bedrock.ControlPlane.Coordinator.DurabilityTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log.InMemoryLog

  defp state(overrides \\ []) do
    struct!(%State{director: :unavailable}, overrides)
  end

  defp leader_raft do
    Node.self()
    |> Raft.new([], InMemoryLog.new(:tuple), RaftAdapter)
    # A single-node cluster elects itself leader when the election timer fires
    |> Raft.handle_event(:election, :timer)
  end

  defp follower_raft do
    # A follower cannot accept transactions; with a peer present it stays follower
    Raft.new(Node.self(), [:peer@somewhere], InMemoryLog.new(:tuple), RaftAdapter)
  end

  defp ack_to_self do
    test_pid = self()
    fn result -> send(test_pid, {:ack, result}) end
  end

  describe "durably_write_service_registration/3" do
    test "tracks the transaction in the waiting list when raft accepts the write" do
      initial_state = state(raft: leader_raft())
      command = {:register_services, %{services: []}}
      ack_fn = ack_to_self()

      assert {:ok, %State{} = updated_state} =
               Durability.durably_write_service_registration(initial_state, command, ack_fn)

      # The single-node leader assigns {term, sequence} = {1, 1} to the first transaction
      assert %{{1, 1} => ^ack_fn} = updated_state.waiting_list
      assert map_size(updated_state.waiting_list) == 1

      # The raft state advances; the ack is deferred until the write is durable
      refute updated_state.raft == initial_state.raft
      refute_received {:ack, _}
    end

    test "invokes the ack function with the error and returns it when raft rejects the write" do
      initial_state = state(raft: follower_raft())
      command = {:register_services, %{services: []}}

      assert {:error, :not_leader} =
               Durability.durably_write_service_registration(initial_state, command, ack_to_self())

      assert_received {:ack, {:error, :not_leader}}
      assert initial_state.waiting_list == %{}
    end
  end

  describe "wait_for_durable_write_to_complete/3" do
    test "adds the ack function to the waiting list keyed by transaction id" do
      ack_fn = ack_to_self()
      existing_ack = fn _ -> :ok end
      initial_state = state(waiting_list: %{{1, 1} => existing_ack})

      updated_state = Durability.wait_for_durable_write_to_complete(initial_state, ack_fn, {1, 2})

      assert updated_state.waiting_list == %{{1, 1} => existing_ack, {1, 2} => ack_fn}
    end
  end

  describe "process_command/2 with :set_node_resources" do
    test "registers a node's services and capabilities into empty state" do
      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [
             {"log_1", :log, {:log_1_worker, :node1@host}},
             {"storage_1", :storage, {:storage_1_worker, :node1@host}}
           ],
           capabilities: [:log, :storage]
         }}

      updated_state = Durability.process_command(state(), command)

      assert updated_state.service_directory == %{
               "log_1" => {:log, {:log_1_worker, :node1@host}},
               "storage_1" => {:storage, {:storage_1_worker, :node1@host}}
             }

      assert updated_state.node_capabilities == %{node1@host: [:log, :storage]}
    end

    test "replaces the node's existing services while preserving other nodes' services" do
      initial_state =
        state(
          service_directory: %{
            "old_log" => {:log, {:old_log_worker, :node1@host}},
            "other_storage" => {:storage, {:other_worker, :node2@host}}
          },
          node_capabilities: %{node1@host: [:log], node2@host: [:storage]}
        )

      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [{"new_log", :log, {:new_log_worker, :node1@host}}],
           capabilities: [:log]
         }}

      updated_state = Durability.process_command(initial_state, command)

      assert updated_state.service_directory == %{
               "new_log" => {:log, {:new_log_worker, :node1@host}},
               "other_storage" => {:storage, {:other_worker, :node2@host}}
             }

      assert updated_state.node_capabilities == %{node1@host: [:log], node2@host: [:storage]}
    end

    test "notifies the director of newly registered services and updated capabilities" do
      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [{"log_1", :log, {:log_1_worker, :node1@host}}],
           capabilities: [:coordination]
         }}

      Durability.process_command(state(director: self()), command)

      assert_received {:"$gen_cast", {:service_registered, [{"log_1", :log, {:log_1_worker, :node1@host}}]}}
      # Nodes unreachable from the test node are filtered out of the capability map
      assert_received {:"$gen_cast", {:capabilities_updated, %{coordination: [], resolution: []}}}
    end

    test "notifies the director only about services whose kind or worker ref changed" do
      initial_state =
        state(
          director: self(),
          service_directory: %{
            "unchanged" => {:log, {:log_worker, :node1@host}},
            "moved" => {:storage, {:storage_worker, :node1@host}}
          },
          node_capabilities: %{node1@host: [:log]}
        )

      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [
             {"unchanged", :log, {:log_worker, :node1@host}},
             {"moved", :storage, {:storage_worker_2, :node1@host}}
           ],
           capabilities: [:log]
         }}

      Durability.process_command(initial_state, command)

      assert_received {:"$gen_cast", {:service_registered, [{"moved", :storage, {:storage_worker_2, :node1@host}}]}}
      refute_received {:"$gen_cast", {:capabilities_updated, _}}
    end

    test "does not notify the director when services and capabilities are unchanged" do
      initial_state =
        state(
          director: self(),
          service_directory: %{"log_1" => {:log, {:log_worker, :node1@host}}},
          node_capabilities: %{node1@host: [:log]}
        )

      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [{"log_1", :log, {:log_worker, :node1@host}}],
           capabilities: [:log]
         }}

      updated_state = Durability.process_command(initial_state, command)

      assert updated_state.service_directory == initial_state.service_directory
      assert updated_state.node_capabilities == initial_state.node_capabilities
      refute_received {:"$gen_cast", _}
    end

    test "does not notify anyone when the director is unavailable" do
      command =
        {:set_node_resources,
         %{
           node: :node1@host,
           services: [{"log_1", :log, {:log_worker, :node1@host}}],
           capabilities: [:log]
         }}

      updated_state = Durability.process_command(state(director: :unavailable), command)

      assert updated_state.service_directory == %{"log_1" => {:log, {:log_worker, :node1@host}}}
      refute_received {:"$gen_cast", _}
    end
  end

  describe "process_command/2 with :merge_node_resources" do
    test "adds services into the directory without removing the node's existing services" do
      initial_state =
        state(
          service_directory: %{
            "existing_log" => {:log, {:existing_worker, :node1@host}},
            "other_storage" => {:storage, {:other_worker, :node2@host}}
          },
          node_capabilities: %{node1@host: [:log]}
        )

      command =
        {:merge_node_resources,
         %{
           node: :node1@host,
           services: [{"new_storage", :storage, {:new_worker, :node1@host}}],
           capabilities: [:log]
         }}

      updated_state = Durability.process_command(initial_state, command)

      assert updated_state.service_directory == %{
               "existing_log" => {:log, {:existing_worker, :node1@host}},
               "other_storage" => {:storage, {:other_worker, :node2@host}},
               "new_storage" => {:storage, {:new_worker, :node1@host}}
             }
    end

    test "merges capabilities uniquely with the node's existing capabilities" do
      initial_state = state(node_capabilities: %{node1@host: [:log, :coordination]})

      command =
        {:merge_node_resources, %{node: :node1@host, services: [], capabilities: [:storage, :coordination]}}

      updated_state = Durability.process_command(initial_state, command)

      assert updated_state.node_capabilities == %{node1@host: [:log, :coordination, :storage]}
    end

    test "notifies the director of new services and merged capabilities" do
      initial_state =
        state(
          director: self(),
          node_capabilities: %{node1@host: [:log]}
        )

      command =
        {:merge_node_resources,
         %{
           node: :node1@host,
           services: [{"storage_1", :storage, {:storage_worker, :node1@host}}],
           capabilities: [:coordination]
         }}

      Durability.process_command(initial_state, command)

      assert_received {:"$gen_cast", {:service_registered, [{"storage_1", :storage, {:storage_worker, :node1@host}}]}}
      assert_received {:"$gen_cast", {:capabilities_updated, %{log: [], coordination: [], resolution: []}}}
    end

    test "does not notify the director when merged capabilities and services are unchanged" do
      initial_state =
        state(
          director: self(),
          service_directory: %{"log_1" => {:log, {:log_worker, :node1@host}}},
          node_capabilities: %{node1@host: [:log, :coordination]}
        )

      command =
        {:merge_node_resources,
         %{
           node: :node1@host,
           services: [{"log_1", :log, {:log_worker, :node1@host}}],
           capabilities: [:coordination]
         }}

      updated_state = Durability.process_command(initial_state, command)

      assert updated_state.node_capabilities == %{node1@host: [:log, :coordination]}
      refute_received {:"$gen_cast", _}
    end

    test "does not notify anyone when the director is unavailable" do
      command =
        {:merge_node_resources,
         %{
           node: :node1@host,
           services: [{"log_1", :log, {:log_worker, :node1@host}}],
           capabilities: [:log]
         }}

      updated_state = Durability.process_command(state(director: :unavailable), command)

      assert updated_state.service_directory == %{"log_1" => {:log, {:log_worker, :node1@host}}}
      refute_received {:"$gen_cast", _}
    end
  end

  describe "reply_to_waiter/2" do
    test "invokes the waiter's ack function with the transaction id and removes it" do
      ack_fn = ack_to_self()
      waiting_list = %{{1, 1} => ack_fn}

      assert Durability.reply_to_waiter(waiting_list, {1, 1}) == %{}
      assert_received {:ack, {:ok, {1, 1}}}
    end

    test "leaves the waiting list unchanged when no waiter matches" do
      ack_fn = ack_to_self()
      waiting_list = %{{1, 1} => ack_fn}

      assert Durability.reply_to_waiter(waiting_list, {2, 5}) == waiting_list
      refute_received {:ack, _}
    end
  end
end
