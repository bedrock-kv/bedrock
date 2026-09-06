defmodule Bedrock.ControlPlane.Coordinator.RecoveryGenerationProtocolTest do
  use ExUnit.Case, async: false

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.RecoveryGeneration
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule Cluster do
    @moduledoc false
    def name, do: "generation_protocol"
    def otp_name(_), do: :generation_protocol_coordinator
    def node_config, do: []
  end

  setup %{tmp_dir: root} do
    log = DiskRaftLog.new(log_dir: root, table_name: :generation_protocol_log)
    {:ok, log} = DiskRaftLog.open(log)
    on_exit(fn -> DiskRaftLog.close(log) end)
    %{log: log}
  end

  test "a bypassed interior committed entry fails before checkpointing", %{log: log} do
    entries =
      for index <- 1..3,
          do: {{1, index}, {:begin_recovery, %{generation: index + 7, owner_term: 1, request_id: "r#{index}"}}}

    {:ok, log} = Log.append_transactions(log, {0, 0}, entries)
    {:ok, log} = Log.commit_up_to(log, {1, 3})
    :ok = :dets.insert(log.table_name, {{:chain, {1, 1}}, {1, 3}})
    assert catch_exit(Durability.restore(%State{last_durable_txn_id: {0, 0}}, log)) == :missing_committed_prefix
    assert [] == :dets.lookup(log.table_name, :coordinator_checkpoint)
  end

  test "checkpoint state must match its exact retained committed prefix", %{log: log} do
    command = {:begin_recovery, %{generation: 10, owner_term: 1, request_id: "allocated"}}
    {:ok, log} = Log.append_transactions(log, {0, 0}, [{{1, 1}, command}])
    {:ok, log} = Log.commit_up_to(log, {1, 1})
    state = %State{cluster_id: "checkpoint", last_durable_txn_id: {0, 0}}
    restored = Durability.restore(state, log)
    assert restored.generation_floor == 10
    [{:coordinator_checkpoint, checkpoint}] = :dets.lookup(log.table_name, :coordinator_checkpoint)

    for corruption <- [%{generation_floor: 0, last_allocation: nil}, %{node_capabilities: %{foreign: [:log]}}] do
      corrupted = Map.merge(checkpoint, corruption)
      :ok = :dets.insert(log.table_name, {:coordinator_checkpoint, corrupted})
      assert {:invalid_coordinator_checkpoint, ^corrupted} = catch_exit(Durability.restore(state, log))
    end
  end

  test "checkpoint format, cluster identity and cursor must match retained committed history", %{log: log} do
    {:ok, log} =
      Log.append_transactions(log, {0, 0}, [
        {{1, 1}, {:begin_recovery, %{generation: 8, owner_term: 1, request_id: "r8"}}}
      ])

    {:ok, log} = Log.commit_up_to(log, {1, 1})
    state = %State{cluster_id: "valid", last_durable_txn_id: {0, 0}}
    Durability.restore(state, log)
    [{:coordinator_checkpoint, valid}] = :dets.lookup(log.table_name, :coordinator_checkpoint)

    for change <- [%{cluster_id: "foreign"}, %{last_durable_txn_id: {1, 2}}, %{generation_floor: 0x10000000000000000}] do
      corrupted = Map.merge(valid, change)
      :ok = :dets.insert(log.table_name, {:coordinator_checkpoint, corrupted})
      assert {:invalid_coordinator_checkpoint, ^corrupted} = catch_exit(Durability.restore(state, log))
    end

    :ok = :dets.insert(log.table_name, {:coordinator_checkpoint, %{valid | format_version: 2}})
    assert catch_exit(Durability.restore(state, log)) == :unsupported_coordinator_checkpoint
  end

  for {allocation, reason} <- [
        {%{generation: 8, owner_term: 2, request_id: "wrong-owner"}, :invalid_recovery_allocation},
        {%{generation: 8, owner_term: 1, request_id: ""}, :invalid_recovery_allocation},
        {%{generation: 0, owner_term: 1, request_id: "zero"}, :invalid_recovery_allocation},
        {%{generation: 0x10000000000000000, owner_term: 1, request_id: "overflow"}, :invalid_recovery_allocation}
      ] do
    test "invalid committed allocation #{inspect(allocation)} fails before checkpoint", %{log: log} do
      {:ok, log} =
        Log.append_transactions(log, {0, 0}, [{{1, 1}, {:begin_recovery, unquote(Macro.escape(allocation))}}])

      {:ok, log} = Log.commit_up_to(log, {1, 1})
      assert catch_exit(Durability.restore(%State{last_durable_txn_id: {0, 0}}, log)) == unquote(reason)
      assert [] == :dets.lookup(log.table_name, :coordinator_checkpoint)
    end
  end

  for {command, reason} <- [
        {{:begin_recovery, %{generation: 8, owner_term: 0, request_id: "term-zero"}}, :invalid_recovery_allocation},
        {{:recovery_barrier, %{owner_term: 0, request_id: "term-zero"}}, :invalid_recovery_barrier}
      ] do
    test "term-zero #{elem(command, 0)} cannot authorize recovery", %{log: log} do
      {:ok, log} = Log.append_transactions(log, {0, 0}, [{{0, 1}, unquote(Macro.escape(command))}])
      {:ok, log} = Log.commit_up_to(log, {0, 1})
      assert catch_exit(Durability.restore(%State{last_durable_txn_id: {0, 0}}, log)) == unquote(reason)
      assert [] == :dets.lookup(log.table_name, :coordinator_checkpoint)
    end
  end

  test "nonmonotonic committed allocation cannot overwrite the checkpoint", %{log: log} do
    entries =
      for index <- 1..2, do: {{1, index}, {:begin_recovery, %{generation: 8, owner_term: 1, request_id: "r#{index}"}}}

    {:ok, log} = Log.append_transactions(log, {0, 0}, entries)
    {:ok, log} = Log.commit_up_to(log, {1, 2})

    assert catch_exit(Durability.restore(%State{last_durable_txn_id: {0, 0}}, log)) ==
             :non_monotonic_recovery_allocation

    assert [] == :dets.lookup(log.table_name, :coordinator_checkpoint)
  end

  test "overlapping committed suffix checkpoints state before each waiter effect", %{log: log} do
    entries =
      for index <- 1..2, do: {{1, index}, Commands.register_services([{"worker#{index}", :log, {:worker, node()}}])}

    {:ok, log} = Log.append_transactions(log, {0, 0}, entries)
    {:ok, log} = Log.commit_up_to(log, {1, 2})
    owner = self()

    waiter = fn expected ->
      fn {:ok, id} ->
        [{:coordinator_checkpoint, checkpoint}] = :dets.lookup(log.table_name, :coordinator_checkpoint)
        send(owner, {:effect, id, checkpoint.last_durable_txn_id, map_size(checkpoint.service_directory), expected})
        :ok
      end
    end

    state = %State{last_durable_txn_id: {0, 0}, waiting_list: %{{1, 1} => waiter.(1), {1, 2} => waiter.(2)}}
    first = Durability.durable_write_completed(state, log, {1, 1})
    assert_received {:effect, {1, 1}, {1, 1}, 1, 1}
    second = Durability.durable_write_completed(first, log, {1, 2})
    assert_received {:effect, {1, 2}, {1, 2}, 2, 2}
    assert second == Durability.durable_write_completed(second, log, {1, 1})
    assert second == Durability.durable_write_completed(second, log, {1, 2})
    refute_received {:effect, _, _, _, _}
  end

  test "historical resource and end-epoch replay produces no actor or waiter effects", %{log: log} do
    entries = [
      {{1, 1}, Commands.register_services([{"restored", :log, {:worker, node()}}])},
      {{1, 2}, {:end_epoch, 7}},
      {{1, 3}, {:begin_recovery, %{generation: 8, owner_term: 1, request_id: "historical"}}}
    ]

    {:ok, log} = Log.append_transactions(log, {0, 0}, entries)
    {:ok, log} = Log.commit_up_to(log, {1, 3})
    :ok = DiskRaftLog.close(log)
    {:ok, reopened} = DiskRaftLog.open(log)
    owner = self()

    observer =
      spawn(fn ->
        receive do
          message -> send(owner, {:unexpected_replay_effect, message})
        end
      end)

    on_exit(fn -> Process.exit(observer, :kill) end)

    state = %State{
      director: observer,
      last_durable_txn_id: {0, 0},
      waiting_list: %{{1, 1} => fn _ -> send(owner, :unexpected_ack) end}
    }

    restored = Durability.restore(state, reopened)
    assert restored.generation_floor == 8
    assert restored.service_directory["restored"] == {:log, {:worker, node()}}
    assert restored.director == observer
    assert Process.alive?(observer)
    refute_received {:unexpected_replay_effect, _}
    refute_received :unexpected_ack
  end

  test "in-memory Raft cannot activate durable recovery even after barrier commit" do
    raft = node() |> Raft.new([], InMemoryLog.new(:tuple), RaftAdapter) |> Raft.handle_event(:election, :timer)

    state = %State{
      cluster: Cluster,
      raft: raft,
      my_node: node(),
      last_durable_txn_id: {0, 0}
    }

    assert {:noreply, pending} = Server.handle_info({:raft, :leadership_changed, {node(), 1}}, state)
    id = pending.recovery_generation.log_id

    assert {:noreply, failed} =
             Server.handle_info({:raft, :consensus_reached, Raft.log(pending.raft), id, :latest}, pending)

    assert failed.recovery_generation.reason == :durable_raft_path_required
    assert failed.director == :unavailable
    assert failed.bootstrap_reservation == nil
  end

  test "a behind committed prefix applies once and survives disk reopen", %{log: log} do
    raft = node() |> Raft.new([], log, RaftAdapter) |> Raft.handle_event(:election, :timer)
    command = Commands.register_services([{"retained", :log, {:log_worker, node()}}])
    {:ok, raft, id} = Raft.add_transaction(raft, command)
    assert Log.newest_safe_transaction_id(Raft.log(raft)) == id
    state = %State{raft: raft, last_durable_txn_id: Log.initial_transaction_id(log)}
    assert {:noreply, applied} = Server.handle_info({:raft, :consensus_reached, log, id, :behind}, state)
    assert applied.service_directory["retained"] == {:log, {:log_worker, node()}}
    assert applied.last_durable_txn_id == id
    assert {:noreply, ^applied} = Server.handle_info({:raft, :consensus_reached, log, id, :latest}, applied)
    assert {:noreply, ^applied} = Server.handle_info({:raft, :consensus_reached, log, {0, 0}, :behind}, applied)
    assert :ok = DiskRaftLog.close(log)
    assert {:ok, reopened} = DiskRaftLog.open(log)
    assert Log.transactions_to(reopened, :newest_safe) == [{id, command}]
    # The protocol's checkpoint must restore the cursor and matching pure state,
    # not only retain the Raft entry while replaying side effects on restart.
    assert [{:coordinator_checkpoint, %{last_durable_txn_id: ^id, service_directory: directory}}] =
             :dets.lookup(reopened.table_name, :coordinator_checkpoint)

    assert directory == applied.service_directory
  end

  test "latest application checkpoints matching pure state and cursor before restart", %{log: log} do
    raft = node() |> Raft.new([], log, RaftAdapter) |> Raft.handle_event(:election, :timer)
    command = Commands.register_services([{"checkpointed", :log, {:log_worker, node()}}])
    {:ok, raft, id} = Raft.add_transaction(raft, command)

    state = %State{
      raft: raft,
      my_node: node(),
      leader_node: :other,
      last_durable_txn_id: Log.initial_transaction_id(log)
    }

    assert {:noreply, applied} = Server.handle_info({:raft, :consensus_reached, log, id, :latest}, state)
    assert applied.service_directory["checkpointed"] == {:log, {:log_worker, node()}}
    assert :ok = DiskRaftLog.close(log)
    assert {:ok, reopened} = DiskRaftLog.open(log)

    assert [{:coordinator_checkpoint, %{last_durable_txn_id: ^id, service_directory: directory}}] =
             :dets.lookup(reopened.table_name, :coordinator_checkpoint)

    assert directory == applied.service_directory
  end

  defmodule ReplayCluster do
    @moduledoc false
    def name, do: "generation_replay"
    def otp_name(_), do: :generation_replay_coordinator
    def fetch_coordinator_nodes, do: {:ok, [node()]}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
  end

  test "same committed allocation prefix replays identically across bootstrap observations and checkpoints", %{
    tmp_dir: root
  } do
    backend = {LocalFilesystem, root: Path.join(root, "objects")}
    Application.put_env(:bedrock, ReplayCluster, object_storage: backend, coordinator: [path: root])
    on_exit(fn -> Application.delete_env(:bedrock, ReplayCluster) end)
    {:ok, disk} = DiskRaftLog.open(DiskRaftLog.new(log_dir: Path.join(root, "raft")))
    on_exit(fn -> DiskRaftLog.close(disk) end)
    raft = node() |> Raft.new([], disk, RaftAdapter) |> Raft.handle_event(:election, :timer)
    command_a = {:begin_recovery, %{request_id: "allocation-a", owner_term: 1, generation: 8}}
    command_b = {:begin_recovery, %{request_id: "allocation-b", owner_term: 1, generation: 10}}
    {:ok, raft, id_a} = Raft.add_transaction(raft, command_a)
    {:ok, _raft, id_b} = Raft.add_transaction(raft, command_b)
    assert :ok = DiskRaftLog.close(disk)

    for observed <- [7, 9, 14] do
      bootstrap = %{
        cluster_id: "replay-cluster",
        epoch: observed,
        logs: [%{id: "committed"}],
        coordinators: [%{node: Atom.to_string(node())}]
      }

      :ok = Bedrock.ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

      for checkpoint? <- [false, true] do
        {:ok, disk} = DiskRaftLog.open(disk)

        if checkpoint? do
          checkpoint = %{
            format_version: 1,
            cluster_id: "replay-cluster",
            last_durable_txn_id: id_a,
            generation_floor: 8,
            last_allocation: %{request_id: "allocation-a", owner_term: 1, generation: 8},
            service_directory: %{},
            node_capabilities: %{}
          }

          :ok = :dets.insert(disk.table_name, {:coordinator_checkpoint, checkpoint})
        else
          :ok = :dets.delete(disk.table_name, :coordinator_checkpoint)
        end

        :ok = DiskRaftLog.sync(disk)
        :ok = DiskRaftLog.close(disk)
        assert {:ok, recovered, _continue} = Server.init({ReplayCluster, ReplayCluster.otp_name(:coordinator)})

        assert Map.get(recovered, :generation_floor) == 10,
               "same A8/B10 prefix changed with bootstrap #{observed}, checkpoint? #{checkpoint?}"

        assert recovered.last_durable_txn_id == id_b
        assert Map.get(recovered, :last_allocation).generation == 10
        assert recovered.director == :unavailable
        assert Log.transactions_to(Raft.log(recovered.raft), :newest_safe) == [{id_a, command_a}, {id_b, command_b}]
        :ok = DiskRaftLog.close(Raft.log(recovered.raft))
      end
    end
  end

  test "real follower acknowledgement releases current-term barrier and allocation separately", %{
    log: log,
    tmp_dir: root
  } do
    backend = {LocalFilesystem, root: Path.join(root, "objects")}
    Application.put_env(:bedrock, ReplayCluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, ReplayCluster) end)

    bootstrap = %{
      cluster_id: "quorum",
      epoch: 7,
      logs: [%{id: "prior"}],
      coordinators: [%{node: Atom.to_string(node())}]
    }

    :ok = Bedrock.ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))
    {:ok, peer_log} = DiskRaftLog.open(DiskRaftLog.new(log_dir: Path.join(root, "peer"), table_name: :generation_peer))
    on_exit(fn -> DiskRaftLog.close(peer_log) end)
    peer = Raft.new(:peer, [node()], peer_log, RaftAdapter)
    leader = node() |> Raft.new([:peer], log, RaftAdapter) |> Raft.handle_event(:election, :timer)
    assert_receive {:raft, :send_rpc, {:request_vote, 1, _} = vote_request, :peer}
    peer = Raft.handle_event(peer, vote_request, node())
    me = node()
    assert_receive {:raft, :send_rpc, {:vote, 1} = vote, ^me}
    leader = Raft.handle_event(leader, vote, :peer)

    state = %State{
      cluster: ReplayCluster,
      cluster_id: "quorum",
      raft: leader,
      raft_term: 1,
      leader_node: me,
      my_node: me,
      last_durable_txn_id: {0, 0}
    }

    pending = RecoveryGeneration.request(state)
    barrier = pending.recovery_generation.log_id
    assert Log.newest_safe_transaction_id(log) == {0, 0}
    assert pending == RecoveryGeneration.advance(pending)
    {leader, peer} = replicate_entry(pending.raft, peer, barrier)
    assert Log.newest_safe_transaction_id(log) == barrier

    assert {:noreply, reading} =
             Server.handle_info({:raft, :consensus_reached, log, barrier, :latest}, %{pending | raft: leader})

    io_id = reading.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^io_id, loaded}, 1000
    allocating = RecoveryGeneration.result(reading, io_id, loaded)
    allocation_id = allocating.recovery_generation.log_id
    assert allocating.generation_floor == 0
    assert allocating.bootstrap_reservation == nil
    assert allocating.director == :unavailable
    assert {:ok, %{bootstrap: %{epoch: 7} = prior}} = Publication.read(backend, "bootstrap")
    assert prior[:recovery_generation] in [nil, 0]
    {leader, _peer} = replicate_entry(allocating.raft, peer, allocation_id)

    assert {:noreply, reserving} =
             Server.handle_info({:raft, :consensus_reached, log, allocation_id, :behind}, %{allocating | raft: leader})

    assert reserving.generation_floor == 8
    reservation_id = reserving.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^reservation_id, {:ok, reservation}}, 1000
    assert reservation.generation == 8
    assert reservation.prior_bootstrap.epoch == 7
    # Crash at reservation-complete / launch-not-yet-delivered: replay never launches.
    assert :ok = DiskRaftLog.close(log)
    assert {:ok, reopened} = DiskRaftLog.open(log)
    restored = Durability.restore(%State{cluster_id: "quorum", last_durable_txn_id: {0, 0}}, reopened)
    assert restored.generation_floor == 8
    assert restored.last_durable_txn_id == allocation_id
    assert restored.director == :unavailable
    RecoveryGeneration.cancel(reserving)
  end

  test "successor barrier commits inherited prior-term allocation without launching its owner", %{
    log: log,
    tmp_dir: root
  } do
    backend = {LocalFilesystem, root: Path.join(root, "objects")}
    Application.put_env(:bedrock, ReplayCluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, ReplayCluster) end)

    bootstrap = %{
      cluster_id: "quorum",
      epoch: 7,
      logs: [%{id: "prior"}],
      coordinators: [%{node: Atom.to_string(node())}]
    }

    :ok = Bedrock.ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))
    {:ok, peer_log} = DiskRaftLog.open(DiskRaftLog.new(log_dir: Path.join(root, "peer"), table_name: :generation_peer))
    on_exit(fn -> DiskRaftLog.close(peer_log) end)
    inherited = {:begin_recovery, %{generation: 8, owner_term: 1, request_id: "abandoned-term1"}}
    {:ok, log} = Log.append_transactions(log, {0, 0}, [{{1, 1}, inherited}])
    {:ok, log} = DiskRaftLog.save_current_term(log, 1)
    assert Log.newest_safe_transaction_id(log) == {0, 0}
    peer = Raft.new(:peer, [node()], peer_log, RaftAdapter)
    leader = node() |> Raft.new([:peer], log, RaftAdapter) |> Raft.handle_event(:election, :timer)
    assert_receive {:raft, :send_rpc, {:request_vote, 2, _} = vote_request, :peer}
    peer = Raft.handle_event(peer, vote_request, node())
    me = node()
    assert_receive {:raft, :send_rpc, {:vote, 2} = vote, ^me}
    leader = Raft.handle_event(leader, vote, :peer)

    state = %State{
      cluster: ReplayCluster,
      cluster_id: "quorum",
      raft: leader,
      raft_term: 2,
      leader_node: me,
      my_node: me,
      last_durable_txn_id: {0, 0}
    }

    pending = RecoveryGeneration.request(state)
    barrier = pending.recovery_generation.log_id
    assert Log.newest_safe_transaction_id(log) == {0, 0}
    assert pending == RecoveryGeneration.advance(pending)
    {leader, peer} = replicate_entry(pending.raft, peer, barrier)
    assert Log.newest_safe_transaction_id(log) == barrier

    assert {:noreply, reading} =
             Server.handle_info({:raft, :consensus_reached, log, barrier, :latest}, %{pending | raft: leader})

    io_id = reading.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^io_id, loaded}, 1000
    allocating = RecoveryGeneration.result(reading, io_id, loaded)
    allocation_id = allocating.recovery_generation.log_id
    assert allocating.generation_floor == 8
    assert allocating.last_allocation.request_id == "abandoned-term1"
    assert allocating.bootstrap_reservation == nil
    assert allocating.director == :unavailable
    assert {:ok, %{bootstrap: %{epoch: 7} = prior}} = Publication.read(backend, "bootstrap")
    assert prior[:recovery_generation] in [nil, 0]
    {leader, _peer} = replicate_entry(allocating.raft, peer, allocation_id)

    assert {:noreply, reserving} =
             Server.handle_info({:raft, :consensus_reached, log, allocation_id, :behind}, %{allocating | raft: leader})

    assert reserving.generation_floor == 9
    reservation_id = reserving.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^reservation_id, {:ok, reservation}}, 1000
    assert reservation.generation == 9
    assert reservation.prior_bootstrap.epoch == 7
    # Crash at reservation-complete / launch-not-yet-delivered: replay never launches.
    assert :ok = DiskRaftLog.close(log)
    assert {:ok, reopened} = DiskRaftLog.open(log)
    restored = Durability.restore(%State{cluster_id: "quorum", last_durable_txn_id: {0, 0}}, reopened)
    assert restored.generation_floor == 9
    assert restored.last_durable_txn_id == allocation_id
    assert restored.director == :unavailable
    RecoveryGeneration.cancel(reserving)
  end

  defp replicate_entry(leader, peer, target) do
    assert_receive {:raft, :send_rpc, {:append_entries, _, _, _entries, _} = event, :peer}, 1000
    peer = Raft.handle_event(peer, event, node())
    me = node()
    assert_receive {:raft, :send_rpc, {:append_entries_ack, _, _, _, _} = ack, ^me}, 1000
    leader = Raft.handle_event(leader, ack, :peer)

    if Log.newest_safe_transaction_id(Raft.log(leader)) >= target,
      do: {leader, peer},
      else: replicate_entry(leader, peer, target)
  end

  test "an elected leader cannot launch recovery before its barrier reaches quorum", %{log: log} do
    raft = node() |> Raft.new([:peer], log, RaftAdapter) |> Raft.handle_event(:election, :timer)
    raft = Raft.handle_event(raft, {:vote, 1}, :peer)
    assert Raft.am_i_the_leader?(raft)
    assert Raft.leadership(raft) == {node(), 1}
    assert Log.newest_safe_transaction_id(log) == {0, 0}
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one})

    state = %State{
      cluster: Cluster,
      raft: raft,
      my_node: node(),
      supervisor_otp_name: sup,
      config: Config.new([:peer]),
      last_durable_txn_id: {0, 0}
    }

    assert {:noreply, pending} = Server.handle_info({:raft, :leadership_changed, {node(), 1}}, state)
    assert pending.director == :unavailable
    assert Log.newest_transaction_id(log) > Log.newest_safe_transaction_id(log)
    assert {:noreply, ^pending} = Server.handle_info({:raft, :leadership_changed, {node(), 1}}, pending)
  end
end
