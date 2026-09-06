defmodule Bedrock.ControlPlane.Coordinator.RecoveryGenerationLifecycleTest do
  use ExUnit.Case, async: false

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.RecoveryGeneration
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule Cluster do
    @moduledoc false
    def name, do: "generation_lifecycle"
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
  end

  defmodule BlockingBackend do
    @moduledoc false
    def get_with_version(config, key) do
      send(Keyword.fetch!(config, :owner), {:blocked_read, self()})

      receive do
        :release -> LocalFilesystem.get_with_version(config, key)
        :crash -> exit(:backend_crash)
      end
    end

    def put_if_version_matches(config, key, token, bytes, opts) do
      caller = self()
      owner = Keyword.fetch!(config, :owner)

      spawn(fn ->
        send(owner, {:external_cas_ready, self()})

        receive do
          :finish ->
            result = LocalFilesystem.put_if_version_matches(config, key, token, bytes, opts)
            send(owner, {:external_cas_done, self(), result})
            send(caller, {:cas_result, result})
        end
      end)

      receive do
        {:cas_result, result} -> result
      end
    end
  end

  setup %{tmp_dir: root} do
    backend = {LocalFilesystem, root: root}
    bootstrap = %{cluster_id: "lifecycle", epoch: 1, logs: [], coordinators: [%{node: Atom.to_string(node())}]}
    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

    Application.put_env(:bedrock, Cluster,
      object_storage: {BlockingBackend, root: root, owner: self()},
      recovery_io_timeout_ms: 50
    )

    on_exit(fn -> Application.delete_env(:bedrock, Cluster) end)
    {:ok, log} = DiskRaftLog.open(DiskRaftLog.new(log_dir: Path.join(root, "raft"), table_name: :lifecycle_log))
    on_exit(fn -> DiskRaftLog.close(log) end)
    raft = node() |> Raft.new([], log, RaftAdapter) |> Raft.handle_event(:election, :timer)

    state = %State{
      cluster: Cluster,
      cluster_id: "lifecycle",
      raft: raft,
      raft_term: 1,
      my_node: node(),
      leader_node: node(),
      last_durable_txn_id: {0, 0}
    }

    pending = RecoveryGeneration.request(state)
    barrier = pending.recovery_generation.log_id
    assert {:noreply, reading} = Server.handle_info({:raft, :consensus_reached, log, barrier, :latest}, pending)
    assert_receive {:blocked_read, worker}
    on_exit(fn -> if Process.alive?(worker), do: Process.exit(worker, :kill) end)
    %{reading: reading, worker: worker, backend: backend, log: log}
  end

  test "actual timer expires blocked I/O, revokes identity and ignores late success", %{
    reading: reading,
    worker: worker,
    backend: backend
  } do
    id = reading.recovery_generation.io_id
    assert_receive {:recovery_io_timeout, ^id}, 1000
    monitor = Process.monitor(worker)
    assert {:noreply, failed} = Server.handle_info({:recovery_io_timeout, id}, reading)
    assert failed.recovery_generation.phase == :failed
    assert failed.recovery_generation.reason == :recovery_io_timeout
    assert failed.recovery_generation.io_id == nil
    assert failed.bootstrap_reservation == nil
    assert_receive {:DOWN, ^monitor, :process, ^worker, _}
    {:ok, loaded} = Publication.read(backend, "bootstrap")
    assert RecoveryGeneration.result(failed, id, {:ok, loaded}) == failed
    assert failed.director == :unavailable
  end

  test "task DOWN is finite and a new attempt uses a distinct barrier identity", %{reading: reading, worker: worker} do
    monitor = reading.recovery_generation.monitor
    send(worker, :crash)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :backend_crash}
    assert {:noreply, failed} = Server.handle_info({:DOWN, monitor, :process, worker, :backend_crash}, reading)
    assert failed.recovery_generation.reason == {:recovery_io_down, :backend_crash}
    retry = RecoveryGeneration.request(failed)
    assert retry.recovery_generation.phase == :barrier
    assert retry.recovery_generation.request_id != reading.recovery_generation.request_id
    assert retry.recovery_generation.log_id > reading.last_durable_txn_id
  end

  test "authoritative leadership loss revokes a blocked task before any late result", %{
    reading: reading,
    worker: worker,
    backend: backend
  } do
    id = reading.recovery_generation.io_id
    follower = Raft.handle_event(reading.raft, {:vote, 2}, :other)
    refute Raft.am_i_the_leader?(follower)

    assert {:noreply, retired} =
             Server.handle_info({:raft, :leadership_changed, {:undecided, 2}}, %{reading | raft: follower})

    assert retired.recovery_generation == nil
    assert retired.bootstrap_reservation == nil
    assert retired.director == :unavailable
    {:ok, loaded} = Publication.read(backend, "bootstrap")
    assert retired == RecoveryGeneration.result(retired, id, {:ok, loaded})
    monitor = Process.monitor(worker)
    assert_receive {:DOWN, ^monitor, :process, ^worker, _}
    assert Log.newest_safe_transaction_id(Raft.log(retired.raft)) == reading.last_durable_txn_id
  end

  test "reservation timeout consumes its committed generation and ignores late result", %{
    reading: reading,
    worker: worker
  } do
    reserving = enter_reservation(reading, worker)
    assert reserving.generation_floor == 2
    id = reserving.recovery_generation.io_id
    assert_receive {:recovery_io_timeout, ^id}, 1000
    failed = RecoveryGeneration.timeout(reserving, id)
    assert failed.recovery_generation.reason == :recovery_io_timeout
    assert failed.generation_floor == 2
    assert failed == RecoveryGeneration.result(failed, id, {:ok, %{generation: 2}})
    assert failed.bootstrap_reservation == nil
    assert failed.director == :unavailable

    assert [{:coordinator_checkpoint, %{generation_floor: 2}}] =
             :dets.lookup(Raft.log(failed.raft).table_name, :coordinator_checkpoint)
  end

  test "reservation task DOWN cannot return the consumed generation to a retry", %{reading: reading, worker: worker} do
    reserving = enter_reservation(reading, worker)
    %{worker: pid, monitor: ref, io_id: old_id} = reserving.recovery_generation
    send(pid, :crash)
    assert_receive {:DOWN, ^ref, :process, ^pid, :backend_crash}
    failed = RecoveryGeneration.down(reserving, ref, :backend_crash)
    assert failed.generation_floor == 2
    retry = RecoveryGeneration.request(failed)
    barrier = retry.recovery_generation.log_id

    assert {:noreply, reading_again} =
             Server.handle_info({:raft, :consensus_reached, Raft.log(retry.raft), barrier, :latest}, retry)

    assert_receive {:blocked_read, new_worker}
    assert reading_again == RecoveryGeneration.result(reading_again, old_id, {:error, :late})
    reserving_again = enter_reservation(reading_again, new_worker)
    assert reserving_again.generation_floor == 3
    RecoveryGeneration.cancel(reserving_again)
  end

  test "external CAS can finish after authority loss but its old token cannot publish", %{
    reading: reading,
    worker: worker,
    backend: backend
  } do
    reserving = enter_reservation(reading, worker)
    %{worker: io_worker, io_id: stale_id, monitor: stale_monitor} = reserving.recovery_generation
    send(io_worker, :release)
    assert_receive {:external_cas_ready, external}, 1000
    on_exit(fn -> if Process.alive?(external), do: Process.exit(external, :kill) end)
    follower = Raft.handle_event(reserving.raft, {:vote, 2}, :other)

    assert {:noreply, retired} =
             Server.handle_info({:raft, :leadership_changed, {:undecided, 2}}, %{reserving | raft: follower})

    assert retired.recovery_generation == nil
    assert retired.generation_floor == 2
    send(external, :finish)
    assert_receive {:external_cas_done, ^external, :ok}, 1000
    {:ok, old} = Publication.read(backend, "bootstrap")
    assert old.bootstrap.recovery_generation == 2
    assert retired == RecoveryGeneration.result(retired, stale_id, {:ok, %{generation: 2}})
    assert retired == RecoveryGeneration.down(retired, stale_monitor, :killed)
    assert retired == RecoveryGeneration.timeout(retired, stale_id)

    elected = Raft.handle_event(retired.raft, :election, :timer)
    assert Raft.leadership(elected) == {node(), 3}

    assert {:noreply, pending} =
             Server.handle_info({:raft, :leadership_changed, {node(), 3}}, %{retired | raft: elected})

    barrier = pending.recovery_generation.log_id

    assert {:noreply, reading_again} =
             Server.handle_info({:raft, :consensus_reached, Raft.log(elected), barrier, :latest}, pending)

    assert_receive {:blocked_read, next_worker}
    higher = enter_reservation(reading_again, next_worker)
    assert higher.generation_floor == 3
    send(higher.recovery_generation.worker, :release)
    assert_receive {:external_cas_ready, successor}, 1000
    on_exit(fn -> if Process.alive?(successor), do: Process.exit(successor, :kill) end)
    send(successor, :finish)
    assert_receive {:external_cas_done, ^successor, :ok}, 1000
    assert_receive {:blocked_read, verification}
    send(verification, :release)
    higher_id = higher.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^higher_id, {:ok, reserved3}}, 1000
    assert reserved3.generation == 3
    assert reserved3.prior_bootstrap == old.bootstrap

    stale_context = %{
      backend: backend,
      key: "bootstrap",
      generation: 2,
      recovery_id: old.bootstrap.recovery_id,
      version_token: old.version_token
    }

    old_final =
      Map.merge(old.bootstrap, %{epoch: 2, logs: [%{id: "stale-log"}], publication_id: old.bootstrap.recovery_id})

    assert {:error, :publication_mismatch} = Publication.publish(stale_context, old_final)
    assert higher == RecoveryGeneration.timeout(higher, stale_id)
    assert higher == RecoveryGeneration.down(higher, stale_monitor, :normal)
    RecoveryGeneration.cancel(higher)
  end

  defp enter_reservation(reading, worker) do
    send(worker, :release)
    id = reading.recovery_generation.io_id
    assert_receive {:recovery_io_result, ^id, result}, 1000
    allocating = RecoveryGeneration.result(reading, id, result)
    allocation = allocating.recovery_generation.log_id

    assert {:noreply, reserving} =
             Server.handle_info({:raft, :consensus_reached, Raft.log(allocating.raft), allocation, :latest}, allocating)

    assert reserving.recovery_generation.phase == :reserving
    assert_receive {:blocked_read, worker}
    on_exit(fn -> if Process.alive?(worker), do: Process.exit(worker, :kill) end)
    reserving
  end
end
