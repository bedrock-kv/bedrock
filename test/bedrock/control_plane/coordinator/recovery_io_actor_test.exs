defmodule Bedrock.ControlPlane.Coordinator.RecoveryIOActorTest do
  use ExUnit.Case, async: false

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
    def name, do: "recovery_io_actor"
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
  end

  defmodule BlockingBackend do
    @moduledoc false

    def get_with_version(config, key) do
      send(Keyword.fetch!(config, :owner), {:blocked_recovery_read, self()})

      receive do
        :release -> LocalFilesystem.get_with_version(config, key)
      end
    end
  end

  defmodule Actor do
    @moduledoc false

    def start(state) do
      pending = RecoveryGeneration.request(state)
      barrier = pending.recovery_generation.log_id

      {:noreply, reading} =
        Server.handle_info(
          {:raft, :consensus_reached, Raft.log(pending.raft), barrier, :latest},
          pending
        )

      :proc_lib.init_ack({:ok, self()})
      :gen_server.enter_loop(Server, [], reading)
    end
  end

  test "Coordinator commits registration while recovery object I/O is blocked", %{tmp_dir: root} do
    backend = {LocalFilesystem, root: Path.join(root, "objects")}

    initial = %{
      cluster_id: "responsive",
      epoch: 1,
      logs: [],
      coordinators: [%{node: Atom.to_string(node())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(initial))

    Application.put_env(
      :bedrock,
      Cluster,
      object_storage: {BlockingBackend, root: elem(backend, 1), owner: self()}
    )

    on_exit(fn -> Application.delete_env(:bedrock, Cluster) end)

    {:ok, log} =
      DiskRaftLog.open(DiskRaftLog.new(log_dir: Path.join(root, "raft"), table_name: :recovery_io_actor_log))

    raft = node() |> Raft.new([], log, RaftAdapter) |> Raft.handle_event(:election, :timer)

    state = %State{
      cluster: Cluster,
      cluster_id: "responsive",
      config: %{marker: :available},
      raft: raft,
      raft_term: 1,
      my_node: node(),
      leader_node: node(),
      last_durable_txn_id: Log.initial_transaction_id(log)
    }

    {:ok, coordinator} = :proc_lib.start(Actor, :start, [state])
    assert_receive {:blocked_recovery_read, worker}, 1_000

    on_exit(fn ->
      if Process.alive?(coordinator), do: Process.exit(coordinator, :kill)
      if Process.alive?(worker), do: Process.exit(worker, :kill)
      DiskRaftLog.close(log)
    end)

    assert {:ok, %{marker: :available}} = GenServer.call(coordinator, :fetch_config, 250)

    service = {"responsive-log", :log, {:responsive_log, node()}}
    assert {:ok, committed_id} = GenServer.call(coordinator, {:register_services, [service]}, 1_000)

    current = :sys.get_state(coordinator)
    assert current.recovery_generation.phase == :reading
    assert current.recovery_generation.worker == worker
    assert Process.alive?(worker)
    assert current.service_directory["responsive-log"] == {:log, {:responsive_log, node()}}
    assert current.last_durable_txn_id == committed_id

    assert [{:coordinator_checkpoint, checkpoint}] =
             :dets.lookup(log.table_name, :coordinator_checkpoint)

    assert checkpoint.last_durable_txn_id == committed_id
    assert checkpoint.service_directory == current.service_directory
  end
end
