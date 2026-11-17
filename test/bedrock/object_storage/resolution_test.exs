defmodule Bedrock.ObjectStorage.ResolutionTest do
  # The cluster's durable state — the chunks and snapshots its workers
  # write, and the bootstrap the director persists and the coordinator
  # cold-boots from — must land in ONE backend. Each site used to find the
  # backend for itself, and they disagreed about where in the node config
  # it lived (bedrock-1cp), so a config any one of them honored could
  # split that state across backends.
  use ExUnit.Case, async: false

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Server, as: CoordinatorServer
  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.Internal.ClusterSupervisor
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Raft
  alias Bedrock.Service.Foreman

  @moduletag :tmp_dir
  @moduletag :capture_log

  defmodule ResolutionCluster do
    @moduledoc false
    def name, do: "object-storage-resolution"
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def fetch_coordinator_nodes, do: {:ok, [Node.self()]}
    def otp_name(component), do: :"object_storage_resolution_#{component}"
  end

  setup do
    app_default = Application.get_env(:bedrock, ObjectStorage)

    on_exit(fn ->
      Application.delete_env(:bedrock, ResolutionCluster)

      if app_default,
        do: Application.put_env(:bedrock, ObjectStorage, app_default),
        else: Application.delete_env(:bedrock, ObjectStorage)
    end)
  end

  describe "every site resolves the same backend" do
    test "a top-level object_storage", %{tmp_dir: tmp_dir} do
      shared = ObjectStorage.backend(LocalFilesystem, root: Path.join(tmp_dir, "shared"))
      node_path = Path.join(tmp_dir, "node")

      assert_one_backend(
        [object_storage: shared, coordinator: [path: node_path], log: [path: node_path]],
        shared
      )
    end

    test "the derived local backend when role paths differ", %{tmp_dir: tmp_dir} do
      coordinator_path = Path.join(tmp_dir, "coordinator")

      assert_one_backend(
        [coordinator: [path: coordinator_path], log: [path: Path.join(tmp_dir, "log")]],
        ObjectStorage.backend(LocalFilesystem, root: Path.join(coordinator_path, "object_storage"))
      )
    end
  end

  test "object_storage inside a capability section is refused, not silently overridden", %{tmp_dir: tmp_dir} do
    shared = ObjectStorage.backend(LocalFilesystem, root: Path.join(tmp_dir, "shared"))

    assert_raise RuntimeError, ~r/not in the :log section/, fn ->
      foreman_backend(
        capabilities: [:log],
        durability_mode: :relaxed,
        log: [path: Path.join(tmp_dir, "node"), object_storage: shared]
      )
    end
  end

  # Before bedrock-1cp this setting reached only materializer snapshots;
  # honoring it now would send an upgraded coordinator looking for a
  # bootstrap that was never written there.
  test "a leftover application-config backend fails startup", %{tmp_dir: tmp_dir} do
    Application.put_env(:bedrock, ObjectStorage, backend: :s3)

    assert_raise RuntimeError, ~r/top level of the node config/, fn ->
      foreman_backend(capabilities: [:log], durability_mode: :relaxed, log: [path: Path.join(tmp_dir, "node")])
    end
  end

  # The foreman's backend is the one every worker it starts writes to;
  # the director persists the bootstrap; the coordinator cold-boots from
  # it. All three have to meet in `expected`.
  defp assert_one_backend(sections, expected) do
    node_config = [capabilities: [:log], durability_mode: :relaxed] ++ sections
    Application.put_env(:bedrock, ResolutionCluster, node_config)

    persist_bootstrap(epoch: 7)
    assert {:ok, _} = ObjectStorage.get(expected, "bootstrap")

    assert coordinator_loaded_epoch() == 7
    assert foreman_backend(node_config) == expected
  end

  defp foreman_backend(node_config) do
    descriptor = %Descriptor{cluster_name: ResolutionCluster.name(), coordinator_nodes: [Node.self()]}

    {:ok, {_flags, children}} =
      ClusterSupervisor.init({Node.self(), ResolutionCluster, nil, node_config, "unused", descriptor})

    %{start: {Supervisor, :start_link, [foreman_children, _]}} = Enum.find(children, &(&1.id == Foreman.Supervisor))
    {Foreman.Server, foreman_opts} = List.keyfind(foreman_children, Foreman.Server, 0)
    foreman_opts[:object_storage]
  end

  defp persist_bootstrap(epoch: epoch) do
    attempt =
      %{cluster: ResolutionCluster, epoch: epoch, proxies: [self()], transaction_system_layout: %{logs: %{}}}
      |> recovery_attempt()
      |> Map.merge(%{pending_tx: Tx.new(), seated_materializer_members: %{}, prior_materializer_members: %{}})

    context = recovery_context(%{commit_transaction_fn: fn _, _, _ -> {:ok, 1, 0} end})

    context =
      put_in(
        context.cluster_config.parameters[:materializer_idle_timeout_ms],
        Parameters.default_materializer_idle_timeout_ms()
      )

    assert {_, :completed} = PersistencePhase.execute(attempt, context)
  end

  defp coordinator_loaded_epoch do
    {:ok, state, _continue} =
      CoordinatorServer.init({ResolutionCluster, ResolutionCluster.otp_name(:coordinator)})

    with %DiskRaftLog{} = raft_log <- Raft.log(state.raft), do: DiskRaftLog.close(raft_log)
    state.epoch
  end
end
