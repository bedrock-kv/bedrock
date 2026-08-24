defmodule Bedrock.ControlPlane.Coordinator.InitializationTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule TestCluster do
    @moduledoc false

    def fetch_coordinator_nodes, do: {:ok, [Node.self()]}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"coordinator_initialization_test_#{component}"
  end

  setup %{tmp_dir: tmp_dir} do
    backend = ObjectStorage.backend(LocalFilesystem, root: tmp_dir)
    Application.put_env(:bedrock, TestCluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, TestCluster) end)
    %{backend: backend}
  end

  test "bootstrap logs are recovery input, not a runnable layout", %{backend: backend} do
    bootstrap = %{
      cluster_id: "cluster-1",
      epoch: 7,
      logs: [%{id: "old-log", otp_ref: nil, shard_tags: []}],
      coordinators: [%{node: Atom.to_string(Node.self())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

    assert {:ok, state, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert state.prior_core_state == %{logs: %{"old-log" => []}}
    assert state.transaction_system_layout == nil
  end
end
