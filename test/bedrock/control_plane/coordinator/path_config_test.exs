defmodule Bedrock.ControlPlane.Coordinator.PathConfigTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.Raft.Log.TupleInMemoryLog

  @moduletag :tmp_dir

  # Mock cluster to test the path configuration pattern
  defmodule TestPathCluster do
    def node_config do
      Application.get_env(:bedrock, __MODULE__, [])
    end
  end

  describe "coordinator path configuration pattern" do
    test "coordinator creates disk log files when path provided", %{tmp_dir: tmp_dir} do
      # Configure cluster with coordinator path
      coordinator_path = Path.join(tmp_dir, "coordinator")
      Application.put_env(:bedrock, TestPathCluster, coordinator: [path: coordinator_path])

      # Use coordinator's actual implementation
      {:ok, raft_log} = Server.init_raft_log(TestPathCluster)

      # Verify it created the expected directory structure
      working_directory = Path.join(coordinator_path, "raft")
      assert File.exists?(working_directory)
      assert File.exists?(Path.join(working_directory, "raft_log.dets"))

      # Verify it's actually a DiskRaftLog
      assert %DiskRaftLog{} = raft_log

      # Clean up
      DiskRaftLog.close(raft_log)
    end

    test "coordinator uses disk log when path provided", %{tmp_dir: tmp_dir} do
      # Configure cluster with coordinator path
      coordinator_path = Path.join(tmp_dir, "coordinator")
      Application.put_env(:bedrock, TestPathCluster, coordinator: [path: coordinator_path])

      # Test init_raft_log function
      {:ok, raft_log} = Server.init_raft_log(TestPathCluster)

      # Should be a DiskRaftLog
      assert %DiskRaftLog{} = raft_log

      # Clean up
      DiskRaftLog.close(raft_log)
    end

    test "coordinator uses in-memory log when no path provided", %{tmp_dir: _tmp_dir} do
      # Configure cluster without coordinator path
      Application.put_env(:bedrock, TestPathCluster, coordinator: [])

      # Test init_raft_log function
      {:ok, raft_log} = Server.init_raft_log(TestPathCluster)

      # Should be a TupleInMemoryLog
      assert %TupleInMemoryLog{} = raft_log
    end
  end
end
