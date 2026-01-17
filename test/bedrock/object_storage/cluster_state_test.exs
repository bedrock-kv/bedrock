defmodule Bedrock.ObjectStorage.ClusterStateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ClusterState
  alias Bedrock.ObjectStorage.LocalFilesystem

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:director), do: :test_director
    def otp_name(:sequencer), do: :test_sequencer
    def otp_name(:rate_keeper), do: :test_rate_keeper
    def otp_name(:commit_proxy), do: :test_commit_proxy
    def otp_name(:resolver), do: :test_resolver
    def otp_name(_service_id), do: :test_service
  end

  setup do
    root = Path.join(System.tmp_dir!(), "cluster_state_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, backend: backend, root: root}
  end

  describe "save/5 and load/3" do
    test "round-trips basic config without transaction_system_layout", %{backend: backend} do
      config = %{
        coordinators: [:node1@host, :node2@host],
        parameters: %{
          desired_logs: 2,
          desired_replication: 2,
          desired_commit_proxies: 3,
          desired_coordinators: 3,
          desired_read_version_proxies: 2,
          empty_transaction_timeout_ms: 1000,
          ping_rate_in_hz: 10,
          retransmission_rate_in_hz: 5,
          transaction_window_in_ms: 5000
        },
        policies: %{
          allow_volunteer_nodes_to_join: true
        }
      }

      assert :ok = ClusterState.save(backend, "test-cluster", 42, config, TestCluster)
      assert {:ok, 42, loaded_config} = ClusterState.load(backend, "test-cluster", TestCluster)

      assert loaded_config.coordinators == config.coordinators
      assert loaded_config.parameters == config.parameters
      assert loaded_config.policies == config.policies
    end

    test "preserves epoch", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, "cluster", 123, config, TestCluster)
      assert {:ok, 123, _} = ClusterState.load(backend, "cluster", TestCluster)

      # Overwrite with new epoch
      assert :ok = ClusterState.save(backend, "cluster", 456, config, TestCluster)
      assert {:ok, 456, _} = ClusterState.load(backend, "cluster", TestCluster)
    end

    test "removes ephemeral recovery_attempt field", %{backend: backend} do
      config = %{
        coordinators: [node()],
        parameters: nil,
        policies: nil,
        recovery_attempt: %{some: :data}
      }

      assert :ok = ClusterState.save(backend, "cluster", 1, config, TestCluster)
      assert {:ok, 1, loaded_config} = ClusterState.load(backend, "cluster", TestCluster)

      refute Map.has_key?(loaded_config, :recovery_attempt)
    end
  end

  describe "load/3 errors" do
    test "returns not_found for missing state", %{backend: backend} do
      assert {:error, :not_found} = ClusterState.load(backend, "nonexistent", TestCluster)
    end
  end

  describe "load_raw/2" do
    test "loads without PID resolution", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, "cluster", 99, config, TestCluster)
      assert {:ok, 99, encoded_config} = ClusterState.load_raw(backend, "cluster")

      # Should have coordinators as-is
      assert encoded_config.coordinators == config.coordinators
    end

    test "returns not_found for missing state", %{backend: backend} do
      assert {:error, :not_found} = ClusterState.load_raw(backend, "nonexistent")
    end
  end

  describe "exists?/2" do
    test "returns false for non-existent state", %{backend: backend} do
      refute ClusterState.exists?(backend, "nonexistent")
    end

    test "returns true after save", %{backend: backend} do
      config = minimal_config()

      refute ClusterState.exists?(backend, "cluster")
      assert :ok = ClusterState.save(backend, "cluster", 1, config, TestCluster)
      assert ClusterState.exists?(backend, "cluster")
    end
  end

  describe "delete/2" do
    test "removes saved state", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, "cluster", 1, config, TestCluster)
      assert ClusterState.exists?(backend, "cluster")

      assert :ok = ClusterState.delete(backend, "cluster")
      refute ClusterState.exists?(backend, "cluster")
    end

    test "succeeds for non-existent state", %{backend: backend} do
      assert :ok = ClusterState.delete(backend, "nonexistent")
    end
  end

  describe "multiple clusters" do
    test "clusters are isolated", %{backend: backend} do
      config1 = %{minimal_config() | coordinators: [:node1@host]}
      config2 = %{minimal_config() | coordinators: [:node2@host]}

      assert :ok = ClusterState.save(backend, "cluster-1", 10, config1, TestCluster)
      assert :ok = ClusterState.save(backend, "cluster-2", 20, config2, TestCluster)

      assert {:ok, 10, loaded1} = ClusterState.load(backend, "cluster-1", TestCluster)
      assert {:ok, 20, loaded2} = ClusterState.load(backend, "cluster-2", TestCluster)

      assert loaded1.coordinators == [:node1@host]
      assert loaded2.coordinators == [:node2@host]
    end
  end

  describe "storage path" do
    test "uses correct path format", %{backend: backend, root: root} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, "my-cluster", 1, config, TestCluster)

      # Verify the file exists at expected path
      expected_path = Path.join(root, "my-cluster/state")
      assert File.exists?(expected_path)
    end
  end

  # Helper to create minimal valid config
  defp minimal_config do
    %{
      coordinators: [node()],
      parameters: nil,
      policies: nil
    }
  end
end
