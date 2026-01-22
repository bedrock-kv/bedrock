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

  describe "save/4 and load/2" do
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

      assert :ok = ClusterState.save(backend, 42, config, TestCluster)
      assert {:ok, 42, loaded_config} = ClusterState.load(backend, TestCluster)

      assert loaded_config.coordinators == config.coordinators
      assert loaded_config.parameters == config.parameters
      assert loaded_config.policies == config.policies
    end

    test "preserves epoch", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, 123, config, TestCluster)
      assert {:ok, 123, _} = ClusterState.load(backend, TestCluster)

      # Overwrite with new epoch
      assert :ok = ClusterState.save(backend, 456, config, TestCluster)
      assert {:ok, 456, _} = ClusterState.load(backend, TestCluster)
    end

    test "removes ephemeral recovery_attempt field", %{backend: backend} do
      config = %{
        coordinators: [node()],
        parameters: nil,
        policies: nil,
        recovery_attempt: %{some: :data}
      }

      assert :ok = ClusterState.save(backend, 1, config, TestCluster)
      assert {:ok, 1, loaded_config} = ClusterState.load(backend, TestCluster)

      refute Map.has_key?(loaded_config, :recovery_attempt)
    end
  end

  describe "load/2 errors" do
    test "returns not_found for missing state", %{backend: backend} do
      assert {:error, :not_found} = ClusterState.load(backend, TestCluster)
    end
  end

  describe "load_raw/1" do
    test "loads without PID resolution", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, 99, config, TestCluster)
      assert {:ok, 99, encoded_config} = ClusterState.load_raw(backend)

      # Should have coordinators as-is
      assert encoded_config.coordinators == config.coordinators
    end

    test "returns not_found for missing state", %{backend: backend} do
      assert {:error, :not_found} = ClusterState.load_raw(backend)
    end
  end

  describe "exists?/1" do
    test "returns false for non-existent state", %{backend: backend} do
      refute ClusterState.exists?(backend)
    end

    test "returns true after save", %{backend: backend} do
      config = minimal_config()

      refute ClusterState.exists?(backend)
      assert :ok = ClusterState.save(backend, 1, config, TestCluster)
      assert ClusterState.exists?(backend)
    end
  end

  describe "delete/1" do
    test "removes saved state", %{backend: backend} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, 1, config, TestCluster)
      assert ClusterState.exists?(backend)

      assert :ok = ClusterState.delete(backend)
      refute ClusterState.exists?(backend)
    end

    test "succeeds for non-existent state", %{backend: backend} do
      assert :ok = ClusterState.delete(backend)
    end
  end

  describe "storage path" do
    test "uses correct path format", %{backend: backend, root: root} do
      config = minimal_config()

      assert :ok = ClusterState.save(backend, 1, config, TestCluster)

      # Verify the file exists at expected path (now just "state" at root)
      expected_path = Path.join(root, "state")
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
