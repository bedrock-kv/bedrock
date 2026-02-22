defmodule Bedrock.Internal.ClusterSupervisorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Mox

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Internal.ClusterSupervisor

  setup :verify_on_exit!

  defmock(Bedrock.MockCluster, for: Bedrock.Cluster)

  # Test helpers
  defp expect_cluster_name(cluster, name) do
    expect(cluster, :name, fn -> name end)
  end

  defp assert_log_contains(fun, expected_message) do
    log = capture_log(fun)
    assert log =~ expected_message
  end

  defp expected_child_spec(node, cluster, path_to_descriptor, descriptor) do
    %{
      id: ClusterSupervisor,
      restart: :permanent,
      start:
        {Supervisor, :start_link,
         [
           ClusterSupervisor,
           {node, cluster, nil, nil, path_to_descriptor, descriptor},
           []
         ]},
      type: :supervisor
    }
  end

  describe "child_spec/1" do
    test "raises when cluster: option is missing" do
      assert_raise RuntimeError, "Missing :cluster option", fn ->
        ClusterSupervisor.child_spec([])
      end
    end

    @tag :tmp_dir
    test "raises an exception when the cluster's name does not match the one read from the descriptor",
         %{tmp_dir: tmp_dir} do
      path_to_descriptor = Path.join([tmp_dir, "cluster_descriptor"])

      expected_name = "config_#{Faker.Lorem.word()}"
      non_matching_name = "descriptor_#{Faker.Lorem.word()}"

      Descriptor.write_to_file!(path_to_descriptor, %Descriptor{
        cluster_name: non_matching_name,
        coordinator_nodes: [:node1, :node2]
      })

      opts = [node: :foo, cluster: Bedrock.MockCluster, path_to_descriptor: path_to_descriptor]

      expected_message =
        "Bedrock: The cluster name in the descriptor file does not match the cluster name (#{expected_name}) in the configuration."

      expect_cluster_name(Bedrock.MockCluster, expected_name)

      assert_log_contains(
        fn -> ClusterSupervisor.child_spec(opts) end,
        expected_message
      )
    end

    @tag :tmp_dir
    test "logs a warning when the current node is not setup as part of a cluster",
         %{tmp_dir: tmp_dir} do
      path_to_descriptor = Path.join([tmp_dir, "cluster_descriptor"])

      expected_name = Faker.Lorem.word()

      Descriptor.write_to_file!(path_to_descriptor, %Descriptor{
        cluster_name: expected_name,
        coordinator_nodes: [:node1, :node2]
      })

      opts = [
        node: :nonode@nohost,
        cluster: Bedrock.MockCluster,
        path_to_descriptor: path_to_descriptor
      ]

      expected_message =
        ~s{Bedrock: This node is not part of a cluster (use the "--name" or "--sname" option when starting the Erlang VM)}

      expect_cluster_name(Bedrock.MockCluster, expected_name)

      assert_log_contains(
        fn -> ClusterSupervisor.child_spec(opts) end,
        expected_message
      )
    end

    test "creates a child spec using the default path when path_to_descriptor is omitted" do
      opts = [node: :some_node, cluster: Bedrock.MockCluster]
      expected_name = Faker.Lorem.word()

      expect_cluster_name(Bedrock.MockCluster, expected_name)

      expected_descriptor = %Descriptor{
        cluster_name: expected_name,
        coordinator_nodes: [:some_node]
      }

      expected_spec = expected_child_spec(:some_node, Bedrock.MockCluster, "bedrock.cluster", expected_descriptor)

      assert_log_contains(
        fn ->
          assert ^expected_spec = ClusterSupervisor.child_spec(opts)
        end,
        "Bedrock: Creating a default single-node configuration"
      )
    end

    test "creates a child spec using the provided path_to_descriptor" do
      opts = [
        node: :some_node,
        cluster: Bedrock.MockCluster,
        path_to_descriptor: "path-to-invalid-descriptor"
      ]

      expected_name = Faker.Lorem.word()

      expect_cluster_name(Bedrock.MockCluster, expected_name)

      expected_descriptor = %Descriptor{
        cluster_name: expected_name,
        coordinator_nodes: [:some_node]
      }

      expected_spec =
        expected_child_spec(:some_node, Bedrock.MockCluster, "path-to-invalid-descriptor", expected_descriptor)

      assert_log_contains(
        fn ->
          assert ^expected_spec = ClusterSupervisor.child_spec(opts)
        end,
        "Bedrock: Creating a default single-node configuration"
      )
    end
  end

  describe "durability profile enforcement" do
    test "fails startup checks in strict mode when requirements are unmet" do
      assert {:error, reasons} =
               ClusterSupervisor.enforce_durability_profile(
                 durability_mode: :strict,
                 coordinator: [],
                 log: [],
                 materializer: [],
                 durability: [desired_replication_factor: 1, desired_logs: 1]
               )

      assert :desired_replication_factor_too_low in reasons
      assert :desired_logs_too_low in reasons
      assert :missing_coordinator_path in reasons
    end

    test "warns in relaxed mode when requirements are unmet" do
      assert {:warn, reasons} =
               ClusterSupervisor.enforce_durability_profile(
                 durability_mode: :relaxed,
                 coordinator: [],
                 log: [],
                 materializer: [],
                 durability: [desired_replication_factor: 1, desired_logs: 1]
               )

      assert :coordinator_persistence_disabled in reasons
      assert :missing_materializer_path in reasons
    end

    test "passes in strict mode when requirements are met" do
      assert :ok =
               ClusterSupervisor.enforce_durability_profile(
                 durability_mode: :strict,
                 coordinator: [path: "/var/lib/bedrock/coordinator", persistent: true],
                 log: [path: "/var/lib/bedrock/log"],
                 materializer: [path: "/var/lib/bedrock/storage"],
                 durability: [desired_replication_factor: 3, desired_logs: 3]
               )
    end

    test "supports nested durability mode config" do
      assert :strict == ClusterSupervisor.durability_mode(durability: [mode: :strict])
      assert :relaxed == ClusterSupervisor.durability_mode(durability: [mode: :relaxed])
      assert :relaxed == ClusterSupervisor.durability_mode(durability: [mode: :unsupported])
    end
  end
end
