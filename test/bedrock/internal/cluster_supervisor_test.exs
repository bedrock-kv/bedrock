defmodule Bedrock.Internal.ClusterSupervisorTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import Mox
  alias Faker

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Internal.ClusterSupervisor

  setup :verify_on_exit!

  defmock(Bedrock.MockCluster, for: Bedrock.Cluster)

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

      expected_name = Faker.Lorem.word()
      non_matching_name = Faker.Lorem.word()

      Descriptor.write_to_file!(path_to_descriptor, %Descriptor{
        cluster_name: non_matching_name,
        coordinator_nodes: [:node1, :node2]
      })

      opts = [node: :foo, cluster: Bedrock.MockCluster, path_to_descriptor: path_to_descriptor]

      expected_message =
        "Bedrock: The cluster name in the descriptor file does not match the cluster name (#{expected_name}) in the configuration."

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      log =
        capture_log(fn ->
          ClusterSupervisor.child_spec(opts)
        end)

      assert log =~ expected_message
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
        "Bedrock: This node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      log =
        capture_log(fn ->
          ClusterSupervisor.child_spec(opts)
        end)

      assert log =~ expected_message
    end

    test "creates a child spec using the default path from the cluster when the path_to_descriptor option is omitted" do
      opts = [node: :some_node, cluster: Bedrock.MockCluster]

      expected_name = Faker.Lorem.word()

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      expected_spec = %{
        id: Bedrock.Internal.ClusterSupervisor,
        restart: :permanent,
        start:
          {Supervisor, :start_link,
           [
             Bedrock.Internal.ClusterSupervisor,
             {:some_node, Bedrock.MockCluster, nil, nil, "bedrock.cluster",
              %Bedrock.Cluster.Descriptor{
                cluster_name: expected_name,
                coordinator_nodes: [:some_node]
              }},
             []
           ]},
        type: :supervisor
      }

      expected_message = "Bedrock: Creating a default single-node configuration"

      log =
        capture_log(fn ->
          assert expected_spec == ClusterSupervisor.child_spec(opts)
        end)

      assert log =~ expected_message
    end

    test "creates a child spec using the path_to_descriptor option" do
      opts = [
        node: :some_node,
        cluster: Bedrock.MockCluster,
        path_to_descriptor: "path-to-invalid-descriptor"
      ]

      expected_name = Faker.Lorem.word()

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      expected_spec = %{
        id: Bedrock.Internal.ClusterSupervisor,
        restart: :permanent,
        start:
          {Supervisor, :start_link,
           [
             Bedrock.Internal.ClusterSupervisor,
             {:some_node, Bedrock.MockCluster, nil, nil, "path-to-invalid-descriptor",
              %Bedrock.Cluster.Descriptor{
                cluster_name: expected_name,
                coordinator_nodes: [:some_node]
              }},
             []
           ]},
        type: :supervisor
      }

      log =
        capture_log(fn ->
          assert expected_spec == ClusterSupervisor.child_spec(opts)
        end)

      assert log =~ "Bedrock: Creating a default single-node configuration"
    end
  end
end
