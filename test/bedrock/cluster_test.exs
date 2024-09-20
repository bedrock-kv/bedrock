defmodule Bedrock.ClusterTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import Mox
  alias Faker

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Descriptor

  setup :verify_on_exit!

  defmock(Bedrock.MockCluster, for: Cluster)

  describe "Bedrock.Cluster.default_descriptor_file_name/0" do
    test "returns the default descriptor file name" do
      assert Cluster.default_descriptor_file_name() == "bedrock.cluster"
    end
  end

  describe "Bedrock.Cluster.otp_name/2" do
    test "returns the OTP name for the given cluster and name" do
      name = Faker.Lorem.word()
      service = ~w[airport bus_station train_station taxi_stand]a |> Enum.random()

      assert Cluster.otp_name(name, service) == :"bedrock_#{name}_#{service}"
    end
  end

  describe "Bedrock.Cluster.child_spec/1" do
    test "raises when cluster: option is missing" do
      assert_raise RuntimeError, "Missing :cluster option", fn ->
        Cluster.child_spec([])
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

      opts = [cluster: Bedrock.MockCluster, path_to_descriptor: path_to_descriptor]

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      expected_message =
        "The cluster name in the descriptor file (#{non_matching_name}) does not match the cluster name (#{expected_name}) in the configuration."

      assert_raise RuntimeError, expected_message, fn ->
        Cluster.child_spec(opts)
      end
    end

    @tag :tmp_dir
    test "raises an exception when the current node is not setup as part of a cluster",
         %{tmp_dir: tmp_dir} do
      path_to_descriptor = Path.join([tmp_dir, "cluster_descriptor"])

      expected_name = Faker.Lorem.word()

      Descriptor.write_to_file!(path_to_descriptor, %Descriptor{
        cluster_name: expected_name,
        coordinator_nodes: [:node1, :node2]
      })

      opts = [cluster: Bedrock.MockCluster, path_to_descriptor: path_to_descriptor]

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      expected_message =
        "Bedrock: this node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"

      assert_raise RuntimeError, expected_message, fn ->
        Cluster.child_spec(opts)
      end
    end

    test "creates a child spec using the default path from the cluster when the path_to_descriptor option is omitted" do
      opts = [cluster: Bedrock.MockCluster]

      expected_name = Faker.Lorem.word()

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)
      expect(Bedrock.MockCluster, :path_to_descriptor, fn -> "path-to-invalid-descriptor" end)

      expected = %{
        id: Bedrock.Cluster,
        restart: :permanent,
        start:
          {Supervisor, :start_link,
           [
             Bedrock.Cluster,
             {Bedrock.MockCluster, "path-to-invalid-descriptor",
              %Bedrock.Cluster.Descriptor{
                cluster_name: expected_name,
                coordinator_nodes: [:nonode@nohost]
              }},
             []
           ]},
        type: :supervisor
      }

      log =
        capture_log(fn ->
          assert Cluster.child_spec(opts) == expected
        end)

      assert log =~ "Bedrock: Creating a default single-node configuration"
    end

    test "creates a child spec using the path_to_descriptor option" do
      opts = [cluster: Bedrock.MockCluster, path_to_descriptor: "path-to-invalid-descriptor"]

      expected_name = Faker.Lorem.word()

      expect(Bedrock.MockCluster, :name, fn -> expected_name end)

      expected = %{
        id: Bedrock.Cluster,
        restart: :permanent,
        start:
          {Supervisor, :start_link,
           [
             Bedrock.Cluster,
             {Bedrock.MockCluster, "path-to-invalid-descriptor",
              %Bedrock.Cluster.Descriptor{
                cluster_name: expected_name,
                coordinator_nodes: [:nonode@nohost]
              }},
             []
           ]},
        type: :supervisor
      }

      log =
        capture_log(fn ->
          assert Cluster.child_spec(opts) == expected
        end)

      assert log =~ "Bedrock: Creating a default single-node configuration"
    end
  end
end
