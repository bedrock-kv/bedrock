defmodule Bedrock.Cluster.DescriptorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Descriptor

  describe "Cluster.Descriptor" do
    test "new/2 creates a new descriptor" do
      descriptor = Descriptor.new("cluster_name", [:node1, :node2])

      assert descriptor.cluster_name == "cluster_name"
      assert descriptor.coordinator_nodes == [:node1, :node2]
    end

    @tag :tmp_dir
    test "write_to_file!/2 writes the descriptor to a file", %{tmp_dir: tmp_dir} do
      descriptor = Descriptor.new("cluster_name", [:node1, :node2])
      path = Path.join([tmp_dir, "descriptor"])
      :ok = Descriptor.write_to_file!(path, descriptor)

      assert File.read!(path) == "cluster_name:node1,node2"
    end

    @tag :tmp_dir
    test "read_from_file!/1 reads the descriptor from a file", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "descriptor"])
      :ok = File.write(path, "cluster_name:node1,node2")

      descriptor = Descriptor.read_from_file!(path)

      assert descriptor.cluster_name == "cluster_name"
      assert descriptor.coordinator_nodes == [:node1, :node2]
    end

    test "read_from_file!/1 raises an exception if the file cannot be read" do
      assert_raise RuntimeError, "Unable to read cluster descriptor: :unable_to_read_file", fn ->
        Descriptor.read_from_file!("non_existent_file")
      end
    end

    test "read_from_file/1 returns an error tuple if the file cannot be read" do
      assert {:error, :unable_to_read_file} == Descriptor.read_from_file("non_existent_file")
    end

    @tag :tmp_dir
    test "read_from_file/1 returns an error tuple if the cluster descriptor is invalid", %{
      tmp_dir: tmp_dir
    } do
      path = Path.join([tmp_dir, "invalid_descriptor"])
      :ok = File.write(path, "invalid_descriptor")

      assert {:error, :invalid_cluster_descriptor} ==
               Descriptor.read_from_file(path)
    end

    test "encode_cluster_file_contents/1 returns a string representation of the descriptor" do
      descriptor = Descriptor.new("test", [:node2, :node1])

      assert Descriptor.encode_cluster_file_contents(descriptor) == "test:node2,node1"
    end

    test "parse_cluster_file_contents/1 returns a descriptor if the descriptor is valid" do
      assert {:ok, Descriptor.new("test", [:node2, :node1])} ==
               Descriptor.parse_cluster_file_contents("test:node2,node1")
    end

    test "parse_cluster_file_contents/1 returns an error tuple if the descriptor is invalid" do
      assert {:error, :invalid_cluster_descriptor} ==
               Descriptor.parse_cluster_file_contents("invalid_descriptor")
    end
  end
end
