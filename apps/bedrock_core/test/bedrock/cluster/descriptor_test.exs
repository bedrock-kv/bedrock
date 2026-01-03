defmodule Bedrock.Cluster.DescriptorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Descriptor

  describe "new/2" do
    test "creates descriptor with cluster name and coordinator nodes" do
      nodes = [:node1, :node2]

      assert %{cluster_name: "cluster_name", coordinator_nodes: ^nodes} =
               Descriptor.new("cluster_name", nodes)
    end

    test "preserves node order" do
      nodes = [:node2, :node1, :node3]
      assert %{coordinator_nodes: ^nodes} = Descriptor.new("test", nodes)
    end
  end

  describe "write_to_file!/2" do
    @tag :tmp_dir
    test "writes descriptor to file with correct format", %{tmp_dir: tmp_dir} do
      descriptor = Descriptor.new("cluster_name", [:node1, :node2])
      path = Path.join([tmp_dir, "descriptor"])

      assert :ok = Descriptor.write_to_file!(path, descriptor)
      assert File.exists?(path)
      assert File.read!(path) == "cluster_name:node1,node2"
    end
  end

  describe "read_from_file!/1" do
    @tag :tmp_dir
    test "reads cluster name and coordinator nodes from file", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "descriptor"])
      :ok = File.write(path, "cluster_name:node1,node2")

      assert %{cluster_name: "cluster_name", coordinator_nodes: [:node1, :node2]} =
               Descriptor.read_from_file!(path)
    end

    test "raises exception with specific error message when file cannot be read" do
      error_msg = "Unable to read cluster descriptor: :unable_to_read_file"

      assert_raise RuntimeError, error_msg, fn ->
        Descriptor.read_from_file!("non_existent_file")
      end
    end
  end

  describe "read_from_file/1" do
    test "returns error tuple when file cannot be read" do
      assert {:error, :unable_to_read_file} == Descriptor.read_from_file("non_existent_file")
    end

    @tag :tmp_dir
    test "returns error tuple when descriptor format is invalid", %{tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "invalid_descriptor"])
      :ok = File.write(path, "invalid_descriptor")

      assert {:error, :invalid_cluster_descriptor} == Descriptor.read_from_file(path)
    end
  end

  describe "encode_cluster_file_contents/1" do
    test "encodes descriptor with various node configurations" do
      # Single node
      single_descriptor = Descriptor.new("single", [:node1])
      assert Descriptor.encode_cluster_file_contents(single_descriptor) == "single:node1"

      # Multiple nodes preserving order
      multi_descriptor = Descriptor.new("multi", [:node2, :node1, :node3])
      assert Descriptor.encode_cluster_file_contents(multi_descriptor) == "multi:node2,node1,node3"
    end
  end

  describe "parse_cluster_file_contents/1" do
    test "parses valid descriptor formats with various node configurations" do
      # Single node
      assert {:ok, descriptor} = Descriptor.parse_cluster_file_contents("single:node1")
      assert %{cluster_name: "single", coordinator_nodes: [:node1]} = descriptor

      # Multiple nodes preserving order
      assert {:ok, descriptor} = Descriptor.parse_cluster_file_contents("multi:node2,node1,node3")
      assert %{cluster_name: "multi", coordinator_nodes: [:node2, :node1, :node3]} = descriptor
    end

    test "returns error for invalid descriptor formats" do
      invalid_formats = [
        "invalid_descriptor",
        "missing_nodes:",
        ":missing_name",
        ""
      ]

      for invalid_format <- invalid_formats do
        assert {:error, :invalid_cluster_descriptor} =
                 Descriptor.parse_cluster_file_contents(invalid_format)
      end
    end
  end
end
