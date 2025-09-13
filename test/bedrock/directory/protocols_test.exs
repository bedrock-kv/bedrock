defmodule Bedrock.Directory.ProtocolsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node
  alias Bedrock.Directory.Partition
  alias Bedrock.Subspace

  describe "String.Chars for Node" do
    test "formats path and layer correctly" do
      node = %Node{
        path: ["users", "profile"],
        layer: "profile_data",
        prefix: <<1, 2, 3>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(node) == ~s{DirectoryNode<users/profile@"profile_data">}
    end

    test "handles nil layer" do
      node = %Node{
        path: ["system"],
        layer: nil,
        prefix: <<7, 8>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(node) == "DirectoryNode<system>"
    end

    test "handles empty string layer" do
      node = %Node{
        path: ["config"],
        layer: "",
        prefix: <<9, 10>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(node) == "DirectoryNode<config>"
    end

    test "handles empty path" do
      node = %Node{
        path: [],
        layer: "root_layer",
        prefix: <<1>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(node) == ~s{DirectoryNode<@"root_layer">}
    end

    test "handles single path element" do
      node = %Node{
        path: ["users"],
        layer: "user_data",
        prefix: <<1, 2>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(node) == ~s{DirectoryNode<users@"user_data">}
    end
  end

  describe "String.Chars for Partition" do
    test "formats path correctly" do
      partition = %Partition{
        path: ["app", "data"],
        prefix: <<4, 5, 6>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(partition) == "DirectoryPartition<app/data>"
    end

    test "handles empty path" do
      partition = %Partition{
        path: [],
        prefix: <<1>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(partition) == "DirectoryPartition<>"
    end

    test "handles single path element" do
      partition = %Partition{
        path: ["main"],
        prefix: <<1, 2>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert to_string(partition) == "DirectoryPartition<main>"
    end
  end

  describe "String.Chars for Layer" do
    test "formats path correctly" do
      layer = %Layer{
        path: ["root"],
        node_subspace: Subspace.new(<<1>>),
        content_subspace: Subspace.new(<<2>>),
        repo: MyApp.Repo,
        next_prefix_fn: nil
      }

      assert to_string(layer) == "DirectoryLayer<root>"
    end

    test "handles empty path" do
      layer = %Layer{
        path: [],
        node_subspace: nil,
        content_subspace: nil,
        repo: nil,
        next_prefix_fn: nil
      }

      assert to_string(layer) == "DirectoryLayer<>"
    end

    test "handles complex nested path" do
      layer = %Layer{
        path: ["app", "services", "auth"],
        node_subspace: nil,
        content_subspace: nil,
        repo: nil,
        next_prefix_fn: nil
      }

      assert to_string(layer) == "DirectoryLayer<app/services/auth>"
    end
  end

  describe "Inspect for Node" do
    test "includes path, layer, and prefix" do
      node = %Node{
        path: ["users", "profile"],
        layer: "profile_data",
        prefix: <<1, 2, 3>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(node) == ~s{#DirectoryNode<users/profile@"profile_data", prefix: <<1, 2, 3>>>}
    end

    test "handles nil layer in inspect" do
      node = %Node{
        path: ["system"],
        layer: nil,
        prefix: <<7, 8>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(node) == "#DirectoryNode<system, prefix: \"\\a\\b\">"
    end

    test "handles empty string layer in inspect" do
      node = %Node{
        path: ["config"],
        layer: "",
        prefix: <<9, 10>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(node) == "#DirectoryNode<config, prefix: \"\\t\\n\">"
    end

    test "handles binary layer data" do
      node = %Node{
        path: ["binary_test"],
        layer: <<0, 1, 2>>,
        prefix: <<3, 4>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(node) == "#DirectoryNode<binary_test@<<0, 1, 2>>, prefix: <<3, 4>>>"
    end
  end

  describe "Inspect for Partition" do
    test "includes path and prefix" do
      partition = %Partition{
        path: ["app", "data"],
        prefix: <<4, 5, 6>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(partition) == "#DirectoryPartition<app/data, prefix: <<4, 5, 6>>>"
    end

    test "handles empty prefix" do
      partition = %Partition{
        path: ["test"],
        prefix: <<>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      assert inspect(partition) == "#DirectoryPartition<test, prefix: \"\">"
    end
  end

  describe "Inspect for Layer" do
    test "includes path and repo" do
      layer = %Layer{
        path: ["root"],
        node_subspace: Subspace.new(<<1>>),
        content_subspace: Subspace.new(<<2>>),
        repo: MyApp.Repo,
        next_prefix_fn: nil
      }

      assert inspect(layer) == "#DirectoryLayer<root, repo: MyApp.Repo>"
    end

    test "handles nil repo" do
      layer = %Layer{
        path: ["test"],
        node_subspace: nil,
        content_subspace: nil,
        repo: nil,
        next_prefix_fn: nil
      }

      assert inspect(layer) == "#DirectoryLayer<test, repo: nil>"
    end

    test "handles module repo" do
      layer = %Layer{
        path: ["services"],
        node_subspace: nil,
        content_subspace: nil,
        repo: Bedrock.TestRepo,
        next_prefix_fn: nil
      }

      assert inspect(layer) == "#DirectoryLayer<services, repo: Bedrock.TestRepo>"
    end
  end

  describe "protocol consistency" do
    test "String.Chars and Inspect show similar but distinct information" do
      node = %Node{
        path: ["users"],
        layer: "data",
        prefix: <<1, 2>>,
        directory_layer: nil,
        version: nil,
        metadata: nil
      }

      string_chars = to_string(node)
      inspect_result = inspect(node)

      # Both should include the path and layer
      assert string_chars =~ "users"
      assert string_chars =~ "data"
      assert inspect_result =~ "users"
      assert inspect_result =~ "data"

      # Inspect should include additional details
      assert inspect_result =~ "prefix: <<1, 2>>"
      refute string_chars =~ "prefix:"

      # String.Chars should be shorter/cleaner
      assert String.length(string_chars) < String.length(inspect_result)
    end
  end
end
