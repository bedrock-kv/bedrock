defmodule Bedrock.Directory.LayerTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Key

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "create/3" do
    test "creates a root directory with empty prefix" do
      # No mock expectations needed - Directory.root() doesn't call the database
      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      # Root directory is now returned directly from Directory.root() with empty prefix
      assert %Directory.Node{prefix: "", path: [], layer: nil} = layer
    end

    test "creates a subdirectory when parent exists" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], nil)
      |> expect_directory_creation(["users"], {<<0, 42>>, ""})

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Directory.Node{path: ["users"]}} = Directory.create(layer, ["users"])
    end

    test "fails when directory already exists" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], Key.pack({<<0, 1>>, ""}))

      layer = Directory.root(MockRepo)
      assert {:error, :directory_already_exists} = Directory.create(layer, ["users"])
    end

    test "fails when parent doesn't exist" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users", "profiles"], nil)
      |> expect_directory_exists(["users"], nil)

      layer = Directory.root(MockRepo)

      assert {:error, :parent_directory_does_not_exist} =
               Directory.create(layer, ["users", "profiles"])
    end
  end

  describe "open/3" do
    test "opens existing directory" do
      MockRepo
      |> expect_directory_exists(["users"], Key.pack({<<0, 42>>, "document"}))
      |> expect_version_check()

      layer = Directory.root(MockRepo)

      assert {:ok, %Directory.Node{prefix: <<0, 42>>, layer: "document", path: ["users"]}} =
               Directory.open(layer, ["users"])
    end

    test "fails when directory doesn't exist" do
      expect_directory_exists(MockRepo, ["nonexistent"], nil)
      layer = Directory.root(MockRepo)
      assert {:error, :directory_does_not_exist} = Directory.open(layer, ["nonexistent"])
    end
  end

  describe "get_subspace/1" do
    test "returns subspace for directory node" do
      node = %Directory.Node{
        prefix: <<0, 42>>,
        path: ["users"],
        layer: nil,
        directory_layer: nil
      }

      subspace = Directory.get_subspace(node)
      assert subspace.prefix == <<0, 42>>
    end
  end

  describe "exists?/3" do
    test "returns true when directory exists" do
      expect_directory_exists(MockRepo, ["users"], Key.pack({<<0, 1>>, ""}))
      layer = Directory.root(MockRepo)
      assert Directory.exists?(layer, ["users"]) == true
    end

    test "returns false when directory doesn't exist" do
      expect_directory_exists(MockRepo, ["nonexistent"], nil)
      layer = Directory.root(MockRepo)
      assert Directory.exists?(layer, ["nonexistent"]) == false
    end
  end

  describe "metadata and versioning support" do
    test "creates directory with version and metadata" do
      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)

      # Root creation via Directory.create() is no longer supported
      assert {:error, :cannot_create_root} =
               Directory.create(layer, [],
                 layer: "document",
                 version: "1.0",
                 metadata: {"created_by", "test"}
               )
    end

    test "opens directory with existing metadata fields" do
      MockRepo
      |> expect_directory_exists(["users"], Key.pack({<<0, 42>>, "document", "2.0", {"updated", 1}}))
      |> expect_version_check()

      layer = Directory.root(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Directory.Node{
                prefix: <<0, 42>>,
                layer: "document",
                version: "2.0",
                metadata: {"updated", 1}
              }} = Directory.open(layer, ["users"])
    end

    test "handles legacy 2-tuple format for backward compatibility" do
      MockRepo
      |> expect_directory_exists(["legacy"], Key.pack({<<0, 42>>, "legacy"}))
      |> expect_version_check()

      layer = Directory.root(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Directory.Node{
                prefix: <<0, 42>>,
                layer: "legacy",
                version: nil,
                metadata: nil
              }} = Directory.open(layer, ["legacy"])
    end

    test "handles 3-tuple format with version only" do
      MockRepo
      |> expect_directory_exists(["versioned"], Key.pack({<<0, 42>>, "versioned", "1.5"}))
      |> expect_version_check()

      layer = Directory.root(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Directory.Node{
                prefix: <<0, 42>>,
                layer: "versioned",
                version: "1.5",
                metadata: nil
              }} = Directory.open(layer, ["versioned"])
    end

    test "creates partition with metadata" do
      MockRepo
      |> expect_directory_exists(
        ["isolated_space"],
        Key.pack({<<0, 42>>, "partition", "1.0", {"type", "isolated"}})
      )
      |> expect_version_check()

      layer = Directory.root(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Directory.Partition{
                prefix: <<0, 42>>,
                version: "1.0",
                metadata: {"type", "isolated"}
              }} = Directory.open(layer, ["isolated_space"])
    end
  end
end
