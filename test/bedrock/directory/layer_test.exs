defmodule Bedrock.Directory.LayerTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "create/3" do
    test "creates a root directory with HCA prefix allocation" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_directory_creation([], {<<0, 42>>, ""})

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{prefix: <<0, 42>>, path: [], layer: nil}} = Directory.create(layer, [])
    end

    test "creates a subdirectory when parent exists" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], nil)
      |> expect_directory_exists([], Bedrock.Key.pack({<<0, 1>>, ""}))
      |> expect_directory_creation(["users"], {<<0, 42>>, ""})

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{path: ["users"]}} = Directory.create(layer, ["users"])
    end

    test "fails when directory already exists" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], Bedrock.Key.pack({<<0, 1>>, ""}))

      layer = Layer.new(MockRepo)
      assert {:error, :directory_already_exists} = Directory.create(layer, ["users"])
    end

    test "fails when parent doesn't exist" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users", "profiles"], nil)
      |> expect_directory_exists(["users"], nil)

      layer = Layer.new(MockRepo)

      assert {:error, :parent_directory_does_not_exist} =
               Directory.create(layer, ["users", "profiles"])
    end
  end

  describe "open/3" do
    test "opens existing directory" do
      MockRepo
      |> expect_directory_exists(["users"], Bedrock.Key.pack({<<0, 42>>, "document"}))
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "document", path: ["users"]}} =
               Directory.open(layer, ["users"])
    end

    test "fails when directory doesn't exist" do
      expect_directory_exists(MockRepo, ["nonexistent"], nil)
      layer = Layer.new(MockRepo)
      assert {:error, :directory_does_not_exist} = Directory.open(layer, ["nonexistent"])
    end
  end

  describe "get_subspace/1" do
    test "returns subspace for directory node" do
      node = %Node{
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
      expect_directory_exists(MockRepo, ["users"], Bedrock.Key.pack({<<0, 1>>, ""}))
      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["users"]) == true
    end

    test "returns false when directory doesn't exist" do
      expect_directory_exists(MockRepo, ["nonexistent"], nil)
      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["nonexistent"]) == false
    end
  end

  describe "metadata and versioning support" do
    test "creates directory with version and metadata" do
      expected_key = <<254>>

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect(:put, fn :mock_txn, ^expected_key, value ->
        assert {<<0, 42>>, "document", "1.0", {"created_by", "test"}} = Bedrock.Key.unpack(value)
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Node{
                prefix: <<0, 42>>,
                layer: "document",
                version: "1.0",
                metadata: {"created_by", "test"}
              }} =
               Directory.create(layer, [],
                 layer: "document",
                 version: "1.0",
                 metadata: {"created_by", "test"}
               )
    end

    test "opens directory with existing metadata fields" do
      MockRepo
      |> expect_directory_exists(["users"], Bedrock.Key.pack({<<0, 42>>, "document", "2.0", {"updated", 1}}))
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Node{
                prefix: <<0, 42>>,
                layer: "document",
                version: "2.0",
                metadata: {"updated", 1}
              }} = Directory.open(layer, ["users"])
    end

    test "handles legacy 2-tuple format for backward compatibility" do
      MockRepo
      |> expect_directory_exists(["legacy"], Bedrock.Key.pack({<<0, 42>>, "legacy"}))
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Node{
                prefix: <<0, 42>>,
                layer: "legacy",
                version: nil,
                metadata: nil
              }} = Directory.open(layer, ["legacy"])
    end

    test "handles 3-tuple format with version only" do
      MockRepo
      |> expect_directory_exists(["versioned"], Bedrock.Key.pack({<<0, 42>>, "versioned", "1.5"}))
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Node{
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
        Bedrock.Key.pack({<<0, 42>>, "partition", "1.0", {"type", "isolated"}})
      )
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)

      # Use pattern matching for struct assertion
      assert {:ok,
              %Bedrock.Directory.Partition{
                prefix: <<0, 42>>,
                version: "1.0",
                metadata: {"type", "isolated"}
              }} = Directory.open(layer, ["isolated_space"])
    end
  end
end
