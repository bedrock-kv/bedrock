defmodule Bedrock.Directory.LayerTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node

  # Version key used by the directory layer
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "create/3" do
    test "creates a root directory with HCA prefix allocation" do
      # First call: check_version - checks if version exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        # No version yet
        nil
      end)

      # Second call: ensure_version_initialized - checks again to ensure
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        # Still no version
        nil
      end)

      # Third call: init_version - writes the version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Fourth call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        # Root directory key
        assert key == <<254>>
        # Directory doesn't exist
        nil
      end)

      # Fifth call: create the directory
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == <<254>>
        assert {<<0, 42>>, ""} = Bedrock.Key.unpack(value)
        :ok
      end)

      # Create layer with deterministic prefix allocation
      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)

      # Test creating root directory
      assert {:ok, %Node{prefix: <<0, 42>>, path: [], layer: nil}} = Directory.create(layer, [])
    end

    test "creates a subdirectory when parent exists" do
      # First call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        # No version yet
        nil
      end)

      # Second call: ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Third call: init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Fourth call: check if child directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        # Child doesn't exist
        nil
      end)

      # Fifth call: check if parent (root) exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        # Root directory
        assert key == <<254>>
        # Parent exists
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)

      # Sixth call: create the child directory
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        assert {<<0, 42>>, ""} = Bedrock.Key.unpack(value)
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{path: ["users"]}} = Directory.create(layer, ["users"])
    end

    test "fails when directory already exists" do
      # First call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Second call: ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Third call: init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Fourth call: check if directory exists - it does!
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        # Directory exists
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)

      layer = Layer.new(MockRepo)
      assert {:error, :directory_already_exists} = Directory.create(layer, ["users"])
    end

    test "fails when parent doesn't exist" do
      # First call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Second call: ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Third call: init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Fourth call: check if child directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users", "profiles"])
        assert key == expected_key
        # Child doesn't exist
        nil
      end)

      # Fifth call: check if parent exists - it doesn't!
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        # Parent doesn't exist
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:error, :parent_directory_does_not_exist} =
               Directory.create(layer, ["users", "profiles"])
    end
  end

  describe "open/3" do
    test "opens existing directory" do
      # First call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 42>>, "document"})
      end)

      # Second call: check_version for reads
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        # No version stored (compatible)
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "document", path: ["users"]}} =
               Directory.open(layer, ["users"])
    end

    test "fails when directory doesn't exist" do
      # First call: check if directory exists - it doesn't
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["nonexistent"])
        assert key == expected_key
        nil
      end)

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
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)

      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["users"]) == true
    end

    test "returns false when directory doesn't exist" do
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["nonexistent"])
        assert key == expected_key
        nil
      end)

      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["nonexistent"]) == false
    end
  end

  describe "metadata and versioning support" do
    test "creates directory with version and metadata" do
      # First call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Second call: ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # Third call: init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Fourth call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Fifth call: create directory with metadata
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == <<254>>
        assert {<<0, 42>>, "document", "1.0", {"created_by", "test"}} = Bedrock.Key.unpack(value)
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "document", version: "1.0", metadata: {"created_by", "test"}}} =
               Directory.create(layer, [],
                 layer: "document",
                 version: "1.0",
                 metadata: {"created_by", "test"}
               )
    end

    test "opens directory with existing metadata fields" do
      # First call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["users"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 42>>, "document", "2.0", {"updated", 1}})
      end)

      # Second call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "document", version: "2.0", metadata: {"updated", 1}}} =
               Directory.open(layer, ["users"])
    end

    test "handles legacy 2-tuple format for backward compatibility" do
      # First call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["legacy"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 42>>, "legacy"})
      end)

      # Second call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "legacy", version: nil, metadata: nil}} =
               Directory.open(layer, ["legacy"])
    end

    test "handles 3-tuple format with version only" do
      # First call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["versioned"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 42>>, "versioned", "1.5"})
      end)

      # Second call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, %Node{prefix: <<0, 42>>, layer: "versioned", version: "1.5", metadata: nil}} =
               Directory.open(layer, ["versioned"])
    end

    test "creates partition with metadata" do
      # First call: check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key = <<254>> <> Bedrock.Key.pack(["isolated_space"])
        assert key == expected_key
        Bedrock.Key.pack({<<0, 42>>, "partition", "1.0", {"type", "isolated"}})
      end)

      # Second call: check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, %Bedrock.Directory.Partition{prefix: <<0, 42>>, version: "1.0", metadata: {"type", "isolated"}}} =
               Directory.open(layer, ["isolated_space"])
    end
  end
end
