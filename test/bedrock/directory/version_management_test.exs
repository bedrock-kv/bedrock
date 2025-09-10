defmodule Bedrock.Directory.VersionManagementTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node

  # Correct version key encoding - packed as ["version"] in node subspace
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "version initialization" do
    test "initializes version on first write operation (create)" do
      # Expect version check and initialization
      expect(MockRepo, :get, 3, fn
        _txn, key ->
          cond do
            # Version key check
            key == @version_key ->
              # Version not yet initialized
              nil

            # Directory check for root
            key == <<254>> ->
              # Directory doesn't exist
              nil

            # Parent check (root has no parent)
            true ->
              nil
          end
      end)

      # Expect version to be written
      expect(MockRepo, :put, 2, fn
        _txn, key, value ->
          if key == @version_key do
            # Verify version format (12 bytes: 3 x 32-bit little-endian)
            assert <<1::little-32, 0::little-32, 0::little-32>> == value
            :ok
          else
            # Regular directory creation
            :ok
          end
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, [])
    end

    test "does not reinitialize version if already set" do
      # Version already initialized
      expect(MockRepo, :get, 4, fn
        _txn, key ->
          cond do
            key == @version_key ->
              # Return existing version
              <<1::little-32, 0::little-32, 0::little-32>>

            # Check for directory "test"
            String.ends_with?(key, Bedrock.Key.pack(["test"])) ->
              # Directory doesn't exist yet
              nil

            # Root directory check for parent
            key == <<254>> ->
              # Root exists
              Bedrock.Key.pack({<<0, 1>>, ""})

            true ->
              nil
          end
      end)

      # Only expect directory put, not version put
      expect(MockRepo, :put, 1, fn _txn, _key, _value ->
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version compatibility checks" do
    test "allows reads from compatible versions" do
      # Setup version 1.0.0
      expect(MockRepo, :get, 2, fn
        _txn, key ->
          cond do
            key == @version_key ->
              <<1::little-32, 0::little-32, 0::little-32>>

            String.ends_with?(key, Bedrock.Key.pack(["test"])) ->
              # Directory exists
              Bedrock.Key.pack({<<0, 42>>, ""})

            true ->
              nil
          end
      end)

      layer = Layer.new(MockRepo)
      assert {:ok, %Node{prefix: <<0, 42>>}} = Directory.open(layer, ["test"])
    end

    test "blocks reads from incompatible major version" do
      # Setup version 2.0.0 (incompatible)
      test_key = <<254>> <> Bedrock.Key.pack(["test"])

      expect(MockRepo, :get, 2, fn
        _txn, key ->
          cond do
            key == @version_key ->
              <<2::little-32, 0::little-32, 0::little-32>>

            key == test_key ->
              # Directory exists
              Bedrock.Key.pack({<<0, 42>>, ""})

            true ->
              nil
          end
      end)

      layer = Layer.new(MockRepo)
      assert {:error, :incompatible_directory_version} = Directory.open(layer, ["test"])
    end

    test "blocks writes to newer minor version" do
      # Setup version 1.1.0 (newer minor)
      expect(MockRepo, :get, fn
        _txn, key ->
          cond do
            key == @version_key ->
              <<1::little-32, 1::little-32, 0::little-32>>

            String.ends_with?(key, Bedrock.Key.pack(["test"])) ->
              # Directory doesn't exist
              nil

            key == <<254>> ->
              # Root exists (parent check)
              Bedrock.Key.pack({<<0, 1>>, ""})

            true ->
              nil
          end
      end)

      layer = Layer.new(MockRepo)
      assert {:error, :directory_version_write_restricted} = Directory.create(layer, ["test"])
    end

    test "allows writes to same or older minor version" do
      # Setup version 1.0.0 (same)
      expect(MockRepo, :get, 4, fn
        _txn, key ->
          cond do
            key == @version_key ->
              <<1::little-32, 0::little-32, 0::little-32>>

            String.ends_with?(key, Bedrock.Key.pack(["test"])) ->
              # Directory doesn't exist
              nil

            key == <<254>> ->
              # Root exists for parent check
              Bedrock.Key.pack({<<0, 1>>, ""})

            true ->
              nil
          end
      end)

      expect(MockRepo, :put, fn _txn, _key, _value ->
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version checks in operations" do
    test "remove operation checks version" do
      # Version check first, then existence check
      expect(MockRepo, :get, 3, fn
        _txn, key ->
          cond do
            key == @version_key ->
              # No version yet, will need initialization
              nil

            String.ends_with?(key, Bedrock.Key.pack(["test"])) ->
              # Directory exists
              Bedrock.Key.pack({<<0, 42>>, ""})

            true ->
              nil
          end
      end)

      # Expect version initialization for remove
      expect(MockRepo, :put, fn _txn, key, value ->
        if key == @version_key do
          assert <<1::little-32, 0::little-32, 0::little-32>> == value
          :ok
        else
          :ok
        end
      end)

      expect(MockRepo, :clear_range, fn _txn, _range ->
        :ok
      end)

      layer = Layer.new(MockRepo)
      assert :ok = Directory.remove(layer, ["test"])
    end

    test "move operation checks version" do
      # All get operations combined (6 now: version check, old exists, new doesn't exist,
      # parent exists, version for ensure, and layer type check in do_move_recursive)
      expect(MockRepo, :get, 6, fn
        _txn, key ->
          cond do
            # Version check
            key == @version_key ->
              <<1::little-32, 0::little-32, 0::little-32>>

            # Source exists (checked twice: once in do_move, once in do_move_recursive)
            String.ends_with?(key, Bedrock.Key.pack(["old"])) ->
              Bedrock.Key.pack({<<0, 42>>, ""})

            # Target doesn't exist
            String.ends_with?(key, Bedrock.Key.pack(["new"])) ->
              nil

            # Parent of new (root) exists - it's an empty key
            key == <<254>> ->
              # Root always exists
              Bedrock.Key.pack({<<0, 1>>, ""})

            true ->
              nil
          end
      end)

      # Get items to move
      expect(MockRepo, :range, fn _txn, _range ->
        [{<<254>> <> Bedrock.Key.pack(["old"]), Bedrock.Key.pack({<<0, 42>>, ""})}]
      end)

      # Put moved item
      expect(MockRepo, :put, fn _txn, _key, _value ->
        :ok
      end)

      # Clear old items
      expect(MockRepo, :clear_range, fn _txn, _range ->
        :ok
      end)

      layer = Layer.new(MockRepo)
      assert :ok = Directory.move(layer, ["old"], ["new"])
    end

    test "list operation checks version for reads" do
      # Version check
      expect(MockRepo, :get, fn
        _txn, key ->
          if key == @version_key do
            <<1::little-32, 0::little-32, 0::little-32>>
          end
      end)

      expect(MockRepo, :range, fn _txn, _range ->
        []
      end)

      layer = Layer.new(MockRepo)
      assert {:ok, []} = Directory.list(layer, [])
    end

    test "exists? operation does not check version directly" do
      # No version check for exists? since it's used internally
      expect(MockRepo, :get, fn
        _txn, key ->
          if String.ends_with?(key, Bedrock.Key.pack(["test"])) do
            # Directory exists
            Bedrock.Key.pack({<<0, 42>>, ""})
          end
      end)

      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["test"])
    end
  end
end
