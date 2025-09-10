defmodule Bedrock.Directory.RootRestrictionsTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  # Version key used by the directory layer
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "root directory restrictions" do
    test "cannot open root directory" do
      # No expectations needed - should fail immediately
      layer = Layer.new(MockRepo)

      assert {:error, :cannot_open_root} = Directory.open(layer, [])
    end

    test "cannot remove root directory" do
      # No expectations needed - should fail immediately
      layer = Layer.new(MockRepo)

      assert {:error, :cannot_remove_root} = Directory.remove(layer, [])
    end

    test "cannot move root directory" do
      # No expectations needed - should fail immediately
      layer = Layer.new(MockRepo)

      assert {:error, :cannot_move_root} = Directory.move(layer, [], ["somewhere"])
    end

    test "cannot move to root directory" do
      # No expectations needed - should fail immediately
      layer = Layer.new(MockRepo)

      assert {:error, :cannot_move_to_root} = Directory.move(layer, ["somewhere"], [])
    end

    test "can create root directory" do
      # Version management expectations
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # Check if root exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Store root directory
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == <<254>>
        assert {prefix, ""} = Bedrock.Key.unpack(value)
        assert is_binary(prefix)
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 1>> end)

      assert {:ok, %{path: [], prefix: <<0, 1>>}} = Directory.create(layer, [])
    end

    test "can check if root exists" do
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)

      layer = Layer.new(MockRepo)

      assert Directory.exists?(layer, []) == true
    end

    test "can list root directory children" do
      # Version check for list operation
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        <<1::little-32, 0::little-32, 0::little-32>>
      end)

      # Range query for children
      expect(MockRepo, :range, fn :mock_txn, range ->
        expected_range = Bedrock.KeyRange.from_prefix(<<254>>)
        assert range == expected_range

        # Return some child directories with properly packed keys
        [
          {<<254>> <> Bedrock.Key.pack(["users"]), Bedrock.Key.pack({<<0, 2>>, ""})},
          {<<254>> <> Bedrock.Key.pack(["docs"]), Bedrock.Key.pack({<<0, 3>>, ""})}
        ]
      end)

      layer = Layer.new(MockRepo)

      assert {:ok, children} = Directory.list(layer, [])
      assert "users" in children
      assert "docs" in children
    end

    test "remove_if_exists handles root directory properly" do
      # Should not attempt to remove root
      layer = Layer.new(MockRepo)

      # remove_if_exists should treat root specially
      assert {:error, :cannot_remove_root} = Directory.remove_if_exists(layer, [])
    end
  end

  describe "root? helper" do
    test "identifies root path correctly" do
      assert Layer.root?([]) == true
      assert Layer.root?(["users"]) == false
      assert Layer.root?(["users", "profiles"]) == false
      assert Layer.root?(nil) == false
      assert Layer.root?("") == false
    end
  end
end
