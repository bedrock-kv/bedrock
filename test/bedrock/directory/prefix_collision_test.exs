defmodule Bedrock.Directory.PrefixCollisionTest do
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

  describe "prefix collision detection" do
    test "rejects manual prefix that collides with existing data" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Check for prefix collision - simulate existing data with prefix <<1, 2>>
      expect(MockRepo, :range, fn :mock_txn, range, opts ->
        assert opts[:limit] == 1
        # Should be checking for keys that start with our prefix <<1, 2>>
        expected_range = Bedrock.KeyRange.from_prefix(<<1, 2>>)
        assert range == expected_range
        # Return a result to indicate collision
        [{<<1, 2, 3>>, "some_data"}]
      end)

      layer = Layer.new(MockRepo)

      # Try to create with a colliding prefix
      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: <<1, 2>>)
    end

    test "accepts manual prefix that doesn't collide" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Check for prefix collision - no existing data
      expect(MockRepo, :range, fn :mock_txn, range, opts ->
        assert opts[:limit] == 1
        # Should be checking for keys that start with our prefix <<10, 20>>
        expected_range = Bedrock.KeyRange.from_prefix(<<10, 20>>)
        assert range == expected_range
        []
      end)

      # Check ancestor prefixes - none exist
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<10>>
        nil
      end)

      # Store the directory
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == <<254>>
        assert {<<10, 20>>, ""} = Bedrock.Key.unpack(value)
        :ok
      end)

      layer = Layer.new(MockRepo)

      # Create with a non-colliding prefix
      assert {:ok, %{prefix: <<10, 20>>}} =
               Directory.create(layer, [], prefix: <<10, 20>>)
    end

    test "rejects reserved system prefix 0xFE" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      layer = Layer.new(MockRepo)

      # Try to create with reserved prefix 0xFE
      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: <<0xFE>>)
    end

    test "rejects reserved system prefix 0xFF" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      layer = Layer.new(MockRepo)

      # Try to create with reserved prefix 0xFF
      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: <<0xFF>>)
    end

    test "detects when new prefix would be ancestor of existing key" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Check for prefix collision - existing key starts with our prefix
      expect(MockRepo, :range, fn :mock_txn, range, opts ->
        assert opts[:limit] == 1
        # Should be checking for keys that start with our prefix <<1, 2>>
        expected_range = Bedrock.KeyRange.from_prefix(<<1, 2>>)
        assert range == expected_range
        # Existing key <<1, 2, 3>> starts with our prefix <<1, 2>>
        [{<<1, 2, 3>>, "existing_data"}]
      end)

      layer = Layer.new(MockRepo)

      # Try to create with prefix that would be ancestor
      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: <<1, 2>>)
    end

    test "detects when existing key would be ancestor of new prefix" do
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

      # Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<254>>
        nil
      end)

      # Check for prefix collision - no keys start with our full prefix
      expect(MockRepo, :range, fn :mock_txn, range, opts ->
        assert opts[:limit] == 1
        # Should be checking for keys that start with our prefix <<1, 2, 3>>
        expected_range = Bedrock.KeyRange.from_prefix(<<1, 2, 3>>)
        assert range == expected_range
        []
      end)

      # Check ancestor prefixes - <<1>> exists!
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == <<1>>
        # This key exists
        "existing_data"
      end)

      layer = Layer.new(MockRepo)

      # Try to create with prefix that extends existing key
      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: <<1, 2, 3>>)
    end
  end
end
