defmodule Bedrock.Directory.MoveTest do
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

  test "move directory preserves metadata and prefixes" do
    # 1. check_version - check if version exists
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      # No version yet
      nil
    end)

    # 2. ensure_version_initialized - check again
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      nil
    end)

    # 3. init_version - write version
    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == @version_key
      assert value == <<1::little-32, 0::little-32, 0::little-32>>
      :ok
    end)

    # 4. Check if source directory exists
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["users"])
      # Return users directory metadata
      Bedrock.Key.pack({<<0, 2>>, "document"})
    end)

    # 5. Check if destination directory exists (should not)
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["accounts"])
      # Destination doesn't exist
      nil
    end)

    # 6. Check if destination parent exists (root in this case)
    expect(MockRepo, :get, fn :mock_txn, key ->
      # Root directory
      assert key == <<254>>
      # Root exists
      Bedrock.Key.pack({<<0, 1>>, ""})
    end)

    # 7. Check layer type in do_move_recursive (to see if it's a partition)
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["users"])
      # Return users directory metadata (not a partition)
      Bedrock.Key.pack({<<0, 2>>, "document"})
    end)

    # 8. Range scan to get all entries under /users
    expect(MockRepo, :range, fn :mock_txn, key_range ->
      base_key = <<254>> <> Bedrock.Key.pack(["users"])
      expected_range = Bedrock.KeyRange.from_prefix(base_key)

      assert key_range == expected_range

      # Return directories in lexicographic order
      [
        # The /users directory itself
        {<<254>> <> Bedrock.Key.pack(["users"]), Bedrock.Key.pack({<<0, 2>>, "document"})},
        # The /users/profiles subdirectory
        {<<254>> <> Bedrock.Key.pack(["users", "profiles"]), Bedrock.Key.pack({<<0, 3>>, "profile"})}
      ]
    end)

    # 9. Put /users directory at /accounts location (preserving prefix and layer)
    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == <<254>> <> Bedrock.Key.pack(["accounts"])
      # Same prefix and layer as original
      assert {<<0, 2>>, "document"} = Bedrock.Key.unpack(value)
      :ok
    end)

    # 10. Put /users/profiles directory at /accounts/profiles location
    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == <<254>> <> Bedrock.Key.pack(["accounts", "profiles"])
      # Same prefix and layer as original
      assert {<<0, 3>>, "profile"} = Bedrock.Key.unpack(value)
      :ok
    end)

    # 11. Clear the old /users range
    expect(MockRepo, :clear_range, fn :mock_txn, key_range ->
      base_key = <<254>> <> Bedrock.Key.pack(["users"])
      expected_range = Bedrock.KeyRange.from_prefix(base_key)

      assert key_range == expected_range
      :ok
    end)

    layer = Layer.new(MockRepo)

    # Execute move operation
    assert :ok = Directory.move(layer, ["users"], ["accounts"])
  end

  test "move fails when source doesn't exist" do
    # 1. check_version
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      nil
    end)

    # 2. ensure_version_initialized
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      nil
    end)

    # 3. init_version
    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == @version_key
      assert value == <<1::little-32, 0::little-32, 0::little-32>>
      :ok
    end)

    # 4. Check if source exists - it doesn't
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["nonexistent"])
      nil
    end)

    layer = Layer.new(MockRepo)

    assert {:error, :directory_does_not_exist} =
             Directory.move(layer, ["nonexistent"], ["new"])
  end

  test "move fails when destination exists" do
    # 1. check_version
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      nil
    end)

    # 2. ensure_version_initialized
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      nil
    end)

    # 3. init_version
    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == @version_key
      assert value == <<1::little-32, 0::little-32, 0::little-32>>
      :ok
    end)

    # 4. Check if source exists - it does
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["source"])
      Bedrock.Key.pack({<<0, 1>>, ""})
    end)

    # 5. Check if destination exists - it does!
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["existing"])
      Bedrock.Key.pack({<<0, 2>>, ""})
    end)

    layer = Layer.new(MockRepo)

    assert {:error, :directory_already_exists} =
             Directory.move(layer, ["source"], ["existing"])
  end
end
