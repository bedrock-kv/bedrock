defmodule Bedrock.Directory.MetadataTest do
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

  test "create non-root directory with manual prefix preserves metadata" do
    path = ["test"]
    layer_name = nil
    prefix = <<0, 0>>

    # CREATE expectations
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

    # 4. Check if directory exists
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["test"])
      nil
    end)

    # 5. Parent check (root exists)
    expect(MockRepo, :get, fn :mock_txn, key ->
      # Root directory key
      assert key == <<254>>
      # Root exists
      Bedrock.Key.pack({<<0, 1>>, ""})
    end)

    # 6. Prefix collision check - range query
    expect(MockRepo, :range, fn :mock_txn, range, opts ->
      assert opts[:limit] == 1
      expected_range = Bedrock.KeyRange.from_prefix(<<0, 0>>)
      assert range == expected_range
      # No collision
      []
    end)

    # 7. Check ancestor prefix <<0>>
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<0>>
      # No collision
      nil
    end)

    # 8. Store directory
    stored_value = Bedrock.Key.pack({prefix, ""})

    expect(MockRepo, :put, fn :mock_txn, key, value ->
      assert key == <<254>> <> Bedrock.Key.pack(["test"])
      assert value == stored_value
      :ok
    end)

    # OPEN expectations
    # 1. Check if directory exists
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == <<254>> <> Bedrock.Key.pack(["test"])
      stored_value
    end)

    # 2. check_version for read
    expect(MockRepo, :get, fn :mock_txn, key ->
      assert key == @version_key
      <<1::little-32, 0::little-32, 0::little-32>>
    end)

    layer = Layer.new(MockRepo, next_prefix_fn: fn -> prefix end)

    # Create with manual prefix
    assert {:ok, created_node} = Directory.create(layer, path, layer: layer_name, prefix: prefix)
    assert created_node.prefix == prefix
    assert created_node.layer == layer_name
    assert created_node.path == path

    # Open should return same metadata
    assert {:ok, opened_node} = Directory.open(layer, path)
    assert opened_node.prefix == created_node.prefix
    assert opened_node.layer == created_node.layer
    assert opened_node.path == created_node.path
  end
end
