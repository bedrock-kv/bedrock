defmodule Bedrock.Directory.MoveTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Keyspace

  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  setup :verify_on_exit!

  test "move directory preserves metadata and prefixes" do
    source_path = ["users"]
    dest_path = ["accounts"]
    source_data = {<<0, 2>>, "document"}
    child_data = {<<0, 3>>, "profile"}
    # For get_range expectations, we need packed data since it's a raw key scan
    packed_source_data = Bedrock.Encoding.Tuple.pack(source_data)
    packed_child_data = Bedrock.Encoding.Tuple.pack(child_data)

    MockRepo
    |> expect_version_initialization()
    # Source exists check
    |> expect_directory_exists(source_path, source_data)
    # Destination doesn't exist check
    |> expect_directory_exists(dest_path, nil)
    # Source fetch for move operation (gets called again)
    |> expect_directory_exists(source_path, source_data)
    # Range scan to get source + all children - now using keyspace get_range/2
    |> expect(:get_range, fn start_key, end_key ->
      assert is_binary(start_key)
      assert is_binary(end_key)

      [
        {build_directory_key(["users"]), packed_source_data},
        {build_directory_key(["users", "profiles"]), packed_child_data}
      ]
    end)
    # Put destination directory - keyspace-aware
    |> expect(:put, fn %Keyspace{}, ^dest_path, value ->
      assert {<<0, 2>>, "document"} == value
      :ok
    end)
    # Put moved child directory - keyspace-aware
    |> expect(:put, fn %Keyspace{}, ["accounts", "profiles"], value ->
      assert {<<0, 3>>, "profile"} == value
      :ok
    end)
    # Clear source range - now using clear_range/3 with binary keys
    |> expect(:clear_range, fn start_key, end_key ->
      assert is_binary(start_key)
      assert is_binary(end_key)
      :ok
    end)

    layer = Directory.root(MockRepo)

    assert :ok = Directory.move(layer, source_path, dest_path)
  end

  test "move fails when source doesn't exist" do
    source_path = ["nonexistent"]
    dest_path = ["new"]

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, nil)

    layer = Directory.root(MockRepo)

    assert {:error, :directory_does_not_exist} =
             Directory.move(layer, source_path, dest_path)
  end

  test "move fails when destination exists" do
    source_path = ["source"]
    dest_path = ["existing"]
    source_data = Bedrock.Encoding.Tuple.pack({<<0, 1>>, ""})
    dest_data = Bedrock.Encoding.Tuple.pack({<<0, 2>>, ""})

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, source_data)
    |> expect_directory_exists(dest_path, dest_data)

    layer = Directory.root(MockRepo)

    assert {:error, :directory_already_exists} =
             Directory.move(layer, source_path, dest_path)
  end
end
