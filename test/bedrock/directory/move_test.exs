defmodule Bedrock.Directory.MoveTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Key
  alias Bedrock.KeyRange

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  test "move directory preserves metadata and prefixes" do
    source_path = ["users"]
    dest_path = ["accounts"]
    source_data = Key.pack({<<0, 2>>, "document"})
    child_data = Key.pack({<<0, 3>>, "profile"})

    MockRepo
    |> expect_version_initialization()
    # Source exists check
    |> expect_directory_exists(source_path, source_data)
    # Destination doesn't exist check
    |> expect_directory_exists(dest_path, nil)
    # Source fetch for move operation (gets called again)
    |> expect_directory_exists(source_path, source_data)
    # Range scan to get source + all children
    |> expect(:get_range, fn :mock_txn, range ->
      expected_range = KeyRange.from_prefix(build_directory_key(source_path))
      assert expected_range == range

      [
        {build_directory_key(["users"]), source_data},
        {build_directory_key(["users", "profiles"]), child_data}
      ]
    end)
    # Put destination directory
    |> expect(:put, fn :mock_txn, key, value ->
      assert key == build_directory_key(dest_path)
      assert {<<0, 2>>, "document"} == Key.unpack(value)
      :ok
    end)
    # Put moved child directory
    |> expect(:put, fn :mock_txn, key, value ->
      assert key == build_directory_key(["accounts", "profiles"])
      assert {<<0, 3>>, "profile"} == Key.unpack(value)
      :ok
    end)
    # Clear source range
    |> expect(:clear_range, fn :mock_txn, range ->
      expected_range = KeyRange.from_prefix(build_directory_key(source_path))
      assert expected_range == range
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
    source_data = Key.pack({<<0, 1>>, ""})
    dest_data = Key.pack({<<0, 2>>, ""})

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, source_data)
    |> expect_directory_exists(dest_path, dest_data)

    layer = Directory.root(MockRepo)

    assert {:error, :directory_already_exists} =
             Directory.move(layer, source_path, dest_path)
  end
end
