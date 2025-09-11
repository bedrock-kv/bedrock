defmodule Bedrock.Directory.MoveTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  # Helper function specific to move tests
  defp expect_directory_move(repo, from_path, to_path, packed_value) do
    from_key = build_directory_key(from_path)
    to_key = build_directory_key(to_path)

    expect(repo, :put, fn :mock_txn, key, value ->
      # Assert we're moving to the correct location
      assert key == to_key, "Expected to move to #{inspect(to_key)} from #{inspect(from_key)}, got #{inspect(key)}"
      # Assert the value contains the correct data that was at from_path
      assert ^packed_value = Bedrock.Key.unpack(value)
      :ok
    end)
  end

  test "move directory preserves metadata and prefixes" do
    source_path = ["users"]
    dest_path = ["accounts"]
    source_data = Bedrock.Key.pack({<<0, 2>>, "document"})
    child_data = Bedrock.Key.pack({<<0, 3>>, "profile"})

    range_results = [
      {build_directory_key(["users"]), source_data},
      {build_directory_key(["users", "profiles"]), child_data}
    ]

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, source_data)
    |> expect_directory_exists(dest_path, nil)
    |> expect_directory_exists([], Bedrock.Key.pack({<<0, 1>>, ""}))
    |> expect_directory_exists(source_path, source_data)
    |> expect_range_scan(source_path, range_results)
    |> expect_directory_move(source_path, dest_path, {<<0, 2>>, "document"})
    |> expect_directory_move(["users", "profiles"], ["accounts", "profiles"], {<<0, 3>>, "profile"})
    |> expect_range_clear(source_path)

    layer = Layer.new(MockRepo)

    assert :ok = Directory.move(layer, source_path, dest_path)
  end

  test "move fails when source doesn't exist" do
    source_path = ["nonexistent"]
    dest_path = ["new"]

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, nil)

    layer = Layer.new(MockRepo)

    assert {:error, :directory_does_not_exist} =
             Directory.move(layer, source_path, dest_path)
  end

  test "move fails when destination exists" do
    source_path = ["source"]
    dest_path = ["existing"]
    source_data = Bedrock.Key.pack({<<0, 1>>, ""})
    dest_data = Bedrock.Key.pack({<<0, 2>>, ""})

    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(source_path, source_data)
    |> expect_directory_exists(dest_path, dest_data)

    layer = Layer.new(MockRepo)

    assert {:error, :directory_already_exists} =
             Directory.move(layer, source_path, dest_path)
  end
end
