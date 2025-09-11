defmodule Bedrock.Directory.MetadataTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  # Custom helper for this test - handles ancestor prefix checking
  defp expect_prefix_collision_check_with_ancestors(repo, prefix) do
    expected_range = Bedrock.KeyRange.from_prefix(prefix)

    repo
    |> expect(:range, fn :mock_txn, ^expected_range, opts ->
      assert opts[:limit] == 1
      []
    end)
    |> expect(:get, fn :mock_txn, key ->
      # This handles both the full prefix and ancestor checks
      assert key in [prefix, <<0>>]
      nil
    end)
  end

  describe "directory metadata preservation" do
    test "create and open non-root directory with manual prefix preserves all metadata" do
      path = ["test"]
      layer_name = nil
      prefix = <<0, 0>>
      packed_value = Bedrock.Key.pack({prefix, ""})

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(path, nil)
      |> expect_parent_exists(path)
      |> expect_prefix_collision_check_with_ancestors(prefix)
      |> expect_directory_creation(path, {prefix, ""})
      |> expect_directory_exists(path, packed_value)
      |> expect_version_check_only()

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> prefix end)

      # Create with manual prefix and verify all fields in one assertion
      assert {:ok, %{prefix: ^prefix, layer: ^layer_name, path: ^path}} =
               Directory.create(layer, path, layer: layer_name, prefix: prefix)

      # Open should return identical metadata using pattern matching
      assert {:ok, %{prefix: ^prefix, layer: ^layer_name, path: ^path}} =
               Directory.open(layer, path)
    end
  end
end
