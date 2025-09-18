defmodule Bedrock.Directory.MetadataTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.Test.DirectoryHelpers
  import Mox
  import StreamData

  alias Bedrock.Directory

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  defp non_reserved_prefix do
    [min_length: 1, max_length: 8]
    |> binary()
    |> filter(fn prefix ->
      not String.starts_with?(prefix, <<0xFE>>) and not String.starts_with?(prefix, <<0xFF>>)
    end)
  end

  describe "directory metadata preservation" do
    property "create and open non-root directory with manual prefix preserves all metadata" do
      check all(
              path_name <- string(:alphanumeric, min_length: 1, max_length: 10),
              prefix <- non_reserved_prefix()
            ) do
        expect_version_initialization(MockRepo)
        layer = Directory.root(MockRepo, next_prefix_fn: fn -> prefix end)

        path = [path_name]
        layer_name = nil
        packed_value = pack_directory_value(prefix, "")

        MockRepo
        |> expect_directory_exists(path, nil)
        |> expect_collision_check(prefix)
        |> expect_ancestor_checks(byte_size(prefix) - 1)
        |> expect_directory_creation(path, {prefix, ""})

        assert {:ok,
                %{
                  prefix: ^prefix,
                  layer: ^layer_name,
                  path: ^path
                } = created_node} =
                 Directory.create(layer, path, layer: layer_name, prefix: prefix)

        MockRepo
        |> expect_directory_exists(path, packed_value)
        |> expect_version_check()

        assert {:ok, ^created_node} = Directory.open(layer, path)
      end
    end
  end
end
