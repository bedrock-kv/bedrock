defmodule Bedrock.Internal.TransactionBuilder.PointReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State

  test "regular point reads record absent keys as read conflicts" do
    key = "missing-key"
    read_version = Version.from_integer(42)

    state = %State{
      layout_index: %LayoutIndex{
        tree: :gb_trees.from_orddict([{<<0xFF, 0xFF>>, {"", [self()]}}])
      },
      read_version: read_version
    }

    storage_get_key_fn = fn _pid, ^key, ^read_version, _opts ->
      {:error, :not_found}
    end

    assert {%State{tx: tx}, {:error, :not_found}} =
             PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    assert %{reads: %{^key => :clear}} = tx
  end
end
