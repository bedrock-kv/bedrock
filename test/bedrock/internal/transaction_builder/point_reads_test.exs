defmodule Bedrock.Internal.TransactionBuilder.PointReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Key

  defp build_state do
    %State{
      layout_index: %LayoutIndex{
        tree: :gb_trees.from_orddict([{<<0xFF, 0xFF>>, {"", [self()]}}])
      },
      read_version: Version.from_integer(42)
    }
  end

  test "point read that misses storage still records a read conflict" do
    key = "missing-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:error, :not_found}
    end

    assert {%State{tx: tx}, {:error, :not_found}} =
             PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    assert {key, Key.key_after(key)} in tx.range_reads
  end

  test "point read that hits storage records a read conflict" do
    key = "present-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:ok, "value"}
    end

    assert {%State{tx: tx}, {:ok, {^key, "value"}}} =
             PointReads.get_key(state, key, storage_get_key_fn: storage_get_key_fn)

    assert {key, Key.key_after(key)} in tx.range_reads
  end

  test "snapshot point read that misses storage records no read conflict" do
    key = "missing-key"
    state = build_state()

    storage_get_key_fn = fn _pid, ^key, _version, _opts ->
      {:error, :not_found}
    end

    assert {%State{tx: tx}, {:error, :not_found}} =
             PointReads.get_key(state, key,
               storage_get_key_fn: storage_get_key_fn,
               snapshot: true
             )

    assert tx.range_reads == []
    assert tx.reads == %{}
  end
end
